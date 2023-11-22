use core::time;
use rusqlite::Connection;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error,
    io::{self, BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread::{self},
    time::Duration,
};
use uuid::Uuid;

// Shared state for signaling between threads, and wait group
struct SharedState {
    should_stop: bool,
    ctr: i32,
}

impl SharedState {
    fn new() -> Self {
        SharedState {
            should_stop: false,
            ctr: 0,
        }
    }

    fn stop(&mut self) {
        self.should_stop = true;
    }

    fn should_stop(&self) -> bool {
        self.should_stop
    }

    fn incr(&mut self) {
        self.ctr += 1;
    }

    fn decr(&mut self) {
        self.ctr -= 1;
    }
}

// A wide table store sorta creates a sparse matrix of text fields for querying logs
// using fields as a lookup mechanism for any nested json object that is ingested
struct EvolvingWideTable {
    col_lookup: HashSet<String>,
    table_name: String,
    conn: Connection,
}

impl EvolvingWideTable {
    // Init table and cleanup if needed
    fn new(table_name: String, path: &PathBuf) -> Result<EvolvingWideTable, Box<dyn Error>> {
        let conn = Connection::open(path)?;

        // Create the table
        let create_query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY
            )",
            table_name
        );

        conn.execute(&create_query, ())?;

        let query = format!("PRAGMA table_info({})", table_name);
        let mut stmt = conn.prepare(&query)?;
        // Use query_map on the Statement to retrieve column names
        let column_names = stmt.query_map([], |row| row.get(1))?;

        let mut col_lookup: HashSet<String> = HashSet::new();
        for col in column_names {
            let col_str: String = col?;
            if col_str == "id" {
                col_lookup.insert(col_str.clone());
            } else {
                let col_sanitized = col_str.trim_matches('`');
                col_lookup.insert(col_sanitized.to_string());
            }
        }

        Ok(EvolvingWideTable {
            col_lookup,
            table_name,
            conn: Connection::open(path)?,
        })
    }

    fn insert_data(&mut self, data: HashMap<String, String>) {
        // check if schema needs to be altered
        let mut new_cols = Vec::new();
        for key in data.keys() {
            if !self.col_lookup.contains(&key.clone()) {
                new_cols.push(key.clone());
                self.col_lookup.insert(key.clone());
            }
        }

        // dirty schema altering hacky af!
        let mut alters: Vec<String> = Vec::new();
        for col in new_cols {
            alters.push(format!(
                "ALTER TABLE {} ADD `{}` TEXT; ",
                self.table_name, col
            ));
        }

        let batch_stmt = format!("BEGIN; {} COMMIT;", alters.join(""));

        if let Err(e) = self.conn.execute_batch(&batch_stmt) {
            eprintln!("Schema Altering failed due to : {}", e);
            return;
        }

        // insert actual data hacky!
        let mut cols: Vec<String> = Vec::new();
        let mut vals: Vec<String> = Vec::new();
        let mut sqlite_vals: Vec<&String> = Vec::new();

        for col in self.col_lookup.iter() {
            if data.contains_key(col) {
                cols.push(format!("`{}`", col));
                vals.push("?".to_string());
                sqlite_vals.push(&data[col]);
            }
        }

        let joined_cols = cols.join(",");
        let joined_vals = vals.join(",");

        let insert_stmt = format!(
            "INSERT INTO {} ({}) VALUES ({});",
            self.table_name, joined_cols, joined_vals
        );

        // TODO: Fix unwrap!
        if let Err(e) = self.conn.execute(
            &insert_stmt,
            sqlite_vals
                .iter()
                .map(|v| v as &dyn rusqlite::ToSql)
                .collect::<Vec<_>>()
                .as_slice(),
        ) {
            eprintln!("Data insertion failed due to: {}", e);
            return;
        }
    }
}

// Recursive JSON flattening for nested objects
fn flatten_json(json: &Value) -> HashMap<String, String> {
    let mut result = HashMap::new();
    flatten_json_recursive(json, String::new(), &mut result);
    result
}

fn flatten_json_recursive(json: &Value, prefix: String, result: &mut HashMap<String, String>) {
    match json {
        Value::Object(obj) => {
            for (key, value) in obj {
                let new_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_json_recursive(value, new_prefix, result);
            }
        }
        Value::Array(arr) => {
            for (index, value) in arr.iter().enumerate() {
                let new_prefix = format!("{}[{}]", prefix, index);
                flatten_json_recursive(value, new_prefix, result);
            }
        }
        fallback => {
            result.insert(prefix, fallback.to_string());
        }
    };
}

// Ingestion Thread Task
// IO redirection from child -> Flat transform -> Shove into wide table store (adapt schema
// internally)
fn transformation(
    child: &mut Child,
    src_name: &str,
    db_path: &PathBuf,
    signal: Arc<Mutex<SharedState>>,
) {
    let stdout = child.stdout.take().expect("Failed to capture stdout");
    let reader = BufReader::new(stdout);
    let lines = reader.lines();

    let wide_table_res = EvolvingWideTable::new(src_name.to_string(), db_path);

    let mut wide_table = match wide_table_res {
        Ok(v) => v,
        Err(e) => {
            signal.lock().unwrap().decr();
            eprintln!("Error in setting up SQLITE3 on your system {:?}", e);
            return;
        }
    };

    for line_res in lines {
        let mut signal_lock = signal.lock().unwrap();
        if signal_lock.should_stop() {
            println!("Ingestion Thread: Received stop signal. Exiting.");
            signal_lock.decr();
            let _ = child.kill();
            return;
        }  
        drop(signal_lock);

        match line_res {
            Err(_) => {
                eprintln!("Error reading line");
            }
            Ok(line) => {
                match serde_json::from_str::<serde_json::Value>(&line) {
                    Ok(json_val) => {
                        if let Some(_) = json_val.as_object() {
                            let flattened_map = flatten_json(&json_val);
                            wide_table.insert_data(flattened_map);
                        }
                    }
                    Err(_) => {} // Willingly ignore logs that are not json
                }
            }
        }
    }

    loop {
        let mut signal_lock = signal.lock().unwrap();
        if let Ok(Some(_)) = child.try_wait() {
            println!("Ingestion Thread: exited!");
            signal_lock.decr();
            return;
        }

        if signal_lock.should_stop() {
            println!("Ingestion Thread: Received stop signal. Exiting.");
            let _ = child.kill();
            signal_lock.decr();
            return;
        }
        drop(signal_lock);
        thread::sleep(time::Duration::from_secs(2));
    }
}

fn add_src(
    cmd: &str,
    db_file_path: &PathBuf,
    signal: Arc<Mutex<SharedState>>,
) -> Result<(), Box<dyn Error>> {
    // check if file exists
    let mut command = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdout(Stdio::piped())
        .spawn()?;

    let src_name = cmd.replace(" ", "_").replace(".", "_").replace("-", "_");

    signal.lock().unwrap().incr();

    // kick off a task to read in data from the child into tmp file
    let _ = thread::spawn({
        let src_name_cl = src_name.clone();
        let db_file_path_cl = db_file_path.clone();
        move || transformation(&mut command, &src_name_cl, &db_file_path_cl, signal)
    });

    Ok(())
}

// CLI Logic
const INVALID_OPTION: &str = "Invalid selection";

const TOP_LEVEL_MENU: &str = "
        #######################
        ---Menu---:
        (1) To add more data sources
        (2) To exit";

const ADD_DATA_SOURCE_MENU: &str = "
        ########################\n
        ---Enter data source or enter DONE to return to Menu---:";

fn add_data_sources_repl(
    db_file: &PathBuf,
    signal: Arc<Mutex<SharedState>>,
) -> Result<(), Box<dyn Error>> {
    let mut run = true;
    while run {
        println!("{}", ADD_DATA_SOURCE_MENU);
        let mut user_input = String::new();
        io::stdin()
            .read_line(&mut user_input)
            .expect("Failed to read line");

        match user_input.trim() {
            "DONE" => {
                run = false;
            }
            _ => {
                match add_src(user_input.trim(), db_file, Arc::clone(&signal)) {
                    Ok(_) => {
                        println!("data source added");
                    }
                    Err(_) => {
                        println!("err in child keep looping")
                    } // TODO: error handling
                }
            }
        }
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut user_passed_db: Option<PathBuf> = Option::None;
    if args.len() > 2 {
        eprintln!("USAGE: {} <optional_db_file_path>", args[0]);
    }
    if args.len() == 2 {
        // second arg is the name of the existing local db
        user_passed_db = Some(PathBuf::from(&args[1]));
    }

    let mut run = true;

    let db_dir_path = match user_passed_db {
        None => {
            let tmp_file_name = format!("{}-logparsely.db", Uuid::new_v4());
            let mut path = env::temp_dir();
            path.push(tmp_file_name);
            path
        }
        Some(v) => v,
    };

    println!(
        "
            All your data will be written to SQLITE database: {}
        ",
        db_dir_path.to_str().unwrap()
    );

    println!("
             Once you add the data sources all data will be ingested into the sqlite db given above.
             You can choose to run commands that stream data to stdout and take advantage of realtime
             ingestion into sqlite while the logparsely session is open.
    ");

    // used for signaling threads to kill child processes
    let shared_signal = Arc::new(Mutex::new(SharedState::new()));

    while run {
        println!("{}", TOP_LEVEL_MENU);

        let mut user_input = String::new();
        io::stdin()
            .read_line(&mut user_input)
            .expect("Failed to read line");

        let choice: u32 = match user_input.trim().parse() {
            Ok(num) => num,
            Err(_) => {
                println!("Invalid input. Please enter a number.");
                continue;
            }
        };

        match choice {
            1 => {
                // data source repl
                let _ = add_data_sources_repl(&db_dir_path, Arc::clone(&shared_signal));
            }
            2 => {
                run = false;
            }
            _ => {
                println!("{}", INVALID_OPTION);
            }
        }
    }
    // kill the child processes
    let mut sig = shared_signal.lock().unwrap();
    sig.stop();
    println!("Signal sent");
    drop(sig);
    println!("Closing background tasks");
    loop {
        let sig = shared_signal.lock().unwrap();

        println!("waiting.... (if you can force an extra log I can exit faster)");
        if sig.ctr <= 0 {
            break;
        }
        thread::sleep(Duration::from_secs(2));
    }

    println!(
        "All data has been saved to {}",
        db_dir_path.to_str().unwrap()
    );
}
