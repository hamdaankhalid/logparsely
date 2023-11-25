use cli::CommonArgs;
use std::io::{self, Read};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::{env, path::PathBuf};
use uuid::Uuid;

use crate::cli::{
    blocking_kill_children, cli_arg_parser, interactive_mode, noninteractive_mode, Mode,
};
use crate::concurrency_helper::SharedState;

mod concurrency_helper {
    // Shared state for signaling between threads, and wait group
    pub struct SharedState {
        should_stop: bool,
        ctr: i32,
    }

    impl SharedState {
        pub fn new() -> Self {
            SharedState {
                should_stop: false,
                ctr: 0,
            }
        }

        pub fn stop(&mut self) {
            self.should_stop = true;
        }

        pub fn should_stop(&self) -> bool {
            self.should_stop
        }

        pub fn incr(&mut self) {
            self.ctr += 1;
        }

        pub fn decr(&mut self) {
            self.ctr -= 1;
        }

        pub fn ctr(&self) -> i32 {
            self.ctr
        }
    }
}

mod storage {
    use std::{
        collections::{HashMap, HashSet},
        error::Error,
        path::PathBuf,
    };

    use rusqlite::Connection;

    // A wide table store sorta creates a sparse matrix of text fields for querying logs
    // using fields as a lookup mechanism for any nested json object that is ingested
    pub struct EvolvingWideTable {
        col_lookup: HashSet<String>,
        table_name: String,
        conn: Connection,
    }

    impl EvolvingWideTable {
        // Init table and cleanup if needed
        pub fn new(
            table_name: String,
            path: &PathBuf,
        ) -> Result<EvolvingWideTable, Box<dyn Error>> {
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

        pub fn insert_data(&mut self, data: HashMap<String, String>) {
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
}

mod ingestion {
    use std::{
        collections::HashMap,
        error::Error,
        io::{BufRead, BufReader},
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use serde_json::Value;

    use crate::{concurrency_helper::SharedState, storage::EvolvingWideTable};

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
        mut child: Child,
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

        let monitor = thread::spawn({
            let sig = Arc::clone(&signal);
            move || {
                loop {
                    let mut signal_lock = sig.lock().unwrap();

                    // if it finished on it's own..great!
                    if let Ok(Some(_)) = child.try_wait() {
                        signal_lock.decr();
                        let _ = child.kill();
                        return;
                    }

                    // signal to finish has been recvd, so we need to kill the command underneath
                    if signal_lock.should_stop() {
                        println!("Ingestion Thread: Received stop signal. Exiting.");
                        signal_lock.decr();
                        match child.kill() {
                            Ok(_) => {}
                            Err(_) => {
                                eprintln!("Child command kicked by background thread failed to exit, please kill process using pid");
                            }
                        }
                        return;
                    }
                    drop(signal_lock);
                    thread::sleep(Duration::from_secs(1));
                }
            }
        });

        for line_res in lines {
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
                        Err(_) => {} // Willingly ignore logs that are not json.
                    }
                }
            }
        }

        match monitor.join() {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Unexpected error in monitoring thread: {:?}", e);
            }
        }
    }

    pub fn add_src(
        cmd: &str,
        db_file_path: &PathBuf,
        signal: Arc<Mutex<SharedState>>,
    ) -> Result<(), Box<dyn Error>> {
        let command = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .stdout(Stdio::piped())
            .spawn()?;

        let src_name = cmd.replace(" ", "_").replace(".", "_").replace("-", "_");

        signal.lock().unwrap().incr();

        thread::spawn({
            let src_name_cl = src_name.clone();
            let db_file_path_cl = db_file_path.clone();
            move || transformation(command, &src_name_cl, &db_file_path_cl, signal)
        });

        Ok(())
    }
}

mod cli {
    use std::{
        error::Error,
        io,
        path::PathBuf,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use clap::Parser;

    use crate::{concurrency_helper::SharedState, ingestion::add_src};

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
                _ => match add_src(user_input.trim(), db_file, Arc::clone(&signal)) {
                    Ok(_) => {
                        println!("data source added");
                    }
                    Err(_) => {
                        println!("err in data source addition, try again");
                    }
                },
            }
        }

        Ok(())
    }

    pub fn interactive_mode(db_dir_path: &PathBuf, shared_signal: Arc<Mutex<SharedState>>) {
        let mut run = true;
        println!("
                 Once you add the data sources all data will be ingested into the sqlite db given above.
                 You can choose to run commands that stream data to stdout and take advantage of realtime
                 ingestion into sqlite while the logparsely session is open.
        ");

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
                    match add_data_sources_repl(db_dir_path, Arc::clone(&shared_signal)) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Unexpected error {:?}", e);
                            // early exit
                            run = false;
                        }
                    };
                }
                2 => {
                    run = false;
                }
                _ => {
                    println!("{}", INVALID_OPTION);
                }
            }
        }
    }

    pub fn noninteractive_mode(
        db_file: &PathBuf,
        srcs: Vec<String>,
        shared_signal: Arc<Mutex<SharedState>>,
    ) {
        for child in srcs {
            match add_src(&child, db_file, Arc::clone(&shared_signal)) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!(
                        "Adding data ingestion source {} failed due to {:?}",
                        child, e
                    );
                }
            }
        }
    }

    pub fn blocking_kill_children(shared_signal: Arc<Mutex<SharedState>>) {
        // signal kill the child processes that were created in transformer threads
        let mut sig = shared_signal.lock().unwrap();
        sig.stop();
        drop(sig);
        println!("Closing background tasks");
        loop {
            let sig = shared_signal.lock().unwrap();

            println!("waiting for children to exit....");
            if sig.ctr() <= 0 {
                break;
            }
            drop(sig);
            thread::sleep(Duration::from_secs(2));
        }
    }

    #[derive(Parser, Debug, Clone)]
    pub struct NoninteractiveArgs {
        #[clap(short, long)]
        pub srcs: Vec<String>,
    }

    #[derive(Parser, Debug, Clone)]
    pub enum Mode {
        #[clap(name = "noninteractive")]
        Noninteractive {
            #[clap(flatten)]
            common_args: CommonArgs,
            #[clap(flatten)]
            args: NoninteractiveArgs,
        },
        #[clap(name = "interactive")]
        Interactive {
            #[clap(flatten)]
            common_args: CommonArgs,
        },
    }

    #[derive(Parser, Debug, Clone)]
    pub struct CommonArgs {
        #[clap(short, long)]
        pub db_file_path: Option<String>,
    }

    #[derive(Parser, Debug, Clone)]
    pub struct CliArgs {
        #[clap(subcommand)]
        pub mode: Mode,
    }

    pub fn cli_arg_parser() -> Result<CliArgs, String> {
        let args = CliArgs::parse();
        Ok(args)
    }
}

fn get_db_path(common_args: CommonArgs) -> Result<PathBuf, ()> {
    let db_dir_path: PathBuf = match common_args.db_file_path {
        None => {
            let tmp_file_name = format!("{}-logparsely.db", Uuid::new_v4());
            let mut path = env::temp_dir();
            path.push(tmp_file_name);
            path
        }
        Some(v) => match PathBuf::from_str(&v) {
            Ok(v) => v,
            Err(_) => {
                eprintln!("DB file path supplied is not a valid File path");
                return Err(());
            }
        },
    };

    return Ok(db_dir_path);
}

fn read_key() -> Option<char> {
    let mut buffer = [0; 1];

    // Read a single character from standard input
    if let Ok(()) = io::stdin().read_exact(&mut buffer) {
        Some(buffer[0] as char)
    } else {
        None
    }
}

fn main() {
    let args = match cli_arg_parser() {
        Ok(v) => v,
        Err(_) => {
            return;
        }
    };
    // used for signaling threads to kill child processes
    let shared_signal = Arc::new(Mutex::new(SharedState::new()));

    match args.mode {
        Mode::Interactive { common_args } => {
            // blocking interactive mode
            let db = get_db_path(common_args).expect("");
            println!(
                "All data is being streamed into SQLITE DB: {}",
                db.to_str().unwrap()
            );
            interactive_mode(&db, Arc::clone(&shared_signal));

            blocking_kill_children(Arc::clone(&shared_signal));
            println!("All data has been saved to {}", db.to_str().unwrap());
        }
        Mode::Noninteractive { common_args, args } => {
            let db = get_db_path(common_args).expect("");
            let db_str = db.to_str().unwrap();

            println!(
                "All data is being streamed into SQLITE DB: {}",
                db_str.to_string()
            );

            let cloned_signal = Arc::clone(&shared_signal);

            ctrlc::set_handler(move || {
                blocking_kill_children(cloned_signal.clone());
            })
            .expect("Error setting sigkill handler");

            // sigkill cleanup handler
            noninteractive_mode(&db, args.srcs, Arc::clone(&shared_signal));

            println!("Press Q to exit");
            while read_key() != Some('q') {
                // Wait for a short duration
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            blocking_kill_children(shared_signal.clone());
            println!("All data has been saved to {}", db.to_str().unwrap());
        }
    }
}
