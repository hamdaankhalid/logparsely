// Copyright (c) Microsoft Corporation.

/// Provides data ingestion functionality.
///
/// This module defines a `add_src` function that is responsible for ingesting data from a single source's standard output.
use std::collections::HashMap;
use std::error::Error;
use std::io::prelude::*;
use std::io::BufReader;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use rusqlite::Connection;
use serde_json::Value;

use crate::concurrency_helper::SharedState;
use crate::storage::{EvolvingWideTable, RAW_UNPARSABLE_COL};

/// Recursively flattens a JSON object into a map of string keys to string values.
///
/// This function is used to transform a nested JSON object into a flat map. The keys in the map are the paths to the values in the JSON object.
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
                    format!("{prefix}.{key}")
                };
                flatten_json_recursive(value, new_prefix, result);
            }
        }
        Value::Array(arr) => {
            for (index, value) in arr.iter().enumerate() {
                let new_prefix = format!("{prefix}[{index}]");
                flatten_json_recursive(value, new_prefix, result);
            }
        }
        fallback => {
            result.insert(prefix, fallback.to_string());
        }
    };
}

/// Ingests data from a single source's standard output.
///
/// This function is responsible for ingesting data from a single source's standard output. It reads the output line by line, transforms each line into a flat JSON object, and inserts the object into a SQLite database.
///
/// The function takes four arguments:
/// * `child`: a `Child` process that is producing the data to ingest.
/// * `src_name`: a string that identifies the data source.
/// * `shared_connection`: a thread-safe `Arc<Mutex<Connection>>` to a SQLite database.
/// * `signal`: a `SharedState` that can be used to signal the function to stop ingesting data.
///
/// The function spawns a monitor thread that kills the `child` process if the main thread signals to stop.
fn transformation(
    mut child: Child,
    src_name: &str,
    shared_connection: Arc<Mutex<Connection>>,
    signal: Arc<SharedState>,
) {
    let stdout = child.stdout.take().expect("Failed to capture stdout");
    let reader = BufReader::new(stdout);
    let lines = reader.lines();

    let wide_table_res =
        EvolvingWideTable::new(src_name.to_string(), Arc::clone(&shared_connection));

    let mut wide_table = match wide_table_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error in setting up SQLITE3 on your system {:?}", e);
            signal.decr_and_notify_all_children_done_awaiters();
            return;
        }
    };

    // since logaprsely creates the Process it is redirecting standardout from, it is responsible for killing it.
    // this monitor thread is responsible for killing the child process if the main thread signals to stop.
    let monitor = thread::spawn({
        let sig = Arc::clone(&signal);
        move || {
            sig.wait_for_stop_signal();

            println!("Ingestion Thread: Received stop signal. Exiting.");

            // kill the child process if it is still running
            if let Ok(None) = child.try_wait() {
                match child.kill() {
                    Ok(_) => {}
                    Err(_) => {
                        eprintln!("Child command kicked by background thread failed to exit, please kill process using pid");
                    }
                }
            }

            sig.decr_and_notify_all_children_done_awaiters();
        }
    });

    for line_res in lines {
        let line = match line_res {
            Err(_) => {
                eprintln!("Error reading line");
                continue;
            }
            Ok(line) => line,
        };

        match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(json_val) => {
                if let None = json_val.as_object() {
                    eprintln!("JSON object expected, but found: {}", json_val);
                    continue;
                }

                let flattened_map = flatten_json(&json_val);
                if let Err(op_err) =
                    wide_table.insert_data(Arc::clone(&shared_connection), flattened_map)
                {
                    eprintln!("Error inserting data into wide table: {:?}", op_err);
                };
            }
            Err(_) => write_non_json_line(line, &mut wide_table, shared_connection.clone()),
        }
    }

    // join on the monitor thread blocks until the monitor thread recvs a signal from main thread that the transormation thread should stop
    match monitor.join() {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Unexpected error in monitoring thread: {:?}", e);
        }
    }
}

// convert line that could not be parsed correctly to json and add it as a catch all
fn write_non_json_line(
    line: String,
    wide_table: &mut EvolvingWideTable,
    shared_connection: Arc<Mutex<Connection>>,
) {
    let mut err_fmt = HashMap::<String, String>::new();
    err_fmt.insert(RAW_UNPARSABLE_COL.to_string(), line);
    if let Err(op_err) = wide_table.insert_data(Arc::clone(&shared_connection), err_fmt) {
        eprintln!("Error inserting data into wide table: {:?}", op_err);
    };
}

/// Pipes a new source into the ingestion pipeline.
///
/// This function is responsible for ingesting data from a new source. The source is a shell command, and the function redirects the standard output of the command to the ingestion pipeline.
///
/// The function spawns a new thread to handle the ingestion of the source and does not wait for the thread to join. It increments a counter in the shared state when the thread is created and the `transformation` function is responsible for decrementing the counter when it finishes. This is used to determine when all threads have finished.
///
/// The function takes three arguments:
/// * `cmd`: a string that specifies the shell command to run to produce the data to ingest.
/// * `shared_connection`: a thread-safe `Arc<Mutex<Connection>>` to a SQLite database.
/// * `signal`: a `SharedState` that can be used to signal the function to stop ingesting data.
///
/// The function returns a `Result<(), Box<dyn Error>>`. If the function is successful, it returns `Ok(())`. If an error occurs, it returns `Err(error)`.
///
/// # Examples
///
/// ```
/// let shared_connection = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
/// let signal = Arc::new(SharedState::new());
/// add_src("ls -l", shared_connection, signal).unwrap();
/// ```
pub fn add_src(
    cmd: &str,
    shared_connection: Arc<Mutex<Connection>>,
    signal: Arc<SharedState>,
) -> Result<(), Box<dyn Error>> {
    let command = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdout(Stdio::piped())
        .spawn()?;

    // sanitize name for table creation
    let table_src_name = cmd
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace("~", "HOME");

    signal.incr();

    thread::spawn({
        move || {
            transformation(
                command,
                &table_src_name,
                Arc::clone(&shared_connection),
                signal,
            )
        }
    });

    Ok(())
}
