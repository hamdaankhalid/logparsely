// Copyright (c) Microsoft Corporation.

mod cli;
mod concurrency_helper;
mod ingestion;
mod storage;

use rusqlite::Connection;
use std::io::{self, Read};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::{fs, path::PathBuf};
use uuid::Uuid;

use crate::cli::{cli_arg_parser, noninteractive_mode, CommonArgs, Mode};

use crate::concurrency_helper::SharedState;

const QUIT_POLL_INTERVAL_MS: u64 = 500;

fn purge() {
    let logs_dir_path = PathBuf::from("logs"); // Relative path within a "logs" directory
    if let Ok(entries) = fs::read_dir(logs_dir_path) {
        for entry in entries.flatten() {
            let file_path = entry.path();
            if !file_path.is_file() {
                continue;
            }
            let file_name = match file_path.file_name().and_then(|name| name.to_str()) {
                Some(name) => name,
                None => continue,
            };
            if !file_name.ends_with("-logparsely.db") {
                continue;
            }
            if let Err(err) = fs::remove_file(file_path) {
                eprintln!("Error deleting the file: {}", err);
            }
        }
    } else {
        eprintln!("Failed to read directory");
    }
}

fn get_db_path(common_args: CommonArgs) -> Result<PathBuf, &'static str> {
    let path: PathBuf = common_args
        .db_file_path
        .map(|v| {
            PathBuf::from_str(&v).map_err(|_| "DB file path supplied is not a valid File path")
        })
        .unwrap_or_else(|| {
            let tmp_file_name = format!("{}-logparsely.db", Uuid::new_v4());
            let mut path = PathBuf::from("logs"); // Relative path within a "logs" directory
            path.push(tmp_file_name);
            Ok(path)
        })?;

    Ok(path)
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

fn blocking_kill_children_processes(shared_signal: Arc<SharedState>) {
    println!("Closing background tasks");
    // send signal to all threads to stop
    shared_signal.stop();

    shared_signal.wait_all_children_done();
}

fn main() {
    let args = cli_arg_parser();

    // used for signaling threads to kill child processes
    let shared_signal = Arc::new(SharedState::new());

    match args.mode {
        Mode::Noninteractive { common_args, args } => {
            let db = get_db_path(common_args).expect("");
            println!(
                "All data is being streamed into SQLITE DB: {}",
                db.to_str().unwrap()
            );

            let conn = Connection::open(&db).unwrap_or_else(|err| {
                // If database connection cannot be established, then no damn point in continuing
                panic!("Error creating database connection: {:?}", err);
            });
            // Db connection is shared across multiple threads in a mutable manner. So wrap it in an arc mutex.
            let shared_connection = Arc::new(Mutex::new(conn));
            // sigkill cleanup handler
            noninteractive_mode(
                Arc::clone(&shared_connection),
                args.srcs,
                Arc::clone(&shared_signal),
            );

            println!("Press 'q' to exit");
            // Wait for a short duration
            while read_key() != Some('q') {
                std::thread::sleep(std::time::Duration::from_millis(QUIT_POLL_INTERVAL_MS));
            }

            blocking_kill_children_processes(shared_signal.clone());
            println!("All data has been saved to {}", db.to_str().unwrap());
        }
        Mode::Purge => {
            println!("Purging all data files from temp storage");
            purge();
            println!("Temp data files cleaned");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_get_db_path_with_valid_path() {
        let common_args = CommonArgs {
            db_file_path: Some("valid/existing.db".to_string()),
        };

        let path = get_db_path(common_args).unwrap();
        assert_eq!(path, Path::new("valid/existing.db"));
    }

    #[test]
    fn test_get_db_path_with_none() {
        let common_args = CommonArgs { db_file_path: None };
        let path = get_db_path(common_args).unwrap();
        println!("{:?}", path);
        assert!(path.starts_with("logs/"));
        assert!(path.to_str().unwrap().ends_with("-logparsely.db"));
    }
}
