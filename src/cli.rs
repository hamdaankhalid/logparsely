// Copyright (c) Microsoft Corporation.

/// Provides command line interface (CLI) argument parsing functionality.
///
/// This module uses the `clap` crate to define and parse CLI arguments.
use std::sync::{Arc, Mutex};

use clap::Parser;
use rusqlite::Connection;

use crate::{concurrency_helper::SharedState, ingestion::add_src};

/// Represents the arguments for the noninteractive mode.
#[derive(Parser, Debug, Clone)]
pub struct NoninteractiveArgs {
    /// A vector of strings representing the data sources to ingest.
    #[clap(short, long)]
    pub srcs: Vec<String>,
}

/// Represents the different modes the application can run in.
#[derive(Parser, Debug, Clone)]
pub enum Mode {
    /// Purge mode.
    #[clap(name = "purge")]
    Purge,
    /// Noninteractive mode.
    #[clap(name = "noninteractive")]
    Noninteractive {
        /// Common arguments used across different modes.
        #[clap(flatten)]
        common_args: CommonArgs,
        /// Arguments specific to the noninteractive mode.
        #[clap(flatten)]
        args: NoninteractiveArgs,
    },
}

/// Represents the common arguments used across different modes.
#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
    /// An optional string representing the path to the database file.
    #[clap(short, long)]
    pub db_file_path: Option<String>,
}

/// Represents the command line arguments for the application.
#[derive(Parser, Debug, Clone)]
pub struct CliArgs {
    /// The mode the application should run in.
    #[clap(subcommand)]
    pub mode: Mode,
}

/// Parses the command line arguments and returns a `CliArgs` struct.
///
/// This function uses the `clap` crate's `Parser` trait to parse the command line arguments.
pub fn cli_arg_parser() -> CliArgs {
    CliArgs::parse()
}

/// This function runs the application in noninteractive mode.
///
/// Noninteractive mode is used when the user wants to ingest data from multiple sources in a single run.
/// Initially we supported an interactive repl mode, but that was deprecated in favor of this mode as the most common use case.
///
/// # Arguments
///
/// * `shared_connection` - An Arc-wrapped Mutex-protected SQLite Connection that is shared across the application.
/// * `srcs` - A vector of strings representing the data sources to ingest.
/// * `shared_signal` - An Arc-wrapped SharedState used for inter-thread communication.
///
/// # Behavior
///
/// This function iterates over each data source in `srcs`, and attempts to add it using the `add_src` function.
/// If adding a source fails, an error message is printed to stderr.
pub fn noninteractive_mode(
    shared_connection: Arc<Mutex<Connection>>,
    srcs: Vec<String>,
    shared_signal: Arc<SharedState>,
) {
    for child in srcs {
        println!("Adding data ingestion source {}", child);

        if let Err(e) = add_src(
            &child,
            Arc::clone(&shared_connection),
            shared_signal.clone(),
        ) {
            eprintln!(
                "Adding data ingestion source {} failed due to {:?}",
                child, e
            );
            continue;
        }

        println!("Data ingestion source {} added successfully", child);
    }
}
