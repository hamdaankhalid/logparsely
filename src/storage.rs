// Copyright (c) Microsoft Corporation.

/// Provides storage functionality as a wrapper/abstraction around SQLITE.
///
/// This module defines a `EvolvingWideTable` struct that is used to store and query logs.
use rusqlite::Connection;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
};

pub const RAW_UNPARSABLE_COL: &str = "raw_unparsable_line";
const ID_COLUMN: &str = "id";

/// Represents errors that can occur when inserting into storage.
///
/// This enum provides specific variants for different types of insertion errors,
/// allowing you to handle them in a granular way.
///
/// # Variants
///
/// * `LockError` - Represents an error that occurred while trying to acquire a lock.
/// * `RecordInsertionError` - Represents an error that occurred while trying to insert a record.
/// * `SchemaManipulationError` - Represents an error that occurred while trying to manipulate the schema.
#[derive(Debug)]
pub enum StorageInsertionError {
    LockError(String),
    RecordInsertionError(Box<dyn Error>),
    SchemaManipulationError(Box<dyn Error>),
}

/// Represents errors that can occur when instantiating an evolving wide table.
///
/// This enum provides specific variants for different types of instantiation errors,
/// allowing you to handle them in a granular way.
///
/// # Variants
///
/// * `LockError` - Represents an error that occurred while trying to acquire a lock.
/// * `SqlError` - Represents an error that occurred while executing an SQL command.
#[derive(Debug)]
pub enum EvolvingWideTableInstantiationError {
    LockError(String),
    SqlError(Box<dyn Error>),
}

/// A wide table store that creates a sparse matrix of text fields for querying logs.
///
/// This struct is used to ingest logs in an efficient manner while evolving the schema of the table as new fields are discovered.
pub struct EvolvingWideTable {
    /// A set of column names in the table, kept in memory for quick lookup.
    col_lookup: HashSet<String>,
    /// The name of the table in the SQLite database.
    table_name: String,
}

impl EvolvingWideTable {
    /// Initializes a new `EvolvingWideTable`.
    ///
    /// This function creates a new table if it does not already exist and cleans up if needed.
    ///
    /// The function takes two arguments:
    /// * `table_name`: a string that specifies the name of the table.
    /// * `shared_connection`: a thread-safe `Arc<Mutex<Connection>>` to a SQLite database.
    ///
    /// The function returns a `Result<EvolvingWideTable, Box<dyn Error>>`. If the function is successful, it returns `Ok(EvolvingWideTable)`. If an error occurs, it returns `Err(error)`.
    pub fn new(
        table_name: String,
        shared_connection: Arc<Mutex<Connection>>,
    ) -> Result<EvolvingWideTable, EvolvingWideTableInstantiationError> {
        // Create the table
        let create_query = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                {RAW_UNPARSABLE_COL} TEXT
            )"
        );

        let conn = shared_connection.lock().map_err(|e| {
            EvolvingWideTableInstantiationError::LockError(format!(
                "Failed to acquire lock on shared connection: {e}"
            ))
        })?;

        conn.execute(&create_query, ())
            .map_err(|e| EvolvingWideTableInstantiationError::SqlError(Box::new(e)))?;

        let query = format!("PRAGMA table_info({table_name})");
        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| EvolvingWideTableInstantiationError::SqlError(Box::new(e)))?;
        // Use query_map on the Statement to retrieve column names
        let column_names = stmt
            .query_map([], |row| row.get(1))
            .map_err(|e| EvolvingWideTableInstantiationError::SqlError(Box::new(e)))?;

        let mut col_lookup: HashSet<String> = HashSet::new();
        for col in column_names {
            // fail instantiation if the column name is not retrievable
            let col_str: String =
                col.map_err(|e| EvolvingWideTableInstantiationError::SqlError(Box::new(e)))?;
            if col_str == ID_COLUMN {
                col_lookup.insert(col_str.clone());
            } else {
                let col_sanitized = col_str.trim_matches('`');
                col_lookup.insert(col_sanitized.to_string());
            }
        }

        Ok(EvolvingWideTable {
            col_lookup,
            table_name,
        })
    }

    const MAX_DB_WRITE_ATTEMPTS: u32 = 3;

    /// Inserts data into the table.
    ///
    /// This method is responsible for inserting data into the table. It takes a map of field names to field values and inserts a new row into the table with these values. If a field does not exist in the table, it is added.
    ///
    /// The method takes one argument:
    /// * `data`: a `HashMap<String, String>` that maps field names to field values.
    ///
    /// The method returns a `Result<(), Box<dyn Error>>`. If the method is successful, it returns `Ok(())`. If an error occurs, it returns `Err(error)`.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut data = HashMap::new();
    /// data.insert("field1".to_string(), "value1".to_string());
    /// data.insert("field2".to_string(), "value2".to_string());
    /// table.insert_data(data).unwrap();
    /// ```
    pub fn insert_data(
        &mut self,
        shared_connection: Arc<Mutex<Connection>>,
        data: HashMap<String, String>,
    ) -> Result<(), StorageInsertionError> {
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

        let conn = shared_connection
            .lock()
            .map_err(|e| StorageInsertionError::LockError(e.to_string()))?;

        let schema_alter_closure = || conn.execute_batch(&batch_stmt).map_err(|e| e.into());

        self.attempt_with_retry::<()>(
            Self::MAX_DB_WRITE_ATTEMPTS,
            Duration::from_secs(1),
            schema_alter_closure,
        )
        .map_err(|e| StorageInsertionError::SchemaManipulationError(e))?;

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

        let insertion_closure = || {
            conn.execute(
                &insert_stmt,
                sqlite_vals
                    .iter()
                    .map(|v| v as &dyn rusqlite::ToSql)
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .map_err(|e| e.into())
        };

        self.attempt_with_retry::<usize>(
            Self::MAX_DB_WRITE_ATTEMPTS,
            Duration::from_secs(1),
            insertion_closure,
        )
        .map_err(|e| StorageInsertionError::RecordInsertionError(e))?;

        Ok(())
    }

    fn attempt_with_retry<T>(
        &self,
        max_attempts: u32,
        sleep_duration: Duration,
        mut f: impl FnMut() -> Result<T, Box<dyn Error>>,
    ) -> Result<T, Box<dyn Error>>
    where
        T: Debug,
    {
        let mut attempt = 0;
        let mut result = f();
        while attempt < max_attempts && result.is_err() {
            attempt += 1;
            println!("Attempt {} failed due to {:?}", attempt, result);
            sleep(sleep_duration);
            result = f();
        }
        result
    }
}
