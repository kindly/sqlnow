mod api;
mod excel;
mod json;
mod session;

pub use session::{
    absolute_uri, default_name_and_check, exec_sql, input_into_parts, list_sessions, local_db_path,
    register_session,
    parse_legacy_sidecar, parse_table_filter, quote_ident, quote_literal, random_id, sidecar_path,
    validate_name, HistoryEntry, Session, SessionError, StoredQuery, StoredSession,
};

use duckdb::arrow::array::Array;
use excel::load_xlsx;
use json::load_json;
use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{
    error::Error, get, post, web, web::ServiceConfig, HttpResponse, Responder, web::Bytes
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use async_stream::stream;
use csv::WriterBuilder;
use duckdb::Connection;
use eyre::Result;
use include_dir::{include_dir, Dir};
use serde::{Deserialize, Serialize};
use serde_json::{self, json};
use std::collections::HashMap;
use std::{sync::Arc, vec};
use tokio::sync::Mutex;
use duckdb::types::{ListType, ValueRef};

static STATIC_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/static");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbType {
    Postgres,
    Sqlite,
    DuckDb,
}

/// Identify a local database file by its magic bytes: DuckDB files carry
/// "DUCK" at offset 8 (after the block checksum), SQLite files start with
/// "SQLite format 3\0".
pub fn sniff_db_type(path: &str) -> Option<DbType> {
    use std::io::Read;
    let mut buf = [0u8; 16];
    let mut file = std::fs::File::open(path).ok()?;
    file.read_exact(&mut buf).ok()?;
    if &buf[8..12] == b"DUCK" {
        return Some(DbType::DuckDb);
    }
    if buf.starts_with(b"SQLite format 3\0") {
        return Some(DbType::Sqlite);
    }
    None
}


#[derive(Debug, Clone, Default)]
pub struct Input {
    pub name: String,
    pub uri: String,
    /// Only expose these tables (empty = all).
    pub tables: Vec<String>,
    /// Never expose these tables (applied after `tables`).
    pub except: Vec<String>,
}

impl Input {
    pub fn is_database(&self) -> bool {
        self.uri.starts_with("postgresql://")
            || self.uri.starts_with("sqlite://")
            || self.uri.ends_with(".db")
            || self.uri.ends_with(".sqlite")
            || self.uri.ends_with(".duckdb")
            || self.uri.ends_with(".ddb")
    }
    pub fn db_type(&self) -> DbType {
        if self.uri.starts_with("postgresql://") {
            DbType::Postgres
        } else if self.uri.starts_with("sqlite://") {
            DbType::Sqlite
        } else if self.uri.ends_with(".duckdb") || self.uri.ends_with(".ddb") {
            DbType::DuckDb
        } else if self.uri.ends_with(".db") || self.uri.ends_with(".sqlite") {
            // .db is used by both formats, so the file's own header decides
            sniff_db_type(&self.uri).unwrap_or(DbType::Sqlite)
        } else {
            unreachable!()
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database: Option<String>,
    pub views: Vec<Input>,
    pub tables: Vec<Input>,
    pub drop:bool,
    pub all_text:bool,
    /// id used to scope browser-side state (query history) to this session
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DBTable {
    catalog: String,
    schema: String,
    name: String,
    //table_type: String,
}

#[derive(Debug, Clone, Serialize)]
struct DBColumns {
    catalog: String,
    schema: String,
    name: String,
    column_name: String,
    data_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Tab {
    name: String,
    tab_type: String,
    schema: Option<TableMeta>,
    section: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableMeta {
    catalog: String,
    schema: String,
    name: String,
    db_name: String,
    schema_display_name: String,
    fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableData {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// What the server holds for the life of a run.
///
/// Deliberately almost nothing: the tab list, the attaches to replay and the
/// browser-state scope are all questions with answers in the database or the
/// session file, so they are asked per request rather than remembered. What is
/// left is the in-memory database itself (which nothing can re-derive, since
/// there is no file behind it), where a main database lives if there is one,
/// and how to reach the session.
#[derive(Clone)]
pub struct AppData {
    pub connection: Option<Arc<Mutex<Connection>>>,
    pub db: Option<String>,
    /// From --text: how a later input is attached. Startup configuration
    /// rather than derived state, so it cannot disagree with anything.
    pub all_text: bool,
    /// The session sidecar store (queries, history, inputs). The mutex
    /// serializes this server's own sidecar operations; the state itself
    /// lives in the sidecar database.
    pub session: Arc<std::sync::Mutex<Session>>,
    /// Bumped on every server-side session mutation; combined with the
    /// sidecar mtime it drives the /api/events change stream.
    pub session_version: Arc<std::sync::atomic::AtomicU64>,
}

pub fn get_app_data(config: Config, session: Arc<std::sync::Mutex<Session>>) -> Result<AppData> {
    let mut db = None;

    let connection = match config.database.clone() {
        Some(db_path) => {
            db = Some(db_path.clone());
            Connection::open(db_path).unwrap()
        }
        None => {
            Connection::open_in_memory().unwrap()
        }
    };


    connection
        .execute_batch(
            "INSTALL parquet; LOAD parquet; 
                 INSTALL httpfs; LOAD https; 
                 INSTALL aws; LOAD aws; 
                 INSTALL postgres; LOAD postgres;
                 INSTALL sqlite; LOAD sqlite;
                 INSTALL mysql; LOAD mysql;
                 SET GLOBAL sqlite_all_varchar = true;
                 ",
        )?;

    if config.drop {
        for input in config.tables.iter().chain(config.views.iter()) {
            if input.is_database() {
                continue;
            }
            if input.uri.ends_with(".xlsx") || input.uri.ends_with(".json") || input.uri.ends_with(".jsonl") {
                continue;
            }
            match connection.execute_batch(&format!("DROP TABLE IF EXISTS \"{}\";", input.name)) {
                Ok(_) => {}
                Err(e) => {if !e.to_string().contains("Catalog Error") {
                    return Err(e.into());
                }}
            }
            match connection.execute_batch(&format!("DROP VIEW IF EXISTS \"{}\";", input.name)) {
                Ok(_) => {}
                Err(e) => {if !e.to_string().contains("Catalog Error") {
                    return Err(e.into());
                }}
            }
        }
    }

    let mut databases = HashMap::new();
    for (kind, input) in config
        .views
        .iter()
        .map(|input| ("view", input))
        .chain(config.tables.iter().map(|input| ("table", input)))
    {
        if attach_input(&connection, kind, input, config.all_text, config.drop)?.is_some() {
            databases.insert(input.name.clone(), input.clone());
        }
    }

    // derived here only to fail loudly at startup; every later reader derives
    // it again for itself
    let (tabs, _) = derive_catalog(&connection, &databases)?;
    if tabs.len() == 1 {
        return Err(eyre::eyre!("No tables found"));
    }

    Ok(AppData {
        connection: if db.is_none() {Some(Arc::new(Mutex::new(connection)))} else {None},
        db: db,
        all_text: config.all_text,
        session,
        session_version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    })
}

/// Attach one input to a connection: an ATTACH for a database, a view or a
/// table for a file. Returns the ATTACH statement when there was one, because
/// that has to be replayed on every later connection to the main database.
///
/// Startup runs this over the inputs it was given; [`add_input`] runs it for
/// one more while the server is up. Both take the same path, so an input
/// attached later behaves exactly like one named on the command line.
fn attach_input(
    connection: &Connection,
    kind: &str,
    input: &Input,
    all_text: bool,
    drop: bool,
) -> Result<Option<String>> {
    let all_varchar = if all_text { ", all_varchar = true" } else { "" };
    if kind == "table" {
        if input.is_database() {
            return Err(eyre::eyre!("External database not yet supported for tables"));
        }
        if input.uri.ends_with(".csv") {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM read_csv({}, header = true{all_varchar});",
                    quote_ident(&input.name), quote_literal(&input.uri)
                ))?
        } else if input.uri.ends_with(".parquet") {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM read_parquet({});",
                    quote_ident(&input.name), quote_literal(&input.uri)
                ))?
        } else if input.uri.ends_with(".xlsx") {
            load_xlsx(&input.uri, &input.name, &input.tables, drop, &connection)?;
        } else if input.uri.ends_with(".json") || input.uri.ends_with(".jsonl") {
            load_json(&input.uri, &input.name, &input.tables, drop, &connection)?;
        } else {
            return Err(eyre::eyre!(
                "Don't know how to load \"{}\" as a table — expected a .parquet/.csv/.xlsx/.json/.jsonl file",
                input.uri
            ));
        }
        return Ok(None);
    }
    let mut attached = None;
    if input.is_database() {
        let sql = attach_statement(input);
        connection.execute_batch(&sql)?;
        attached = Some(sql);
    } else {
        if input.uri.ends_with(".csv") {
            connection
                .execute_batch(&format!(
                    "CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM read_csv({}, header = true{all_varchar});",
                    quote_ident(&input.name), quote_literal(&input.uri)
                ))?;
        } else if input.uri.ends_with(".parquet") {
            connection
                .execute_batch(&format!(
                    "CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM read_parquet({});",
                    quote_ident(&input.name), quote_literal(&input.uri)
                ))?;
        } else if input.uri.ends_with(".xlsx") {
            return Err(eyre::eyre!("XLSX not supported for views"));
        } else if input.uri.ends_with(".json") || input.uri.ends_with(".jsonl") {
            return Err(eyre::eyre!("json not supported for views"));
        } else {
            return Err(eyre::eyre!(
                "Don't know how to load \"{}\" — expected a database (a .duckdb/.sqlite/.db file, \
                 sqlite:// or postgresql:// URI) or a .parquet/.csv/.xlsx/.json/.jsonl file",
                input.uri
            ));
        }
    }
    Ok(attached)
}

/// Build the tab list from what the connection can actually see, applying each
/// database input's --only/--except filters.
///
/// The catalog is derived, never accumulated: after attaching or detaching an
/// input, re-running this is what makes the change visible, by exactly the
/// same route as at startup.
fn derive_catalog(
    connection: &Connection,
    databases: &HashMap<String, Input>,
) -> Result<(Vec<Tab>, Vec<String>)> {
    // compile the --only/--except patterns once, failing fast on bad ones
    let mut table_filters: HashMap<String, (Vec<regex::Regex>, Vec<regex::Regex>)> = HashMap::new();
    for (name, input) in databases {
        table_filters.insert(
            name.clone(),
            (
                compile_table_filters(&input.tables)?,
                compile_table_filters(&input.except)?,
            ),
        );
    }

    let mut tabs = vec![];

    tabs.push(Tab{
        name: "query".to_string(),
        tab_type: "query".to_string(),
        schema: None,
        section: None,
    });

    tabs.push(Tab{
        name: "history".to_string(),
        tab_type: "history".to_string(),
        schema: None,
        section: None,
    });

    let mut prepared = connection
        .prepare("select table_catalog, table_schema, table_name from information_schema.tables 
                       where table_schema not in ('information_schema', 'pg_catalog')")?;

    let db_tables = prepared.query_map([], |row| {
        Ok(DBTable {
            schema: row.get(1)?,
            name: row.get(2)?,
            catalog: row.get(0)?,
        })
    })?;

    let mut prepared = connection
        .prepare("select table_catalog, table_schema, table_name, column_name, data_type from 
                      information_schema.columns 
                      where table_schema not in ('information_schema', 'pg_catalog')")?;

    let db_columns: Vec<_> = prepared.query_map([], |row| {
        Ok(DBColumns {
            schema: row.get(1)?,
            name: row.get(2)?,
            catalog: row.get(0)?,
            column_name: row.get(3)?,
            data_type: row.get(4)?,
        })
    })?.collect();
    
    for row in db_tables {
        let t = row.expect("should be able to get table");
        let mut fields = vec![];

        let external_database = databases.get(&t.catalog);

        if let Some(external_database) = external_database {
            if t.catalog == external_database.name {
                if let Some((only, except)) = table_filters.get(&external_database.name) {
                    if !only.is_empty() && !any_filter_matches(only, &t.name) {
                        continue;
                    }
                    if any_filter_matches(except, &t.name) {
                        continue;
                    }
                }
            }
        }

        // let mut prepared = connection
        //     .prepare("select column_name, data_type from information_schema.columns where table_catalog = ? and table_schema = ? and table_name = ?")?;
        // let iter = prepared
        //     .query_map(params![t.catalog, t.schema, t.name], |row| {
        //         Ok((row.get(0)?, row.get(1)?))
        //     })?;

        for row in db_columns.iter() {
            let col = row.as_ref().expect("should be able to get column");

            if col.catalog == t.catalog && col.schema == t.schema && col.name == t.name {
                fields.push((col.column_name.clone(), col.data_type.clone()));
            }
        }

        let schema = if t.schema == "main" && !external_database.is_some() {
            "".to_string()
        } else {
            t.schema
        };

        let schema_display_name = if external_database.is_some() {
            match external_database.unwrap().db_type() {
                DbType::Postgres => {
                    if schema == "public" {
                        t.catalog.clone()
                    } else {
                        format!("{}.{}", t.catalog, schema)
                    }
                }
                DbType::Sqlite | DbType::DuckDb => {
                    if schema == "main" {
                        t.catalog.clone()
                    } else {
                        format!("{}.{}", t.catalog, schema)
                    }
                }
            }
        } else {
            schema.clone()
        };

        let mut db_name = String::new();
        if external_database.is_some() {
            db_name.push_str(&format!("\"{}\".", t.catalog));
        }

        if external_database.is_some() || !schema.is_empty() {

            if external_database.is_some() {
                let db_type = external_database.unwrap().db_type();

                match db_type {
                    DbType::Postgres => {
                        if schema != "public" {
                            db_name.push_str(&format!("\"{}\".", schema));
                        }
                    }
                    DbType::Sqlite | DbType::DuckDb => {
                        if schema != "main" {
                            db_name.push_str(&format!("\"{}\".", schema));
                        }
                    }
                }
            } else {
                db_name.push_str(&format!("\"{}\".", schema));
            }

        }
        db_name.push_str(&format!("\"{}\"", t.name));


        if fields.is_empty() {
            continue;
        }

        let section = if schema_display_name.is_empty() {
            None
        } else {
            Some(schema_display_name.clone())
        };

        let table_meta = TableMeta {
            catalog: t.catalog,
            schema,
            name: t.name,
            db_name,
            schema_display_name,
            fields,
        };

        tabs.push(Tab {
            name: table_meta.db_name.clone().replace("\"", ""),
            tab_type: "table".to_string(),
            schema: Some(table_meta),
            section
        });
    };

    if tabs.len() == 1 {
        return Err(eyre::eyre!("No tables found"));
    }

    tabs.sort_by(|a, b| a.name.cmp(&b.name));

    let mut section_list = tabs.iter().filter_map(|t| t.section.clone()).collect::<Vec<String>>();
    section_list.sort();
    section_list.dedup();

    Ok((tabs, section_list))
}

/// The database inputs this session records, by name.
///
/// Read from the session rather than remembered, so an input added by anything
/// — this server, an agent through `sqlnow exec`, another process — is picked
/// up without a restart. Their --only/--except filters travel with them.
fn recorded_databases(app_data: &AppData) -> HashMap<String, Input> {
    let session = match app_data.session.lock() {
        Ok(session) => session,
        Err(_) => return HashMap::new(),
    };
    session
        .list_inputs()
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, input)| input.is_database())
        .map(|(_, input)| (input.name.clone(), input))
        .collect()
}

/// The tabs and sections as the database sees them right now.
///
/// Two `information_schema` queries, ~10ms with a database attached, which is
/// far cheaper than the class of bug a remembered copy invites.
async fn current_catalog(app_data: &AppData) -> Result<(Vec<Tab>, Vec<String>)> {
    let databases = recorded_databases(app_data);
    let mutexed = catalog_connection(app_data).await?;
    let connection = mutexed.lock().await;
    derive_catalog(&connection, &databases)
}

/// A connection to run a catalog change on: the shared in-memory one, or a
/// fresh connection to the main database with the existing attaches replayed.
async fn catalog_connection(app_data: &AppData) -> Result<Arc<Mutex<Connection>>> {
    if let Some(connection) = &app_data.connection {
        return Ok(connection.clone());
    }
    if app_data.db.is_some() {
        return Ok(Arc::new(Mutex::new(fresh_db_connection(app_data))));
    }
    Err(eyre::eyre!("no database connection"))
}

/// Attach an input to the running server and make it visible.
///
/// The same path startup takes, so a table added here is indistinguishable
/// from one named on the command line. With a main database the new view or
/// table is written into that file and so outlives the run; an attached
/// database is recorded for replay on later connections instead.
pub async fn add_input(app_data: &AppData, kind: &str, input: &Input) -> Result<()> {
    let databases = recorded_databases(app_data);
    if databases.contains_key(&input.name) {
        return Err(eyre::eyre!(
            "\"{}\" is already attached — remove it first to replace it",
            input.name
        ));
    }

    let mutexed = catalog_connection(app_data).await?;
    let connection = mutexed.lock().await;
    let (tabs, _) = derive_catalog(&connection, &databases)?;
    if tabs.iter().any(|tab| tab.name == input.name) {
        return Err(eyre::eyre!(
            "\"{}\" is already attached — remove it first to replace it",
            input.name
        ));
    }

    attach_input(&connection, kind, input, app_data.all_text, false)?;
    // nothing to update: the caller records the input in the session, and the
    // next reader derives the catalog again
    Ok(())
}

/// Detach an input and make it disappear.
///
/// A view or table is dropped, which with a main database removes it from that
/// file for good; an attached database is only detached, and the file it points
/// at is untouched.
pub async fn remove_input(app_data: &AppData, name: &str) -> Result<()> {
    let databases = recorded_databases(app_data);
    let is_database = databases.contains_key(name);

    let mutexed = catalog_connection(app_data).await?;
    let connection = mutexed.lock().await;
    if is_database {
        connection.execute_batch(&format!("DETACH {};", quote_ident(name)))?;
    } else {
        let (tabs, _) = derive_catalog(&connection, &databases)?;
        if !tabs.iter().any(|tab| tab.name == name) {
            return Err(eyre::eyre!("nothing named \"{}\" is attached", name));
        }
        // a view or a table: which one is not worth tracking, so try both
        let view = format!("DROP VIEW IF EXISTS {};", quote_ident(name));
        if connection.execute_batch(&view).is_err() {
            connection.execute_batch(&format!("DROP TABLE IF EXISTS {};", quote_ident(name)))?;
        }
    }
    // the caller forgets the input in the session, which is what stops a later
    // connection from attaching it again
    Ok(())
}

/// Compile table filter patterns: fully anchored regular expressions, so a
/// plain table name matches exactly that table and `entity_.*` works as
/// expected. Invalid patterns fail at startup with a clear error.
fn compile_table_filters(patterns: &[String]) -> Result<Vec<regex::Regex>> {
    patterns
        .iter()
        .map(|pattern| {
            regex::Regex::new(&format!("^(?:{})$", pattern))
                .map_err(|e| eyre::eyre!("invalid table filter pattern \"{}\": {}", pattern, e))
        })
        .collect()
}

fn any_filter_matches(filters: &[regex::Regex], name: &str) -> bool {
    filters.iter().any(|filter| filter.is_match(name))
}

/// The ATTACH statement for a database input (only valid when
/// `input.is_database()`).
fn attach_statement(input: &Input) -> String {
    let connection_string = input.uri.strip_prefix("sqlite://").unwrap_or(&input.uri);
    let uri = quote_literal(connection_string);
    let name = quote_ident(&input.name);
    match input.db_type() {
        DbType::Postgres => format!("ATTACH IF NOT EXISTS {} AS {} (TYPE POSTGRES)", uri, name),
        DbType::Sqlite => format!("ATTACH IF NOT EXISTS {} AS {} (TYPE SQLITE)", uri, name),
        DbType::DuckDb => format!("ATTACH IF NOT EXISTS {} AS {}", uri, name),
    }
}

/// Inputs recorded in the file's own `inputs` table, when the file is a
/// session database. Non-session duckdb files simply have no such table.
fn own_inputs(conn: &Connection) -> Vec<Input> {
    // except_tables was added later; older session files lack the column
    let mut stmt = match conn
        .prepare("SELECT name, uri, tables, except_tables FROM inputs")
        .or_else(|_| conn.prepare("SELECT name, uri, tables, NULL FROM inputs"))
    {
        Ok(stmt) => stmt,
        Err(_) => return vec![],
    };
    let rows = match stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        let uri: String = row.get(1)?;
        let table_list: duckdb::types::Value = row.get(2)?;
        let except_list: duckdb::types::Value = row.get(3)?;
        Ok(Input {
            name,
            uri,
            tables: session::table_list_from_value(table_list),
            except: session::table_list_from_value(except_list),
        })
    }) {
        Ok(rows) => rows,
        Err(_) => return vec![],
    };
    rows.filter_map(|r| r.ok()).collect()
}

/// Run SQL directly against a DuckDB database file, without a server.
/// Recorded inputs are replayed first so names resolve the same way they do
/// in the UI: database attaches (and file views, as temporary views) come
/// from the file's own `inputs` table when it is a session file, and from
/// its `<db>.sqlnow` sidecar when one exists. Single statements return
/// rows; multi-statement batches return an empty result. Writes persist —
/// this is the CLI path for agents doing database work through sqlnow
/// alone.
pub fn query_database(db_path: &str, sql: &str, limit: usize) -> Result<TableData> {
    let path = std::path::Path::new(db_path);
    if !path.exists() {
        return Err(eyre::eyre!("Database {} does not exist", db_path));
    }
    if sniff_db_type(db_path) != Some(DbType::DuckDb) {
        return Err(eyre::eyre!("{} is not a DuckDB database file", db_path));
    }

    let conn = session::open_with_retry(path).map_err(|e| eyre::eyre!("{}", e))?;

    let mut inputs = own_inputs(&conn);

    let sidecar = sidecar_path(db_path);
    if sidecar.exists() {
        let session = Session::open(&sidecar)?;
        inputs.extend(
            session
                .list_inputs()
                .map_err(|e| eyre::eyre!("{}", e))?
                .into_iter()
                .map(|(_, input)| input),
        );
    }
    inputs.dedup_by(|a, b| a.name == b.name);

    if inputs.iter().any(|input| input.is_database()) {
        // best effort — extensions may already be loaded, or we may be offline
        for stmt in [
            "INSTALL sqlite; LOAD sqlite;",
            "INSTALL postgres; LOAD postgres;",
            "SET GLOBAL sqlite_all_varchar = true;",
        ] {
            let _ = conn.execute_batch(stmt);
        }
    }

    for input in &inputs {
        let replay = if input.is_database() {
            attach_statement(input)
        } else if input.uri.ends_with(".csv") {
            // temporary views: never written into the target file
            format!(
                "CREATE TEMPORARY VIEW IF NOT EXISTS {} AS SELECT * FROM read_csv({}, header = true);",
                quote_ident(&input.name), quote_literal(&input.uri)
            )
        } else if input.uri.ends_with(".parquet") {
            format!(
                "CREATE TEMPORARY VIEW IF NOT EXISTS {} AS SELECT * FROM read_parquet({});",
                quote_ident(&input.name), quote_literal(&input.uri)
            )
        } else {
            // xlsx/json inputs are loaded as tables by the server; there is
            // nothing lightweight to replay here
            continue;
        };
        if let Err(e) = conn.execute_batch(&replay) {
            eprintln!("warning: could not replay input {}: {}", input.name, e);
        }
    }

    match run_query(sql, &conn, limit) {
        Ok(table_data) => Ok(table_data),
        Err(prepare_error) => {
            // multi-statement input (a genuine error surfaces identically here)
            conn.execute_batch(sql).map_err(|_| prepare_error)?;
            Ok(TableData { headers: vec![], rows: vec![] })
        }
    }
}

/// Open a new connection to the main database, replaying the attaches and
/// settings that do not survive across connections.
fn fresh_db_connection(app_data: &AppData) -> Connection {
    let connection = Connection::open(app_data.db.clone().unwrap()).unwrap();
    // regenerated from what the session records rather than remembered: an
    // attach added since this server started still takes effect here
    let mut replay = vec!["SET GLOBAL sqlite_all_varchar = true;".to_string()];
    replay.extend(recorded_databases(app_data).values().map(attach_statement));
    for sql in &replay {
        if let Err(e) = connection.execute_batch(sql) {
            eprintln!("Failed to replay `{}` on new connection: {}", sql, e);
        }
    }
    connection
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inputs_attach_and_detach_while_the_server_runs() {
        let dir = std::env::temp_dir().join(format!("sqlnow-attach-test-{}", random_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let first = dir.join("plants.csv");
        std::fs::write(&first, "name,co2\nPlant A,120\n").unwrap();
        let second = dir.join("units.csv");
        std::fs::write(&second, "name,mw\nUnit 1,50\n").unwrap();

        let session = Session::open(&dir.join("session.sqlnow")).unwrap();
        let view = |path: &std::path::Path, name: &str| Input {
            name: name.to_string(),
            uri: path.to_string_lossy().to_string(),
            tables: vec![],
            except: vec![],
        };
        let app_data = get_app_data(
            Config {
                database: None,
                views: vec![view(&first, "plants")],
                tables: vec![],
                drop: false,
                all_text: false,
                scope: None,
            },
            Arc::new(std::sync::Mutex::new(session)),
        )
        .unwrap();

        // asked of the database, never of a remembered copy
        async fn table_names(app_data: &AppData) -> Vec<String> {
            let (tabs, _) = current_catalog(app_data).await.unwrap();
            let mut names: Vec<String> = tabs
                .iter()
                .filter(|tab| tab.tab_type == "table")
                .map(|tab| tab.name.clone())
                .collect();
            names.sort();
            names
        }

        actix_web::rt::System::new().block_on(async {
            assert_eq!(table_names(&app_data).await, vec!["plants"]);

            add_input(&app_data, "view", &view(&second, "units")).await.unwrap();
            assert_eq!(table_names(&app_data).await, vec!["plants", "units"]);

            // the same name twice is refused rather than silently ignored
            let clash = add_input(&app_data, "view", &view(&second, "units")).await;
            assert!(clash.is_err(), "attaching a name twice should fail");

            remove_input(&app_data, "units").await.unwrap();
            assert_eq!(table_names(&app_data).await, vec!["plants"]);

            assert!(
                remove_input(&app_data, "units").await.is_err(),
                "detaching something absent should fail"
            );
        });
    }

    #[test]
    fn query_database_replays_a_session_files_own_inputs() {
        let dir = std::env::temp_dir().join(format!("sqlnow-qd-test-{}", random_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let csv = dir.join("plants.csv");
        std::fs::write(&csv, "name,co2\nPlant A,120\nPlant B,340\n").unwrap();

        let session_path = dir.join("session.sqlnow");
        let session = Session::open(&session_path).unwrap();
        session
            .set_inputs(&[(
                "view".to_string(),
                Input {
                    name: "plants".to_string(),
                    uri: csv.to_string_lossy().to_string(),
                    tables: vec![],
                    except: vec![],
                },
            )])
            .unwrap();
        drop(session);

        // the recorded view resolves by name, exactly as in the UI
        let table_data = query_database(
            &session_path.to_string_lossy(),
            "SELECT count(*) FROM plants",
            10,
        )
        .unwrap();
        assert_eq!(table_data.rows[0][0], "2");

        // and the temporary view was not persisted into the session file
        let session = Session::open(&session_path).unwrap();
        let stored = session.raw_sql("SELECT count(*) FROM duckdb_views() WHERE NOT internal").unwrap();
        assert_eq!(stored.rows[0][0], "0");
    }

    #[test]
    fn table_filters_are_anchored_regexes() {
        let filters = compile_table_filters(&["users".into(), "entity_.*".into()]).unwrap();
        // plain names match exactly, not as substrings
        assert!(any_filter_matches(&filters, "users"));
        assert!(!any_filter_matches(&filters, "users_archive"));
        assert!(!any_filter_matches(&filters, "superusers"));
        // patterns work
        assert!(any_filter_matches(&filters, "entity_statement"));
        assert!(!any_filter_matches(&filters, "person_statement"));
        // invalid patterns fail loudly with the pattern in the message
        let err = compile_table_filters(&["cost (usd".into()]).unwrap_err();
        assert!(err.to_string().contains("cost (usd"));
    }

    #[test]
    fn timestamps_dates_and_times_format_as_text() {
        let conn = Connection::open_in_memory().unwrap();
        let table_data = run_query(
            "SELECT TIMESTAMP '2024-03-31 12:34:56.789' AS ts,
                    TIMESTAMPTZ '2024-03-31 12:34:56+00' AS tstz,
                    DATE '1999-12-31' AS d,
                    TIME '01:02:03' AS t,
                    TIMESTAMP '1969-12-31 23:00:00' AS pre_epoch",
            &conn,
            10,
        )
        .unwrap();
        let row = &table_data.rows[0];
        assert_eq!(row[0], "2024-03-31 12:34:56.789000");
        assert_eq!(row[1], "2024-03-31 12:34:56");
        assert_eq!(row[2], "1999-12-31");
        assert_eq!(row[3], "01:02:03");
        assert_eq!(row[4], "1969-12-31 23:00:00");
    }
}

pub fn main_web(service_config: &mut ServiceConfig) {
    service_config
       .service(sql_query)
       .service(static_files)
       .service(tables)
       .service(table)
       .service(outputs)
       .configure(api::configure)
       .default_service(web::get().to(ui));
}

fn process_row(row: &duckdb::Row, headers: &Vec<String>) -> Result<Vec<String>> {
    let mut data = vec![];
    for i in 0..headers.len() {
        let value =  match row.get_ref(i).unwrap() {
            ValueRef::Null => "".to_string(),
            ValueRef::Boolean(bool) => bool.to_string(),
            ValueRef::TinyInt(int) => int.to_string(),
            ValueRef::SmallInt(int) => int.to_string(),
            ValueRef::Int(int) => int.to_string(),
            ValueRef::BigInt(int) => int.to_string(),
            ValueRef::HugeInt(int) => int.to_string(),
            ValueRef::UTinyInt(int) => int.to_string(),
            ValueRef::USmallInt(int) => int.to_string(),
            ValueRef::UInt(int) => int.to_string(),
            ValueRef::UBigInt(int) => int.to_string(),
            ValueRef::Float(float) => float.to_string(),
            ValueRef::Double(double) => double.to_string(),
            ValueRef::Decimal(decimal) => decimal.to_string(),
            ValueRef::Timestamp(unit, value) => format_timestamp(unit, value),
            ValueRef::Text(text) => String::from_utf8_lossy(text).to_string(),
            ValueRef::Blob(blob) => String::from_utf8_lossy(blob).to_string(),
            ValueRef::Date32(days) => format_date(days as i64),
            ValueRef::Time64(unit, value) => {
                format_time_micros(to_micros(unit, value).rem_euclid(86_400_000_000))
            }
            ValueRef::List(array,_) => {
                match array {
                    ListType::Regular(array) => {
                        let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())?;
                        let mut buffer = String::new();
                        for i in 0..array.len() {
                            formatter.value(i).write(&mut buffer).unwrap()
                        }
                        buffer
                    }
                    ListType::Large(array) => {
                        let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())?;
                        let mut buffer = String::new();
                        for i in 0..array.len() {
                            formatter.value(i).write(&mut buffer).unwrap()
                        }
                        buffer
                    }
                }

            },
            _ => panic!("Type not supported"),
        };
        data.push(value)
    }
    Ok(data)
}

/// days since 1970-01-01 → (year, month, day); Howard Hinnant's civil_from_days
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn format_date(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn format_time_micros(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let sub = micros % 1_000_000;
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if sub == 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}:{:02}.{:06}", h, m, s, sub)
    }
}

fn to_micros(unit: duckdb::types::TimeUnit, value: i64) -> i64 {
    match unit {
        duckdb::types::TimeUnit::Second => value * 1_000_000,
        duckdb::types::TimeUnit::Millisecond => value * 1_000,
        duckdb::types::TimeUnit::Microsecond => value,
        duckdb::types::TimeUnit::Nanosecond => value / 1_000,
    }
}

fn format_timestamp(unit: duckdb::types::TimeUnit, value: i64) -> String {
    let micros = to_micros(unit, value);
    let days = micros.div_euclid(86_400_000_000);
    let time_of_day = micros.rem_euclid(86_400_000_000);
    format!("{} {}", format_date(days), format_time_micros(time_of_day))
}

fn run_query(sql: &str, conn: &Connection, display_limit: usize) -> Result<TableData> {
    let mut headers: Vec<String> = vec![];
    let mut rows: Vec<Vec<String>> = vec![];

    let mut prepared = conn.prepare(sql)?;

    let mut db_rows = prepared.query([])?;

    let statement = db_rows.as_ref().expect("should be able to get rows");

    headers.extend(statement.column_names());

    let mut count: usize = 0;

    while let Some(row) = db_rows.next()? {
        rows.push(process_row(&row, &headers)?);
        count += 1;
        if count >= display_limit {
            break;
        }
    }

    Ok(TableData { headers, rows: rows })
}

#[get("/assets/{filename:.*}")]
async fn static_files(filename: web::Path<String>) -> Result<impl Responder, Error> {
    let data = STATIC_DIR.get_file("assets/".to_owned() + filename.as_str()).ok_or(ErrorBadRequest("file not found"))?;
    let contents = data.contents();

    let content_type = mime_guess::from_path(filename.as_str())
        .first_or_octet_stream()
        .to_string();

    return Ok(
        HttpResponse::Ok()
            .append_header(("Content-Type", content_type))
            // vite asset filenames are content-hashed, so they can be cached forever
            .append_header(("Cache-Control", "public, max-age=31536000, immutable"))
            .body(contents)
    );
}

async fn ui(app_data: web::Data<AppData>) -> Result<impl Responder, Error> {
    let data = STATIC_DIR.get_file("index.html").ok_or(ErrorBadRequest("file not found"))?;
    let mut res = data.contents_utf8().expect("utf8 file").to_string();
    // make the session scope available to the UI before any of its code runs,
    // so browser-stored state (query history) can be keyed per session
    let scope = app_data
        .session
        .lock()
        .ok()
        .filter(|session| session.is_persistent())
        .map(|session| session.id().to_string());
    if let Some(scope) = scope {
        let script = format!("<script>window.SQLNOW_SCOPE = {};</script></head>", json!(scope));
        res = res.replace("</head>", &script);
    }
    Ok(HttpResponse::Ok().body(res))
}


#[post("/tables.json")]
async fn tables(app_data: web::Data<AppData>) -> Result<impl Responder, Error> {
    let (tabs, sections) = current_catalog(&app_data)
        .await
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    let table_tabs = tabs.iter().filter(
        |t| t.tab_type == "table"
    ).collect::<Vec<&Tab>>();
    let output = json!({
        "tables": table_tabs,
        "sections": sections
    });

    Ok(HttpResponse::Ok().json(output))
}


#[derive(Debug, Clone, Deserialize, Serialize)]
struct TableRequest {
    name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TableResponse {
    table: String,
    select_star: String,
    select_fields: String,
    select_fields_type: String,
}

#[post("/table.json")]
async fn table(app_data: web::Data<AppData>, post_data: web::Form<TableRequest>) -> Result<impl Responder, Error> {
    let (tabs, _) = current_catalog(&app_data)
        .await
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    let table = tabs.iter().find(|t| t.name == post_data.name).ok_or(ErrorBadRequest("table not found"))?;

    let select_star = generate_sql(table.schema.as_ref().expect("checked"), SqlType::SelectStar);
    let select_fields = generate_sql(table.schema.as_ref().expect("checked"), SqlType::SelectFields);
    let select_fields_type = generate_sql(table.schema.as_ref().expect("checked"), SqlType::SelectFieldsType);

    Ok(HttpResponse::Ok().json(TableResponse {
        table: table.name.clone(),
        select_star,
        select_fields,
        select_fields_type
    }))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SqlRequest {
    sql: String,
    display_limit: String,
}

#[derive(Debug, Clone, Serialize)]
struct SqlResponse {
    error: Option<String>,
    table_data: TableData,
}

#[post("/query.json")]
async fn sql_query(app_data: web::Data<AppData>, post_data: web::Form<SqlRequest>) -> Result<impl Responder, Error> {
    let sql = post_data.sql.clone();

    let mutexed_connection = if app_data.connection.is_some() {
        app_data.connection.clone().unwrap()
    } else if app_data.db.is_some() {
        Arc::new(Mutex::new(fresh_db_connection(&app_data)))
    } else {
        return Err(ErrorBadRequest("No database connection"));
    };

    let conn = mutexed_connection.lock().await;

    let table_data = if sql.is_empty() {
        Ok(TableData { headers: vec![], rows: vec![] })
    } else {
        run_query(&sql.as_str(), &conn, post_data.display_limit.parse().unwrap_or(500))
    };

    // every run lands in the session history, failed ones included, so the
    // user (and any agent) can always get back to what was tried
    if !sql.trim().is_empty() {
        if let Ok(session) = app_data.session.lock() {
            if let Err(e) = session.append_history(&sql) {
                eprintln!("Failed to record query history: {}", e);
            }
        }
        app_data.session_version.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    match table_data {
        Ok(table_data) => {
            Ok(HttpResponse::Ok().json(SqlResponse {
                error: None,
                table_data,
            }))
        }
        Err(e) => {
            Ok(HttpResponse::Ok().json(SqlResponse {
                error: Some(e.to_string()),
                table_data: TableData { headers: vec![], rows: vec![] },
            }))
        }
    }


}


#[post("/outputs")]
async fn outputs(
    app_data: web::Data<AppData>,
    q: web::Form<HashMap<String, String>>,
) -> Result<impl Responder, Error> {
    let form = q.clone();

    let sql = form
        .get("sql")
        .ok_or(ErrorBadRequest("sql not found"))?
        .to_owned();

    let output_format = if form.contains_key("jsonl") {
        OutputFormat::JSON
    } else if form.contains_key("tab") {
        OutputFormat::TSV
    } else if form.contains_key("csv") {
        OutputFormat::CSV
    } else {
        OutputFormat::CSV
    };

    match output_stream(app_data, sql, output_format).await {
        Ok(res) => return Ok(res),
        Err(e) => return Err(e),
    }

}

#[derive(PartialEq, Copy, Clone)]
enum SqlType {
    SelectStar,
    SelectFields,
    SelectFieldsType,
}

/// Starter SQL offered on a table tab: select-star, an explicit field list,
/// or a field list with each column's type as a trailing comment.
fn generate_sql(schema: &TableMeta, sql_type: SqlType) -> String {
    let field_lines: Vec<String> = match sql_type {
        SqlType::SelectStar => vec!["    *".to_owned()],
        SqlType::SelectFields | SqlType::SelectFieldsType => {
            let max_field_length =
                schema.fields.iter().map(|(f, _)| f.len()).max().unwrap_or(0);
            schema
                .fields
                .iter()
                .enumerate()
                .map(|(i, (field, field_type))| {
                    let sep = if i + 1 < schema.fields.len() { "," } else { " " };
                    match sql_type {
                        SqlType::SelectFieldsType => {
                            let pad = " ".repeat(max_field_length + 4 - field.len());
                            format!("    {}{sep}{pad}-- {field_type}", quote_ident(field))
                        }
                        _ => format!("    {}{sep}", quote_ident(field)),
                    }
                })
                .collect()
        }
    };

    format!(
        "SELECT\n{}\nFROM\n    {}\nLIMIT 10000",
        field_lines.join("\n"),
        schema.db_name
    )
}

#[derive(PartialEq, Copy, Clone)]
enum OutputFormat {
    CSV,
    TSV,
    JSON,
}

async fn output_stream(
    app_data: web::Data<AppData>,
    sql: String,
    output: OutputFormat,
) -> Result<impl Responder, Error> {

    let mutexed_connection = if app_data.connection.is_some() {
        app_data.connection.clone().unwrap()
    } else if app_data.db.is_some() {
        Arc::new(Mutex::new(fresh_db_connection(&app_data)))
    } else {
        return Err(ErrorBadRequest("No database connection"));
    };

    let output_stream = stream! {

        let conn = mutexed_connection.lock().await;

        let mut prepared = conn.prepare(&sql).unwrap();
        let mut db_rows = prepared.query([]).unwrap();

        let mut headers: Vec<String> = vec![];

        let statement = db_rows.as_ref().expect("should be able to get rows");

        headers.extend(statement.column_names());

        match output {
            OutputFormat::CSV => {
                let buf = Vec::new();
                let mut writer = WriterBuilder::new().from_writer(buf);
                writer.write_record(&headers).map_err(ErrorInternalServerError)?;
                let buf = writer.into_inner().map_err(ErrorInternalServerError)?;
                yield Ok::<Bytes, Error>(Bytes::from(buf));
            }
            _ => {}
        }

        while let Some(row) = db_rows.next().map_err(ErrorInternalServerError)? {
            let row = process_row(&row, &headers).map_err(ErrorInternalServerError)?;
            let mut buf = Vec::new();
            match output {
                OutputFormat::CSV => {
                    let mut writer = WriterBuilder::new().from_writer(buf);
                    writer.write_record(row).map_err(ErrorInternalServerError)?;
                    let buf = writer.into_inner().map_err(ErrorInternalServerError)?;
                    yield Ok::<Bytes, Error>(Bytes::from(buf));
                }
                OutputFormat::TSV => {
                    let mut writer = WriterBuilder::new().delimiter(b'\t').from_writer(buf);
                    writer.write_record(row).map_err(ErrorInternalServerError)?;
                    let buf = writer.into_inner().map_err(ErrorInternalServerError)?;
                    yield Ok::<Bytes, Error>(Bytes::from(buf));
                }
                OutputFormat::JSON => {
                    let map = headers.iter().zip(row.iter()).collect::<HashMap<_, _>>();
                    serde_json::to_writer(&mut buf, &map).map_err(ErrorInternalServerError)?;
                    yield Ok::<Bytes, Error>(Bytes::from(buf));
                    yield Ok::<Bytes, Error>(Bytes::from("\n"));
                }
            }
        }
    };

    let content_disposition = match output {
        OutputFormat::CSV => "attachment; filename=download.csv",
        OutputFormat::TSV => "attachment; filename=download.tsv",
        OutputFormat::JSON => "attachment; filename=download.json",
    };

    let content_type = match output {
        OutputFormat::CSV => "text/csv",
        OutputFormat::TSV => "text/tab-separated-values",
        OutputFormat::JSON => "application/json",
    };

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Disposition", content_disposition))
        .insert_header(("Content-Type", content_type))
        .streaming(Box::pin(output_stream)))
}
