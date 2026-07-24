mod api;
mod excel;
mod json;
mod session;

pub use session::{
    absolute_uri, default_name_and_check, input_into_parts, local_db_path, parse_legacy_sidecar,
    parse_table_filter, quote_ident, quote_literal, random_id, sidecar_path, validate_name,
    HistoryEntry, Session, SessionError, StoredQuery,
};

use duckdb::arrow::array::Array;
use excel::load_xlsx;
use json::load_json;
use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{
    error::Error, get, post, web, web::ServiceConfig, Either, HttpResponse, Responder, web::Bytes
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use async_stream::stream;
use csv::WriterBuilder;
use duckdb::Connection;
use eyre::Result;
use include_dir::{include_dir, Dir, DirEntry};
use minijinja::{context, Environment};
use serde::{Deserialize, Serialize};
use serde_json::{self, json};
use std::collections::HashMap;
use std::{sync::Arc, vec};
use tokio::sync::Mutex;
use duckdb::types::{ListType, ValueRef};

static TEMPLATE_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/templates");
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


#[derive(Debug, Clone)]
pub struct Input {
    pub name: String,
    pub uri: String,
    pub tables: Vec<String>,
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

#[derive(Clone)]
pub struct AppData {
    pub config: Config,
    pub connection: Option<Arc<Mutex<Connection>>>,
    pub db: Option<String>,
    pub tabs: Vec<Tab>,
    pub sections: Vec<String>,
    pub env: Environment<'static>,
    pub scope: Option<String>,
    /// ATTACH statements and settings that only live for the lifetime of a
    /// connection. When `db` is set, requests open fresh connections, so
    /// these are replayed on each one.
    pub per_connection_sql: Vec<String>,
    /// The session sidecar store (queries, history, inputs). The mutex
    /// serializes this server's own sidecar operations; the state itself
    /// lives in the sidecar database.
    pub session: Arc<std::sync::Mutex<Session>>,
    /// Bumped on every server-side session mutation; combined with the
    /// sidecar mtime it drives the /api/events change stream.
    pub session_version: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct History {
    pub hash: HashMap<String, String>,
    pub history: Vec<String>,
}

pub fn get_app_data(config: Config, session: Arc<std::sync::Mutex<Session>>) -> Result<AppData> {
    
    let mut env = Environment::new();

    for glob in ["**/*.html", "**/*.sql"] {
        for entry in TEMPLATE_DIR.find(glob).expect("template dir should exist") {
            if let DirEntry::File(file) = entry {
                let content = file.contents_utf8().expect("utf8 file");
                let path = file.path();
                env.add_template_owned(path.to_string_lossy(), content)?;
            }
        }
    }

    env.add_filter("pad", |field: String, number: usize| {
        " ".repeat((number+4)-field.len())
    });

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

    let mut external_database_map = HashMap::new();
    let mut per_connection_sql = vec!["SET GLOBAL sqlite_all_varchar = true;".to_string()];

    let all_varchar = if config.all_text {
        ", all_varchar = true"
    } else {
        ""
    };

    for input in config.views.iter() {
        if input.is_database() {
            let sql = attach_statement(input);
            connection.execute_batch(&sql)?;
            per_connection_sql.push(sql);

            external_database_map.insert(input.name.clone(), input.clone());
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
    }


    for input in config.tables.iter() {
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
            load_xlsx(&input.uri, &input.name, &input.tables, config.drop, &connection)?;
        } else if input.uri.ends_with(".json") || input.uri.ends_with(".jsonl") {
            load_json(&input.uri, &input.name, &input.tables, config.drop, &connection)?;
        } else {
            return Err(eyre::eyre!(
                "Don't know how to load \"{}\" as a table — expected a .parquet/.csv/.xlsx/.json/.jsonl file",
                input.uri
            ));
        }
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

        let external_database = external_database_map.get(&t.catalog);

        if let Some(external_database) = external_database {
            if !external_database.tables.is_empty() {
                if t.catalog == external_database.name {
                    if !external_database.tables.contains(&t.name) {
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

    let scope = config.scope.clone();

    Ok(AppData {
        config,
        connection: if db.is_none() {Some(Arc::new(Mutex::new(connection)))} else {None},
        db: db,
        tabs,
        sections: section_list,
        env,
        scope,
        per_connection_sql,
        session,
        session_version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    })
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
    let mut stmt = match conn.prepare("SELECT name, uri, tables FROM inputs") {
        Ok(stmt) => stmt,
        Err(_) => return vec![],
    };
    let rows = match stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        let uri: String = row.get(1)?;
        let table_list: duckdb::types::Value = row.get(2)?;
        Ok(Input {
            name,
            uri,
            tables: session::table_list_from_value(table_list),
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
    for sql in &app_data.per_connection_sql {
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
       .service(post_sql)
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
    if let Some(scope) = &app_data.scope {
        let script = format!("<script>window.SQLNOW_SCOPE = {};</script></head>", json!(scope));
        res = res.replace("</head>", &script);
    }
    Ok(HttpResponse::Ok().body(res))
}


#[post("/tables.json")]
async fn tables(app_data: web::Data<AppData>) -> Result<impl Responder, Error> {
    let table_tabs = app_data.tabs.iter().filter(
        |t| t.tab_type == "table"
    ).collect::<Vec<&Tab>>();
    let output = json!({
        "tables": table_tabs,
        "sections": app_data.sections.clone()
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
    let table = app_data.tabs.iter().find(|t| t.name == post_data.name).ok_or(ErrorBadRequest("table not found"))?;

    let select_star = generate_sql(&app_data, table.schema.as_ref().expect("checked"), SqlType::SelectStar);
    let select_fields = generate_sql(&app_data, table.schema.as_ref().expect("checked"), SqlType::SelectFields);
    let select_fields_type = generate_sql(&app_data, table.schema.as_ref().expect("checked"), SqlType::SelectFieldsType);

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

// #[get("/")]
// async fn ui(app_data: web::Data<AppData>) -> Result<impl Responder, Error> {
//     let tmpl = app_data
//         .env
//         .get_template("layout.html")
//         .expect("template exists");

//     let current_tab = app_data.tabs.get(0).expect("at least one table should exists");

//     let sql = "";

//     let res = tmpl
//         .render(&context! {
//             current_tab => current_tab,
//             tabs => app_data.tabs,
//             sql => sql,
//             display_limit => "500",
//             sections => app_data.sections
//         })
//         .map_err(|e| ErrorInternalServerError(e))?;

//     Ok(HttpResponse::Ok().body(res))
// }

#[derive(PartialEq, Copy, Clone)]
enum SqlType {
    SelectStar,
    SelectFields,
    SelectFieldsType,
}

fn generate_sql(app_data: &AppData, schema: &TableMeta, sql_type: SqlType) -> String {
    let template = match sql_type {
        SqlType::SelectStar => "select_star.sql",
        SqlType::SelectFields => "table_schema.sql",
        SqlType::SelectFieldsType => "table_with_types.sql",
    };

    let max_field_length = schema.fields.iter().map(|(f, _)| f.len()).max().unwrap_or(0);

    let sql_tmpl = app_data
        .env
        .get_template(template)
        .expect("template exists");

    sql_tmpl
        .render(&context! {
            schema  => schema,
            max_field_length => max_field_length,
        })
        .expect("should render")
}




#[post("/")]
async fn post_sql(
    app_data: web::Data<AppData>,
    q: web::Form<HashMap<String, String>>,
) -> Result<Either<impl Responder, impl Responder>, Error> {
    let form = q.clone();

    let current_tab_name = form
        .get("current_tab")
        .ok_or(ErrorBadRequest("current_tab not found"))?;

    let current_tab = app_data
        .tabs
        .iter()
        .find(|t| t.name == *current_tab_name)
        .unwrap();

    let display_limit = form
        .get("display_limit")
        .unwrap_or(&"500".to_string())
        .parse::<usize>()
        .unwrap_or(1000);

    let mut other_sql = HashMap::new();
    for (key, value) in form.iter() {
        if key.starts_with("sql-") {
            other_sql.insert(key.to_owned(), value.to_owned());
        }
    }

    let mut sql = match other_sql.remove(&format!("sql-{current_tab_name}")) {
        Some(sql) => sql.to_owned(),
        None => {
            if let Some(schema) = current_tab.schema.as_ref() {
                generate_sql(&app_data, schema, SqlType::SelectFields)
            } else { 
                "".to_owned()
            }
        },
    };

    if let Some(new_sql) = form.get("new_sql") {
        if current_tab.schema.is_some() {
            if new_sql == "select_star" {
                sql = generate_sql(&app_data, &current_tab.schema.as_ref().expect("checked"), SqlType::SelectStar);
            } else if new_sql == "select_fields" {
                sql = generate_sql(&app_data, &current_tab.schema.as_ref().expect("checked"), SqlType::SelectFields);
            }
        }
    }

    let output_format = if form.contains_key("jsonl") {
        OutputFormat::JSON
    } else if form.contains_key("tab") {
        OutputFormat::TSV
    } else if form.contains_key("csv") {
        OutputFormat::CSV
    } else {
        OutputFormat::WEB
    };

    if output_format != OutputFormat::WEB {
        match output_stream(app_data, sql, output_format).await {
            Ok(res) => return Ok(Either::Right(res)),
            Err(e) => return Err(e),
        }
    }

    let tmpl = app_data
        .env
        .get_template("layout.html")
        .expect("template exists");


    let mutexed_connection = if app_data.connection.is_some() {
        app_data.connection.clone().unwrap()
    } else if app_data.db.is_some() {
        Arc::new(Mutex::new(fresh_db_connection(&app_data)))
    } else {
        return Err(ErrorBadRequest("No database connection"));
    };

    let conn = mutexed_connection.lock().await;

    let other_sql_list: Vec<(String, String)> = other_sql
        .iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

    if current_tab.tab_type == "history" {

        let history_json = form.get("history").ok_or(ErrorBadRequest("history not found"))?;
        let history = serde_json::from_str::<History>(history_json).map_err(|e| ErrorBadRequest(format!("Bad JSON: {e}")))?;

        let mut sql_history = vec![];
        for hash in history.history {
            if let Some(sql) = history.hash.get(&hash) {
                sql_history.push(sql.clone());
            }
        }

        let res = tmpl
            .render(&context! {
                current_tab => current_tab,
                other_sql => other_sql_list,
                tabs => app_data.tabs,
                display_limit => display_limit.to_string(),
                sections => app_data.sections,
                history => sql_history,
            })
            .map_err(|e| ErrorInternalServerError(e))?;
        return Ok(Either::Left(HttpResponse::Ok().body(res)));
    }

    let table_data = if sql.is_empty() {
        Ok(TableData { headers: vec![], rows: vec![] })
    } else {
        run_query(&sql.as_str(), &conn, display_limit)
    };

    if table_data.is_err() {
        let res = tmpl
            .render(&context! {
                current_tab => current_tab,
                other_sql => other_sql_list,
                tabs => app_data.tabs.clone(),
                table_data => TableData { headers: vec![], rows: vec![] },
                display_limit => display_limit.to_string(),
                sql => sql,
                sql_error => table_data.unwrap_err().to_string(),
                sections => app_data.sections
            })
            .map_err(|e| ErrorInternalServerError(e))?;

        return Ok(Either::Left(HttpResponse::Ok().body(res)));
    }

    let res = tmpl
        .render(&context! {
            current_tab => current_tab,
            other_sql => other_sql_list,
            tabs => app_data.tabs.clone(),
            table_data => table_data.unwrap(),
            display_limit => display_limit.to_string(),
            sql => sql,
            sections => app_data.sections
        })
        .map_err(|e| ErrorInternalServerError(e))?;

    Ok(Either::Left(HttpResponse::Ok().body(res)))
}

#[derive(PartialEq, Copy, Clone)]
enum OutputFormat {
    CSV,
    TSV,
    JSON,
    WEB,
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
                OutputFormat::WEB => {}
            }
        }
    };

    let content_disposition = match output {
        OutputFormat::CSV => "attachment; filename=download.csv",
        OutputFormat::TSV => "attachment; filename=download.tsv",
        OutputFormat::JSON => "attachment; filename=download.json",
        OutputFormat::WEB => "",
    };

    let content_type = match output {
        OutputFormat::CSV => "text/csv",
        OutputFormat::TSV => "text/tab-separated-values",
        OutputFormat::JSON => "application/json",
        OutputFormat::WEB => "text/html",
    };

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Disposition", content_disposition))
        .insert_header(("Content-Type", content_type))
        .streaming(Box::pin(output_stream)))
}
