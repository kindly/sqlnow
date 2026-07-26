//! Session sidecar storage.
//!
//! A `.sqlnow` sidecar is itself a small DuckDB database holding everything
//! about a session that is not user data: the scope id, the attached inputs,
//! the saved queries, and the full run history. The sidecar db is the live
//! store — the server holds no in-memory copy and opens the file only for the
//! duration of each operation, so external processes (`sqlnow exec`, a duckdb
//! CLI) can read and write it between server operations.

use crate::{run_query, sniff_db_type, DbType, Input, TableData};
use duckdb::{params, Connection};
use eyre::Result;
use std::fmt;
use std::path::{Path, PathBuf};

/// Storage format written into every session database. Format 1 held exactly
/// one session per file with no version marker; format 2 adds `format` and
/// `sessions` and gives every other row a `session` column, so one database
/// can hold many; format 3 adds `sessions.url`, where a running server
/// publishes its address. Bump only alongside a migration in [`ensure_format`].
const FORMAT_VERSION: i64 = 3;

const SESSION_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS format(version INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS sessions(
        id TEXT PRIMARY KEY,
        key TEXT,
        path TEXT,
        last_used TIMESTAMP NOT NULL DEFAULT now(),
        changed_at TIMESTAMP NOT NULL DEFAULT now(),
        url TEXT
    );
    CREATE TABLE IF NOT EXISTS meta(session TEXT NOT NULL, key TEXT NOT NULL, value TEXT, PRIMARY KEY (session, key));
    CREATE TABLE IF NOT EXISTS queries(session TEXT NOT NULL, pos INTEGER NOT NULL, name TEXT NOT NULL, sql TEXT NOT NULL, PRIMARY KEY (session, name));
    CREATE TABLE IF NOT EXISTS history(session TEXT NOT NULL, \"at\" TIMESTAMP NOT NULL DEFAULT now(), sql TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS inputs(session TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL, uri TEXT NOT NULL, tables TEXT[], except_tables TEXT[]);
";

const LOCK_RETRIES: u32 = 5;
const LOCK_RETRY_DELAY_MS: u64 = 100;

#[derive(Debug)]
pub enum SessionError {
    NotFound(String),
    Conflict(String),
    Invalid(String),
    Locked(String),
    Db(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::NotFound(m) => write!(f, "not found: {}", m),
            SessionError::Conflict(m) => write!(f, "conflict: {}", m),
            SessionError::Invalid(m) => write!(f, "invalid: {}", m),
            SessionError::Locked(m) => write!(f, "locked: {}", m),
            SessionError::Db(m) => write!(f, "{}", m),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<duckdb::Error> for SessionError {
    fn from(e: duckdb::Error) -> Self {
        SessionError::Db(e.to_string())
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StoredQuery {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HistoryEntry {
    pub at: String,
    pub sql: String,
}

enum Store {
    File(PathBuf),
    Memory(Connection),
}

pub struct Session {
    id: String,
    store: Store,
    /// Last (file mtime, this session's `changed_at`) seen by
    /// [`Session::change_stamp`], so a quiet file costs only a `stat`.
    changed_cache: std::sync::Mutex<Option<(std::time::SystemTime, Option<i64>)>>,
}

pub fn validate_name(name: &str) -> std::result::Result<(), SessionError> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed != name {
        return Err(SessionError::Invalid(
            "query name must be non-empty without leading/trailing whitespace".into(),
        ));
    }
    if name.len() > 100 {
        return Err(SessionError::Invalid("query name too long (max 100 chars)".into()));
    }
    if name.contains('/') || name.chars().any(|c| c.is_control()) {
        return Err(SessionError::Invalid(
            "query name may not contain '/' or control characters".into(),
        ));
    }
    Ok(())
}

fn is_lock_error(message: &str) -> bool {
    message.contains("lock") || message.contains("Lock")
}

pub(crate) fn open_with_retry(path: &Path) -> std::result::Result<Connection, SessionError> {
    let mut last = String::new();
    for attempt in 0..LOCK_RETRIES {
        match Connection::open(path) {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                last = e.to_string();
                if !is_lock_error(&last) {
                    return Err(SessionError::Db(last));
                }
                if attempt + 1 < LOCK_RETRIES {
                    std::thread::sleep(std::time::Duration::from_millis(LOCK_RETRY_DELAY_MS));
                }
            }
        }
    }
    Err(SessionError::Locked(format!(
        "session file {} is locked by another process (is a query running?): {}",
        path.display(),
        last
    )))
}

impl Session {
    fn at(id: String, store: Store) -> Session {
        Session { id, store, changed_cache: std::sync::Mutex::new(None) }
    }

    /// Open a session database, upgrading a legacy line-format sidecar or a
    /// format 1 file on the way in, and refusing one written by a newer
    /// sqlnow. Shared by every constructor below.
    pub(crate) fn open_database(path: &Path) -> Result<Connection> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let preexisted = path.exists();
        if preexisted && sniff_db_type(&path.to_string_lossy()) != Some(DbType::DuckDb) {
            upgrade_legacy_sidecar(path)?;
        }
        let conn = open_with_retry(path).map_err(|e| eyre::eyre!("{}", e))?;

        // never quietly add session tables to somebody's existing data
        // database: a pre-existing duckdb file only counts as a session if
        // it has the session schema or no tables at all
        if preexisted
            && !has_table(&conn, "format")?
            && !has_table(&conn, "meta")?
            && user_table_count(&conn)? > 0
        {
            return Err(eyre::eyre!(
                "{} is a database with its own tables, not a sqlnow session file — \
                 refusing to add session tables to it. To query it, use: sqlnow sql {} \"...\"",
                path.display(),
                path.display()
            ));
        }

        ensure_format(&conn, path)?;
        Ok(conn)
    }

    /// Open (or create) a database holding a single session: the sidecar next
    /// to a main database, or a `.sqlnow` named on the command line.
    pub fn open(path: &Path) -> Result<Session> {
        let conn = Self::open_database(path)?;
        let ids = session_ids(&conn)?;
        let id = match ids.as_slice() {
            [] => insert_session(&conn, &random_id(), None, None)?,
            [only] => only.clone(),
            many => {
                return Err(eyre::eyre!(
                    "{} holds {} sessions, so it is a session store rather than one session — \
                     list them with `sqlnow --resume`",
                    path.display(),
                    many.len()
                ))
            }
        };
        touch_used(&conn, &id)?;
        drop(conn);
        Ok(Session::at(id, Store::File(path.to_path_buf())))
    }

    /// Open the session in `store` recorded under `key` — the digest of a
    /// run's inputs — creating it the first time those inputs are seen. The
    /// flag is true when the session was created rather than resumed.
    pub fn open_in_store(store: &Path, key: &str) -> Result<(Session, bool)> {
        let conn = Self::open_database(store)?;
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM sessions WHERE key = ? ORDER BY last_used DESC LIMIT 1",
                params![key],
                |row| row.get(0),
            )
            .ok();
        let created = existing.is_none();
        let id = match existing {
            Some(id) => id,
            None => insert_session(&conn, &random_id(), Some(key), None)?,
        };
        touch_used(&conn, &id)?;
        drop(conn);
        Ok((Session::at(id, Store::File(store.to_path_buf())), created))
    }

    /// Open one specific session already in `store`, by id.
    pub fn open_in_store_by_id(store: &Path, id: &str) -> Result<Session> {
        let conn = Self::open_database(store)?;
        let found: Option<String> = conn
            .query_row("SELECT id FROM sessions WHERE id = ?", params![id], |row| row.get(0))
            .ok();
        let id = found.ok_or_else(|| {
            eyre::eyre!("no stored session {} (`sqlnow --resume` lists them)", id)
        })?;
        touch_used(&conn, &id)?;
        drop(conn);
        Ok(Session::at(id, Store::File(store.to_path_buf())))
    }

    /// A session that lives only in memory: same behaviour, nothing on disk.
    pub fn in_memory() -> Result<Session> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SESSION_SCHEMA)?;
        conn.execute("INSERT INTO format(version) VALUES (?)", params![FORMAT_VERSION])?;
        let id = random_id();
        conn.execute("INSERT INTO sessions(id) VALUES (?)", params![id])?;
        Ok(Session::at(id, Store::Memory(conn)))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn is_persistent(&self) -> bool {
        matches!(self.store, Store::File(_))
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.store {
            Store::File(path) => Some(path),
            Store::Memory(_) => None,
        }
    }

    /// Change indicator for file-backed sessions, driving `/api/events`.
    ///
    /// The file's mtime is a cheap gate — reads never touch it and every write
    /// does — but a store holds many sessions, so a moved mtime only means
    /// *something* changed. When it moves, this session's own `changed_at`
    /// says whether that something was this session; without that, every
    /// server would refetch whenever any unrelated session was written.
    /// `None` for in-memory sessions, which have no external writers.
    pub fn change_stamp(&self) -> Option<i64> {
        let path = match &self.store {
            Store::File(path) => path,
            Store::Memory(_) => return None,
        };
        let mtime = std::fs::metadata(path).and_then(|meta| meta.modified()).ok()?;
        let mut cache = self.changed_cache.lock().ok()?;
        if let Some((seen, stamp)) = cache.as_ref() {
            if *seen == mtime {
                return *stamp;
            }
        }
        let stamp = self.changed_at();
        *cache = Some((mtime, stamp));
        stamp
    }

    /// When this session's contents last changed, in microseconds since the
    /// epoch. Read straight from the database, unlike [`Session::change_stamp`]
    /// which only looks when the file has moved.
    pub fn changed_at(&self) -> Option<i64> {
        self.with_conn(|conn| {
            Ok(conn
                .query_row(
                    "SELECT epoch_us(changed_at)::BIGINT FROM sessions WHERE id = ?",
                    params![self.id],
                    |row| row.get::<_, i64>(0),
                )
                .ok())
        })
        .ok()
        .flatten()
    }

    fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> std::result::Result<T, SessionError>,
    ) -> std::result::Result<T, SessionError> {
        match &self.store {
            Store::File(path) => {
                let conn = open_with_retry(path)?;
                f(&conn)
            }
            Store::Memory(conn) => f(conn),
        }
    }

    pub fn list_queries(&self) -> std::result::Result<Vec<StoredQuery>, SessionError> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT name, sql FROM queries WHERE session = ? ORDER BY pos, name")?;
            let rows = stmt.query_map(params![self.id], |row| {
                Ok(StoredQuery {
                    name: row.get(0)?,
                    sql: row.get(1)?,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    pub fn get_query(&self, name: &str) -> std::result::Result<StoredQuery, SessionError> {
        self.with_conn(|conn| get_query_on(conn, &self.id, name))
    }

    /// Create a query. `name: None` picks the first unused "query N".
    pub fn create_query(
        &self,
        name: Option<&str>,
        sql: &str,
    ) -> std::result::Result<StoredQuery, SessionError> {
        self.with_conn(|conn| {
            let name = match name {
                Some(name) => {
                    validate_name(name)?;
                    if get_query_on(conn, &self.id, name).is_ok() {
                        return Err(SessionError::Conflict(format!("query \"{}\" already exists", name)));
                    }
                    name.to_string()
                }
                None => next_query_name_on(conn, &self.id)?,
            };
            conn.execute(
                "INSERT INTO queries(session, pos, name, sql)
                 SELECT ?, coalesce(max(pos), 0) + 1, ?, ? FROM queries WHERE session = ?",
                params![self.id, name, sql, self.id],
            )?;
            touch_changed(conn, &self.id)?;
            Ok(StoredQuery { name, sql: sql.to_string() })
        })
    }

    /// Insert or overwrite a query by name (used for CLI seeding — CLI wins).
    /// Overwritten SQL is preserved in history.
    pub fn upsert_query(&self, name: &str, sql: &str) -> std::result::Result<(), SessionError> {
        validate_name(name)?;
        self.with_conn(|conn| {
            if let Ok(existing) = get_query_on(conn, &self.id, name) {
                if existing.sql != sql && !existing.sql.trim().is_empty() {
                    append_history_on(conn, &self.id, &existing.sql)?;
                }
            }
            let updated = conn.execute(
                "UPDATE queries SET sql = ? WHERE session = ? AND name = ?",
                params![sql, self.id, name],
            )?;
            if updated == 0 {
                conn.execute(
                    "INSERT INTO queries(session, pos, name, sql)
                     SELECT ?, coalesce(max(pos), 0) + 1, ?, ? FROM queries WHERE session = ?",
                    params![self.id, name, sql, self.id],
                )?;
            }
            touch_changed(conn, &self.id)?;
            Ok(())
        })
    }

    /// Update sql and/or rename. Renames follow the `open` pointer.
    ///
    /// `base_sql` is the version the writer based its edit on: when it is
    /// absent or does not match the stored SQL, the write is clobbering
    /// someone else's version, so the stored SQL is preserved in history
    /// first. Incremental saves from an editor pass their base and skip
    /// this.
    pub fn update_query(
        &self,
        name: &str,
        new_sql: Option<&str>,
        new_name: Option<&str>,
        base_sql: Option<&str>,
    ) -> std::result::Result<StoredQuery, SessionError> {
        self.with_conn(|conn| {
            let mut current = get_query_on(conn, &self.id, name)?;
            if let Some(sql) = new_sql {
                let clobbering = base_sql.map(|base| base != current.sql).unwrap_or(true);
                if clobbering && sql != current.sql && !current.sql.trim().is_empty() {
                    append_history_on(conn, &self.id, &current.sql)?;
                }
                conn.execute(
                    "UPDATE queries SET sql = ? WHERE session = ? AND name = ?",
                    params![sql, self.id, name],
                )?;
                current.sql = sql.to_string();
            }
            if let Some(new_name) = new_name {
                if new_name != name {
                    validate_name(new_name)?;
                    if get_query_on(conn, &self.id, new_name).is_ok() {
                        return Err(SessionError::Conflict(format!(
                            "query \"{}\" already exists",
                            new_name
                        )));
                    }
                    conn.execute_batch("BEGIN")?;
                    conn.execute(
                        "UPDATE queries SET name = ? WHERE session = ? AND name = ?",
                        params![new_name, self.id, name],
                    )?;
                    conn.execute(
                        "UPDATE meta SET value = ? WHERE session = ? AND key = 'open' AND value = ?",
                        params![new_name, self.id, name],
                    )?;
                    conn.execute_batch("COMMIT")?;
                    current.name = new_name.to_string();
                }
            }
            touch_changed(conn, &self.id)?;
            Ok(current)
        })
    }

    /// Delete a query; its sql is preserved in history. Clears `open` if it
    /// pointed at the deleted query.
    pub fn delete_query(&self, name: &str) -> std::result::Result<(), SessionError> {
        self.with_conn(|conn| {
            let query = get_query_on(conn, &self.id, name)?;
            conn.execute_batch("BEGIN")?;
            if !query.sql.trim().is_empty() {
                append_history_on(conn, &self.id, &query.sql)?;
            }
            conn.execute(
                "DELETE FROM queries WHERE session = ? AND name = ?",
                params![self.id, name],
            )?;
            conn.execute(
                "DELETE FROM meta WHERE session = ? AND key = 'open' AND value = ?",
                params![self.id, name],
            )?;
            conn.execute_batch("COMMIT")?;
            touch_changed(conn, &self.id)?;
            Ok(())
        })
    }

    pub fn append_history(&self, sql: &str) -> std::result::Result<(), SessionError> {
        if sql.trim().is_empty() {
            return Ok(());
        }
        self.with_conn(|conn| {
            append_history_on(conn, &self.id, sql)?;
            touch_changed(conn, &self.id)
        })
    }

    /// Newest first. `limit == 0` returns everything — history is uncapped.
    pub fn list_history(&self, limit: usize) -> std::result::Result<Vec<HistoryEntry>, SessionError> {
        self.with_conn(|conn| {
            let mut sql = "SELECT strftime(\"at\", '%Y-%m-%d %H:%M:%S'), sql
                           FROM history WHERE session = ? ORDER BY \"at\" DESC, rowid DESC"
                .to_string();
            if limit > 0 {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![self.id], |row| {
                Ok(HistoryEntry {
                    at: row.get(0)?,
                    sql: row.get(1)?,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    pub fn open_query(&self) -> std::result::Result<Option<String>, SessionError> {
        self.with_conn(|conn| get_meta_on(conn, &self.id, "open"))
    }

    pub fn set_open(&self, name: Option<&str>) -> std::result::Result<(), SessionError> {
        self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM meta WHERE session = ? AND key = 'open'",
                params![self.id],
            )?;
            if let Some(name) = name {
                conn.execute(
                    "INSERT INTO meta(session, key, value) VALUES (?, 'open', ?)",
                    params![self.id, name],
                )?;
            }
            touch_changed(conn, &self.id)?;
            Ok(())
        })
    }

    pub fn list_inputs(&self) -> std::result::Result<Vec<(String, Input)>, SessionError> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT kind, name, uri, tables, except_tables FROM inputs WHERE session = ?")?;
            let rows = stmt.query_map(params![self.id], |row| {
                let kind: String = row.get(0)?;
                let name: String = row.get(1)?;
                let uri: String = row.get(2)?;
                let only: duckdb::types::Value = row.get(3)?;
                let except: duckdb::types::Value = row.get(4)?;
                Ok((
                    kind,
                    Input {
                        name,
                        uri,
                        tables: table_list_from_value(only),
                        except: table_list_from_value(except),
                    },
                ))
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    /// Replace the recorded inputs. Queries and history are never touched.
    pub fn set_inputs(&self, entries: &[(String, Input)]) -> std::result::Result<(), SessionError> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN")?;
            conn.execute("DELETE FROM inputs WHERE session = ?", params![self.id])?;
            for (kind, input) in entries {
                conn.execute(
                    &format!(
                        "INSERT INTO inputs(session, kind, name, uri, tables, except_tables) VALUES (?, ?, ?, ?, {}, {})",
                        table_list_literal(&input.tables),
                        table_list_literal(&input.except)
                    ),
                    params![self.id, kind, input.name, absolute_uri(&input.uri)],
                )?;
            }
            conn.execute_batch("COMMIT")?;
            touch_changed(conn, &self.id)?;
            Ok(())
        })
    }

    /// Record that the session is in use now.
    ///
    /// Called when a run opens a session and again when it ends: from the
    /// outside, "last used" means when you finished with it, so a session you
    /// just closed belongs at the top of the list.
    pub fn touch_used(&self) -> std::result::Result<(), SessionError> {
        self.with_conn(|conn| {
            conn.execute("UPDATE sessions SET last_used = now() WHERE id = ?", params![self.id])?;
            Ok(())
        })
    }

    /// Record one more input, so it replays on the next launch. Replaces any
    /// entry of the same name.
    pub fn add_input(
        &self,
        kind: &str,
        input: &Input,
    ) -> std::result::Result<(), SessionError> {
        self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM inputs WHERE session = ? AND name = ?",
                params![self.id, input.name],
            )?;
            conn.execute(
                &format!(
                    "INSERT INTO inputs(session, kind, name, uri, tables, except_tables) VALUES (?, ?, ?, ?, {}, {})",
                    table_list_literal(&input.tables),
                    table_list_literal(&input.except)
                ),
                params![self.id, kind, input.name, absolute_uri(&input.uri)],
            )?;
            touch_changed(conn, &self.id)?;
            Ok(())
        })
    }

    /// Forget a recorded input, so it is not replayed again.
    pub fn remove_input(&self, name: &str) -> std::result::Result<(), SessionError> {
        self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM inputs WHERE session = ? AND name = ?",
                params![self.id, name],
            )?;
            touch_changed(conn, &self.id)?;
            Ok(())
        })
    }

    /// Run arbitrary SQL against the sidecar (the `sqlnow exec` path).
    /// Single statements return rows; multi-statement batches return an
    /// empty result.
    pub fn raw_sql(&self, sql: &str) -> std::result::Result<TableData, SessionError> {
        self.with_conn(|conn| run_raw(conn, sql))
    }
}

/// Run SQL against a session database as a database, not as one session: the
/// `sqlnow exec` path. It has to work on a store holding many sessions —
/// inspecting and repairing one is exactly what `exec` is for — so it does
/// not bind to a single session the way [`Session::open`] does. A file that
/// holds no session yet gets one, so `... SELECT id FROM sessions` works on a
/// freshly created session file.
pub fn exec_sql(path: &Path, sql: &str) -> Result<TableData> {
    let conn = Session::open_database(path)?;
    if session_ids(&conn)?.is_empty() {
        insert_session(&conn, &random_id(), None, None)?;
    }
    run_raw(&conn, sql).map_err(|e| eyre::eyre!("{}", e))
}

fn run_raw(conn: &Connection, sql: &str) -> std::result::Result<TableData, SessionError> {
    match run_query(sql, conn, usize::MAX) {
        Ok(table_data) => Ok(table_data),
        Err(prepare_error) => {
            // multi-statement input (or a genuine error, which will surface
            // identically here)
            conn.execute_batch(sql)
                .map_err(|_| SessionError::Db(prepare_error.to_string()))?;
            Ok(TableData { headers: vec![], rows: vec![], truncated: false })
        }
    }
}

/// A duckdb list literal for a table filter — names are quoted, so any
/// characters (including commas) survive storage.
fn table_list_literal(tables: &[String]) -> String {
    if tables.is_empty() {
        return "NULL".to_string();
    }
    let quoted: Vec<String> = tables.iter().map(|t| quote_literal(t)).collect();
    format!("[{}]", quoted.join(", "))
}

pub(crate) fn table_list_from_value(value: duckdb::types::Value) -> Vec<String> {
    match value {
        duckdb::types::Value::List(items) => items
            .into_iter()
            .filter_map(|item| match item {
                duckdb::types::Value::Text(text) => Some(text),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

fn has_table(conn: &Connection, name: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
        params![name],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn user_table_count(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main'",
        [],
        |row| row.get(0),
    )?)
}

/// Bring a session database up to [`FORMAT_VERSION`], or refuse to touch it.
fn ensure_format(conn: &Connection, path: &Path) -> Result<()> {
    // format 1 had no version marker: session tables with no session column
    if !has_table(conn, "format")? && has_table(conn, "meta")? {
        migrate_format_1(conn)?;
        eprintln!(
            "Upgraded session file {} to format {}",
            path.display(),
            FORMAT_VERSION
        );
        return Ok(());
    }

    conn.execute_batch(SESSION_SCHEMA)?;
    let version: Option<i64> =
        conn.query_row("SELECT max(version) FROM format", [], |row| row.get(0))?;
    match version {
        None => {
            conn.execute("INSERT INTO format(version) VALUES (?)", params![FORMAT_VERSION])?;
        }
        Some(2) => {
            // format 3 only adds a column, so the rows carry over untouched
            conn.execute_batch(
                "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS url TEXT;
                 UPDATE format SET version = 3;",
            )?;
        }
        Some(found) if found > FORMAT_VERSION => {
            return Err(eyre::eyre!(
                "{} was written by a newer sqlnow (session format {}, this build understands {}) \
                 — upgrade sqlnow to open it",
                path.display(),
                found,
                FORMAT_VERSION
            ))
        }
        Some(_) => {}
    }
    Ok(())
}

/// Format 1 → 2. The tables gain a `session` column, so they are rebuilt
/// rather than altered (duckdb cannot change a primary key in place), and the
/// old `meta['id']` becomes the one row in `sessions`.
fn migrate_format_1(conn: &Connection) -> Result<()> {
    let id = get_meta_on_unscoped(conn, "id")?.unwrap_or_else(random_id);

    conn.execute_batch("BEGIN")?;
    // creates format and sessions; the format 1 tables already exist, so the
    // IF NOT EXISTS clauses leave them alone until they are rebuilt below
    conn.execute_batch(SESSION_SCHEMA)?;
    conn.execute("INSERT INTO format(version) VALUES (?)", params![FORMAT_VERSION])?;
    conn.execute("INSERT INTO sessions(id) VALUES (?)", params![id])?;

    // a format 1 file written before --except existed may lack the column
    conn.execute_batch("ALTER TABLE inputs ADD COLUMN IF NOT EXISTS except_tables TEXT[];")?;

    for (table, columns, rebuilt) in [
        ("meta", "key, value", "session TEXT NOT NULL, key TEXT NOT NULL, value TEXT, PRIMARY KEY (session, key)"),
        ("queries", "pos, name, sql", "session TEXT NOT NULL, pos INTEGER NOT NULL, name TEXT NOT NULL, sql TEXT NOT NULL, PRIMARY KEY (session, name)"),
        ("history", "\"at\", sql", "session TEXT NOT NULL, \"at\" TIMESTAMP NOT NULL DEFAULT now(), sql TEXT NOT NULL"),
        ("inputs", "kind, name, uri, tables, except_tables", "session TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL, uri TEXT NOT NULL, tables TEXT[], except_tables TEXT[]"),
    ] {
        conn.execute_batch(&format!("CREATE TABLE {}_2({});", table, rebuilt))?;
        // 'id' moves out of meta and into the sessions row
        let filter = if table == "meta" { " WHERE key <> 'id'" } else { "" };
        conn.execute(
            &format!(
                "INSERT INTO {}_2(session, {}) SELECT ?, {} FROM {}{}",
                table, columns, columns, table, filter
            ),
            params![id],
        )?;
        conn.execute_batch(&format!(
            "DROP TABLE {}; ALTER TABLE {}_2 RENAME TO {};",
            table, table, table
        ))?;
    }
    conn.execute_batch("COMMIT")?;
    Ok(())
}

/// The ids of every session in a database, oldest first.
fn session_ids(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT id FROM sessions ORDER BY last_used")?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|row| row.ok())
        .collect();
    Ok(ids)
}

fn insert_session(
    conn: &Connection,
    id: &str,
    key: Option<&str>,
    path: Option<&str>,
) -> Result<String> {
    conn.execute(
        "INSERT INTO sessions(id, key, path) VALUES (?, ?, ?)",
        params![id, key, path],
    )?;
    Ok(id.to_string())
}

/// Records that a session was opened: orders the `--resume` listing.
fn touch_used(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("UPDATE sessions SET last_used = now() WHERE id = ?", params![id])?;
    Ok(())
}

/// Records that a session's contents changed, which is what a connected UI
/// watches. Opening a session is deliberately not a change.
fn touch_changed(conn: &Connection, id: &str) -> std::result::Result<(), SessionError> {
    conn.execute("UPDATE sessions SET changed_at = now() WHERE id = ?", params![id])?;
    Ok(())
}

/// Read a format 1 `meta` row, before the session column exists.
fn get_meta_on_unscoped(conn: &Connection, key: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT value FROM meta WHERE key = ?")?;
    let mut rows = stmt.query_map(params![key], |row| row.get::<_, String>(0))?;
    Ok(match rows.next() {
        Some(Ok(value)) => Some(value),
        _ => None,
    })
}

fn get_meta_on(
    conn: &Connection,
    session: &str,
    key: &str,
) -> std::result::Result<Option<String>, SessionError> {
    let mut stmt = conn.prepare("SELECT value FROM meta WHERE session = ? AND key = ?")?;
    let mut rows = stmt.query_map(params![session, key], |row| row.get::<_, String>(0))?;
    match rows.next() {
        Some(Ok(value)) => Ok(Some(value)),
        _ => Ok(None),
    }
}

fn get_query_on(
    conn: &Connection,
    session: &str,
    name: &str,
) -> std::result::Result<StoredQuery, SessionError> {
    let mut stmt =
        conn.prepare("SELECT name, sql FROM queries WHERE session = ? AND name = ?")?;
    let mut rows = stmt.query_map(params![session, name], |row| {
        Ok(StoredQuery {
            name: row.get(0)?,
            sql: row.get(1)?,
        })
    })?;
    match rows.next() {
        Some(Ok(query)) => Ok(query),
        _ => Err(SessionError::NotFound(format!("query \"{}\" does not exist", name))),
    }
}

fn next_query_name_on(
    conn: &Connection,
    session: &str,
) -> std::result::Result<String, SessionError> {
    let mut stmt = conn.prepare("SELECT name FROM queries WHERE session = ?")?;
    let names: Vec<String> = stmt
        .query_map(params![session], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();
    let mut i = 1;
    loop {
        let candidate = format!("query {}", i);
        if !names.contains(&candidate) {
            return Ok(candidate);
        }
        i += 1;
    }
}

fn append_history_on(
    conn: &Connection,
    session: &str,
    sql: &str,
) -> std::result::Result<(), SessionError> {
    // identical sql just refreshes its place in history; nothing is capped,
    // every distinct query ever run stays retrievable
    conn.execute(
        "DELETE FROM history WHERE session = ? AND trim(sql) = trim(?)",
        params![session, sql],
    )?;
    conn.execute("INSERT INTO history(session, sql) VALUES (?, ?)", params![session, sql])?;
    Ok(())
}

/// Rewrite a legacy line-format sidecar as a session database at the same
/// path (via a temp file + rename).
fn upgrade_legacy_sidecar(path: &Path) -> Result<()> {
    let (id, entries) = parse_legacy_sidecar(path)?;
    let tmp = path.with_extension("sqlnow.upgrade");
    if tmp.exists() {
        std::fs::remove_file(&tmp)?;
    }
    {
        let conn = Connection::open(&tmp)?;
        conn.execute_batch(SESSION_SCHEMA)?;
        conn.execute("INSERT INTO format(version) VALUES (?)", params![FORMAT_VERSION])?;
        let id = id.unwrap_or_else(random_id);
        conn.execute("INSERT INTO sessions(id) VALUES (?)", params![id])?;
        for (kind, input) in &entries {
            conn.execute(
                &format!(
                    "INSERT INTO inputs(session, kind, name, uri, tables, except_tables) VALUES (?, ?, ?, ?, {}, {})",
                    table_list_literal(&input.tables),
                    table_list_literal(&input.except)
                ),
                params![id, kind, input.name, absolute_uri(&input.uri)],
            )?;
        }
    }
    std::fs::rename(&tmp, path)?;
    eprintln!("Upgraded legacy session file {} to the database format", path.display());
    Ok(())
}

/// Parse the old line-based sidecar format: `id <hex>`, `view name=uri#t1,t2`,
/// `table name=uri`.
pub fn parse_legacy_sidecar(path: &Path) -> Result<(Option<String>, Vec<(String, Input)>)> {
    let content = std::fs::read_to_string(path)?;
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut id = None;
    let mut entries = vec![];

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (kind, spec) = line
            .split_once(char::is_whitespace)
            .ok_or_else(|| eyre::eyre!("Invalid line in {}: {}", path.display(), line))?;
        if kind == "id" {
            id = Some(spec.trim().to_string());
            continue;
        }
        if kind != "view" && kind != "table" {
            return Err(eyre::eyre!("Invalid line in {}: {}", path.display(), line));
        }

        let mut input = input_into_parts(spec.trim())?;

        // hand-edited sidecars may use paths relative to the sidecar's directory
        if let Some(local) = local_db_path(&input.uri).or_else(|| {
            (!input.uri.contains("://")).then(|| input.uri.clone())
        }) {
            let local_buf = PathBuf::from(&local);
            if local_buf.is_relative() {
                let joined = dir.join(local_buf).to_string_lossy().to_string();
                input.uri = if input.uri.starts_with("sqlite://") {
                    format!("sqlite://{}", joined)
                } else {
                    joined
                };
            }
        }

        if let Err(e) = default_name_and_check(&mut input) {
            eprintln!("Skipping sidecar entry from {}: {}", path.display(), e);
            continue;
        }

        entries.push((kind.to_string(), input));
    }

    Ok((id, entries))
}

// --- input parsing helpers (shared by the CLI and sidecar code) ---

/// Quote a SQL identifier (double quotes, embedded quotes doubled).
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a SQL string literal (single quotes, embedded quotes doubled).
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Could this be an input name given on the left of `=`? Rules out anything
/// path- or URI-shaped, so `postgresql://host/db?sslmode=disable` or
/// `C:\data.csv` can never be split. (The fully general way to name an input
/// is the `--as` flag, which does no splitting at all.)
fn plausible_input_name(candidate: &str) -> bool {
    validate_name(candidate).is_ok() && !candidate.contains(':') && !candidate.contains('\\')
}

/// Split a table-filter list (csv rules, so quoted names may contain commas).
pub fn parse_table_filter(list: &str) -> Vec<String> {
    let mut tables = Vec::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(list.as_bytes());
    if let Some(Ok(record)) = reader.records().next() {
        for field in record.iter() {
            tables.push(field.to_owned());
        }
    }
    tables
}

/// Parse an input spec: `name=uri#table1,table2`, each part optional except
/// uri. `=` only splits a name off when the left side is plausibly a name,
/// and neither `=` nor `#` split when the spec (or the part after the name)
/// is an existing file — so real paths containing either character work.
pub fn input_into_parts(input: &str) -> Result<Input> {
    // a spec that names an existing file is always just a path
    if std::path::Path::new(input).exists() {
        return Ok(Input {
            name: "".to_owned(),
            uri: input.to_owned(),
            tables: vec![],
            except: vec![],
        });
    }

    let (name, not_name) = match input.split_once('=') {
        Some((start, end)) if plausible_input_name(start) => (start.to_owned(), end.to_owned()),
        _ => ("".to_owned(), input.to_owned()),
    };

    if std::path::Path::new(&not_name).exists() {
        return Ok(Input {
            name,
            uri: not_name,
            tables: vec![],
            except: vec![],
        });
    }

    let (uri, tables) = match not_name.rsplit_once('#') {
        Some((start, end)) => (start.to_owned(), parse_table_filter(end)),
        None => (not_name, vec![]),
    };

    Ok(Input { name, uri, tables, except: vec![] })
}

pub fn default_name_and_check(input: &mut Input) -> Result<()> {
    let local = input.uri.ends_with(".parquet")
        || input.uri.ends_with(".csv")
        || input.uri.ends_with(".db")
        || input.uri.ends_with(".sqlite")
        || input.uri.ends_with(".duckdb")
        || input.uri.ends_with(".ddb")
        || input.uri.starts_with("sqlite://");
    if !local {
        return Ok(());
    }

    let path = input.uri.strip_prefix("sqlite://").unwrap_or(&input.uri);
    let path_buf = PathBuf::from(path);

    if !input.uri.starts_with("s3://") && !path_buf.exists() {
        return Err(eyre::eyre!("File {} does not exist", path));
    }

    if input.name.is_empty() {
        let mut name = path_buf.file_stem().expect("is file").to_string_lossy().to_string();
        // duckdb reserves these as attached database names
        if ["main", "system", "temp"].contains(&name.as_str()) {
            name = format!("{}_db", name);
        }
        input.name = name;
    }

    Ok(())
}

pub fn local_db_path(uri: &str) -> Option<String> {
    if let Some(path) = uri.strip_prefix("sqlite://") {
        return Some(path.to_string());
    }
    if !uri.contains("://")
        && (uri.ends_with(".db") || uri.ends_with(".sqlite") || uri.ends_with(".duckdb") || uri.ends_with(".ddb"))
    {
        return Some(uri.to_string());
    }
    None
}

pub fn absolute_uri(uri: &str) -> String {
    if let Some(path) = uri.strip_prefix("sqlite://") {
        match std::fs::canonicalize(path) {
            Ok(abs) => format!("sqlite://{}", abs.display()),
            Err(_) => uri.to_string(),
        }
    } else if uri.contains("://") {
        uri.to_string()
    } else {
        match std::fs::canonicalize(uri) {
            Ok(abs) => abs.display().to_string(),
            Err(_) => uri.to_string(),
        }
    }
}

pub fn sidecar_path(anchor: &str) -> PathBuf {
    PathBuf::from(format!("{}.sqlnow", anchor))
}

/// One row of the `--resume` listing.
#[derive(Debug, Clone)]
pub struct StoredSession {
    pub id: String,
    /// Digest of the inputs the session was created for; `None` for a session
    /// registered from a file of its own.
    pub key: Option<String>,
    /// Where the session's contents live, when that is not this database.
    pub path: Option<String>,
    /// Seconds since the session was last opened. Computed inside the query
    /// so it cannot be skewed by how duckdb stores the timestamp.
    pub age_seconds: i64,
    pub queries: usize,
    pub inputs: Vec<String>,
    /// Where a server published itself when it opened this session. Present
    /// does not mean running: a process that was killed leaves it behind, so
    /// it has to be pinged before it is believed.
    pub url: Option<String>,
}

/// Record a session that lives in its own file, so `--resume` can find it
/// alongside the ones the store holds itself. The row is a pointer: the
/// queries and history stay in that file, and only where it is, when it was
/// last used and when it last changed are kept here.
///
/// Keyed on the session's own id rather than its path, so moving the file and
/// opening it again moves the pointer instead of leaving a duplicate behind.
pub fn register_session(
    store: &Path,
    id: &str,
    path: &Path,
    changed_at: Option<i64>,
) -> Result<()> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let text = canonical.to_string_lossy().into_owned();
    let conn = Session::open_database(store)?;
    // the session's own changed_at is copied up, so the row is a truthful
    // index and not just a correctly ordered one — as fresh as `last_used`,
    // which is to say as of this moment
    let updated = conn.execute(
        "UPDATE sessions
            SET path = ?, last_used = now(),
                changed_at = coalesce(make_timestamp(?), changed_at)
          WHERE id = ?",
        params![text, changed_at, id],
    )?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO sessions(id, path, changed_at)
             VALUES (?, ?, coalesce(make_timestamp(?), now()))",
            params![id, text, changed_at],
        )?;
    }
    Ok(())
}

/// What deleting a session took with it, for the report the command prints.
#[derive(Default)]
pub struct Deleted {
    pub queries: usize,
    pub history: usize,
    pub inputs: usize,
    /// Whether there was a session row here at all. False for a store entry
    /// whose contents live in a file that has since gone.
    pub found: bool,
}

/// Delete one session and everything recorded under it.
///
/// Every table is keyed by session id, so this is all of it: the saved
/// queries, the query history, the recorded inputs, the metadata and the
/// session row itself. Other sessions in the same database are untouched, and
/// nothing the session read is looked at, let alone removed.
pub fn delete_session(path: &Path, id: &str) -> Result<Deleted> {
    let conn = Session::open_database(path)?;
    let mut deleted = Deleted::default();
    // one transaction: a session half deleted would be worse than either
    // outcome, and would still be listed
    conn.execute_batch("BEGIN")?;
    deleted.queries = conn.execute("DELETE FROM queries WHERE session = ?", params![id])?;
    deleted.history = conn.execute("DELETE FROM history WHERE session = ?", params![id])?;
    deleted.inputs = conn.execute("DELETE FROM inputs WHERE session = ?", params![id])?;
    conn.execute("DELETE FROM meta WHERE session = ?", params![id])?;
    deleted.found = conn.execute("DELETE FROM sessions WHERE id = ?", params![id])? > 0;
    conn.execute_batch("COMMIT")?;
    Ok(deleted)
}

/// Publish where a running server can be reached for this session, or clear it
/// when the run ends.
///
/// The row is the only place another process can look, so this is what makes a
/// running session visible in the listing and what stops a second server from
/// attaching to it. A killed process leaves a stale address behind, which is
/// why readers ping before believing it.
pub fn set_session_url(store: &Path, id: &str, url: Option<&str>) -> Result<()> {
    let conn = Session::open_database(store)?;
    conn.execute("UPDATE sessions SET url = ? WHERE id = ?", params![url, id])?;
    Ok(())
}

/// The address recorded for a session, if any.
pub fn session_url(store: &Path, id: &str) -> Option<String> {
    let conn = Session::open_database(store).ok()?;
    conn.query_row("SELECT url FROM sessions WHERE id = ?", params![id], |row| row.get(0))
        .ok()
        .flatten()
}

/// The session recorded in `store` for a set of inputs, if it exists yet.
pub fn session_id_for_key(store: &Path, key: &str) -> Option<String> {
    let conn = Session::open_database(store).ok()?;
    conn.query_row(
        "SELECT id FROM sessions WHERE key = ? ORDER BY last_used DESC LIMIT 1",
        params![key],
        |row| row.get(0),
    )
    .ok()
}

/// Every session in a store, most recently used first.
///
/// One connection and one query, however many sessions there are: the whole
/// point of a store over a file per session. Opened read-only so a listing
/// never writes, and so it works while a server holds the store open.
pub fn list_sessions(store: &Path) -> Result<Vec<StoredSession>> {
    if !store.exists() {
        return Ok(vec![]);
    }
    let conn = match duckdb::Config::default()
        .access_mode(duckdb::AccessMode::ReadOnly)
        .and_then(|config| Connection::open_with_flags(store, config))
    {
        Ok(conn) => conn,
        // a store with a write-ahead log to replay cannot be opened read-only
        Err(_) => open_with_retry(store).map_err(|e| eyre::eyre!("{}", e))?,
    };

    let mut stmt = conn.prepare(
        "SELECT s.id, s.key, s.path,
                epoch(now()::TIMESTAMP - s.last_used)::BIGINT,
                (SELECT count(*) FROM queries q WHERE q.session = s.id),
                (SELECT list(i.uri) FROM inputs i WHERE i.session = s.id),
                s.url
         FROM sessions s
         ORDER BY s.last_used DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(StoredSession {
            id: row.get(0)?,
            key: row.get(1)?,
            path: row.get(2)?,
            age_seconds: row.get::<_, Option<i64>>(3)?.unwrap_or(0),
            queries: row.get::<_, i64>(4)? as usize,
            inputs: table_list_from_value(row.get(5)?),
            url: row.get(6)?,
        })
    })?;
    Ok(rows.filter_map(|row| row.ok()).collect())
}

pub fn random_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time after epoch")
        .as_nanos();
    hasher.write_u128(nanos);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sqlnow-session-test-{}", random_id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn create_and_reload_round_trip() {
        let path = temp_path("s.sqlnow");
        let session = Session::open(&path).unwrap();
        let id = session.id().to_string();
        session.create_query(Some("top"), "SELECT 1").unwrap();
        session.set_open(Some("top")).unwrap();
        drop(session);

        let session = Session::open(&path).unwrap();
        assert_eq!(session.id(), id);
        assert_eq!(session.open_query().unwrap(), Some("top".to_string()));
        let queries = session.list_queries().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].sql, "SELECT 1");
    }

    #[test]
    fn history_is_uncapped_and_deduped() {
        let session = Session::in_memory().unwrap();
        for i in 0..300 {
            session.append_history(&format!("SELECT {}", i)).unwrap();
        }
        assert_eq!(session.list_history(0).unwrap().len(), 300);
        assert_eq!(session.list_history(10).unwrap().len(), 10);

        // rerunning identical sql refreshes rather than duplicates
        session.append_history("SELECT 5").unwrap();
        let all = session.list_history(0).unwrap();
        assert_eq!(all.len(), 300);
        assert_eq!(all[0].sql, "SELECT 5");
    }

    #[test]
    fn rename_collision_and_open_tracking() {
        let session = Session::in_memory().unwrap();
        session.create_query(Some("a"), "SELECT 1").unwrap();
        session.create_query(Some("b"), "SELECT 2").unwrap();
        session.set_open(Some("a")).unwrap();

        let err = session.update_query("a", None, Some("b"), None).unwrap_err();
        assert!(matches!(err, SessionError::Conflict(_)));

        session.update_query("a", None, Some("c"), None).unwrap();
        assert_eq!(session.open_query().unwrap(), Some("c".to_string()));

        session.delete_query("c").unwrap();
        assert_eq!(session.open_query().unwrap(), None);
        // deleted query's sql lands in history
        assert_eq!(session.list_history(0).unwrap()[0].sql, "SELECT 1");
    }

    #[test]
    fn auto_names_fill_gaps() {
        let session = Session::in_memory().unwrap();
        let q1 = session.create_query(None, "").unwrap();
        let q2 = session.create_query(None, "").unwrap();
        assert_eq!(q1.name, "query 1");
        assert_eq!(q2.name, "query 2");
        session.delete_query("query 1").unwrap();
        assert_eq!(session.create_query(None, "").unwrap().name, "query 1");
    }

    #[test]
    fn legacy_sidecar_upgrades_in_place() {
        let path = temp_path("legacy.sqlnow");
        // the uri must exist for default_name_and_check, use a scratch parquet-free path:
        // non-local uris skip the existence check
        std::fs::write(
            &path,
            "# old format\nid deadbeef01234567\nview remote=postgresql://example/db\n",
        )
        .unwrap();

        let session = Session::open(&path).unwrap();
        assert_eq!(session.id(), "deadbeef01234567");
        let inputs = session.list_inputs().unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].1.uri, "postgresql://example/db");

        // the file itself is now a duckdb database
        assert_eq!(sniff_db_type(&path.to_string_lossy()), Some(DbType::DuckDb));
    }

    #[test]
    fn inputs_round_trip_with_tables() {
        let session = Session::in_memory().unwrap();
        session
            .set_inputs(&[(
                "view".to_string(),
                Input {
                    name: "db".to_string(),
                    uri: "postgresql://example/db".to_string(),
                    // commas and quotes in table names survive storage
                    tables: vec!["a".to_string(), "weird,name".to_string(), "it's".to_string()],
                    except: vec!["audit,log".to_string()],
                },
            )])
            .unwrap();
        let inputs = session.list_inputs().unwrap();
        assert_eq!(inputs[0].1.tables, vec!["a", "weird,name", "it's"]);
        assert_eq!(inputs[0].1.except, vec!["audit,log"]);
    }

    #[test]
    fn clobbered_sql_is_preserved_in_history() {
        let session = Session::in_memory().unwrap();
        session.create_query(Some("q"), "SELECT 1").unwrap();

        // an editor save based on the current version: no history entry
        session.update_query("q", Some("SELECT 2"), None, Some("SELECT 1")).unwrap();
        assert_eq!(session.list_history(0).unwrap().len(), 0);

        // a writer with no base (agent PUT) clobbers: old sql -> history
        session.update_query("q", Some("SELECT 3"), None, None).unwrap();
        assert_eq!(session.list_history(0).unwrap()[0].sql, "SELECT 2");

        // a writer with a stale base clobbers: stored sql -> history
        session.update_query("q", Some("SELECT 4"), None, Some("SELECT 2")).unwrap();
        assert_eq!(session.list_history(0).unwrap()[0].sql, "SELECT 3");

        // upsert over an existing query preserves the old sql too
        session.upsert_query("q", "SELECT 5").unwrap();
        assert_eq!(session.list_history(0).unwrap()[0].sql, "SELECT 4");
    }

    /// The format 1 schema, as shipped before sessions gained a session
    /// column — written by hand so the migration is tested against the real
    /// old shape rather than whatever the current code produces.
    const FORMAT_1_SCHEMA: &str = "
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE queries(pos INTEGER NOT NULL, name TEXT PRIMARY KEY, sql TEXT NOT NULL);
        CREATE TABLE history(\"at\" TIMESTAMP NOT NULL DEFAULT now(), sql TEXT NOT NULL);
        CREATE TABLE inputs(kind TEXT NOT NULL, name TEXT NOT NULL, uri TEXT NOT NULL, tables TEXT[], except_tables TEXT[]);
    ";

    #[test]
    fn a_format_1_session_migrates_with_everything_intact() {
        let path = temp_path("old.sqlnow");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(FORMAT_1_SCHEMA).unwrap();
            conn.execute_batch(
                "INSERT INTO meta VALUES ('id', 'abc123'), ('open', 'emitters');
                 INSERT INTO queries VALUES (1, 'emitters', 'SELECT * FROM plants'),
                                            (2, 'counts', 'SELECT count(*) FROM plants');
                 INSERT INTO history(sql) VALUES ('SELECT 1'), ('SELECT 2');
                 INSERT INTO inputs VALUES ('view', 'plants', '/data/plants.parquet', NULL, NULL);",
            )
            .unwrap();
        }

        let session = Session::open(&path).unwrap();
        // the id carries over, so browser state and any registry row still match
        assert_eq!(session.id(), "abc123");
        assert_eq!(session.open_query().unwrap().as_deref(), Some("emitters"));

        let queries = session.list_queries().unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].name, "emitters");
        assert_eq!(queries[0].sql, "SELECT * FROM plants");
        assert_eq!(session.list_history(0).unwrap().len(), 2);
        let inputs = session.list_inputs().unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].1.uri, "/data/plants.parquet");

        // and it is a format 2 file now, with one session row and no stray id
        let conn = Connection::open(&path).unwrap();
        let version: i64 =
            conn.query_row("SELECT max(version) FROM format", [], |row| row.get(0)).unwrap();
        assert_eq!(version, FORMAT_VERSION);
        let sessions: i64 =
            conn.query_row("SELECT count(*) FROM sessions", [], |row| row.get(0)).unwrap();
        assert_eq!(sessions, 1);
        let stray_id: i64 = conn
            .query_row("SELECT count(*) FROM meta WHERE key = 'id'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(stray_id, 0);

        // reopening an already-migrated file must not migrate again
        drop(conn);
        let again = Session::open(&path).unwrap();
        assert_eq!(again.id(), "abc123");
        assert_eq!(again.list_queries().unwrap().len(), 2);
    }

    #[test]
    fn a_format_2_session_gains_the_url_column() {
        let path = temp_path("v2.sqlnow");
        {
            // format 2: everything but sessions.url
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE format(version INTEGER NOT NULL);
                 INSERT INTO format VALUES (2);
                 CREATE TABLE sessions(id TEXT PRIMARY KEY, key TEXT, path TEXT,
                     last_used TIMESTAMP NOT NULL DEFAULT now(),
                     changed_at TIMESTAMP NOT NULL DEFAULT now());
                 INSERT INTO sessions(id) VALUES ('keepme');
                 CREATE TABLE meta(session TEXT NOT NULL, key TEXT NOT NULL, value TEXT, PRIMARY KEY (session, key));
                 CREATE TABLE queries(session TEXT NOT NULL, pos INTEGER NOT NULL, name TEXT NOT NULL, sql TEXT NOT NULL, PRIMARY KEY (session, name));
                 INSERT INTO queries VALUES ('keepme', 1, 'kept', 'SELECT 1');
                 CREATE TABLE history(session TEXT NOT NULL, \"at\" TIMESTAMP NOT NULL DEFAULT now(), sql TEXT NOT NULL);
                 CREATE TABLE inputs(session TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL, uri TEXT NOT NULL, tables TEXT[], except_tables TEXT[]);",
            )
            .unwrap();
        }

        let session = Session::open(&path).unwrap();
        assert_eq!(session.id(), "keepme", "the session carries over");
        assert_eq!(session.list_queries().unwrap().len(), 1);

        let conn = Connection::open(&path).unwrap();
        let version: i64 =
            conn.query_row("SELECT max(version) FROM format", [], |row| row.get(0)).unwrap();
        assert_eq!(version, FORMAT_VERSION);
        // the new column is there and empty, which is what "not running" means
        let url: Option<String> =
            conn.query_row("SELECT url FROM sessions", [], |row| row.get(0)).unwrap();
        assert_eq!(url, None);

        drop(conn);
        set_session_url(&path, "keepme", Some("http://127.0.0.1:9999")).unwrap();
        assert_eq!(session_url(&path, "keepme").as_deref(), Some("http://127.0.0.1:9999"));
        set_session_url(&path, "keepme", None).unwrap();
        assert_eq!(session_url(&path, "keepme"), None);
    }

    #[test]
    fn a_newer_format_is_refused_rather_than_guessed_at() {
        let path = temp_path("future.sqlnow");
        Session::open(&path).unwrap();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute("UPDATE format SET version = ?", params![FORMAT_VERSION + 1]).unwrap();
        }
        let err = match Session::open(&path) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected a newer format to be refused"),
        };
        assert!(err.contains("newer sqlnow"), "{}", err);
    }

    #[test]
    fn one_store_holds_independent_sessions() {
        let store = temp_path("sessions.sqlnow");
        let (first, created) = Session::open_in_store(&store, "key-one").unwrap();
        assert!(created);
        first.upsert_query("mine", "SELECT 1").unwrap();

        let (second, created) = Session::open_in_store(&store, "key-two").unwrap();
        assert!(created);
        second.upsert_query("theirs", "SELECT 2").unwrap();

        // neither session sees the other's queries, history or open pointer
        assert_eq!(first.list_queries().unwrap().len(), 1);
        assert_eq!(first.list_queries().unwrap()[0].name, "mine");
        assert_eq!(second.list_queries().unwrap()[0].name, "theirs");
        first.set_open(Some("mine")).unwrap();
        assert_eq!(second.open_query().unwrap(), None);

        // the same key comes back to the same session
        let (again, created) = Session::open_in_store(&store, "key-one").unwrap();
        assert!(!created);
        assert_eq!(again.id(), first.id());
        assert_eq!(again.list_queries().unwrap()[0].name, "mine");

        // and the listing sees both, most recently used first
        let listed = list_sessions(&store).unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, first.id());
        assert_eq!(listed[0].queries, 1);

        // a single-session open must refuse a store rather than pick one
        let err = match Session::open(&store) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected open to refuse a multi-session store"),
        };
        assert!(err.contains("session store"), "{}", err);
    }

    #[test]
    fn a_change_stamp_only_moves_for_its_own_session() {
        let store = temp_path("sessions.sqlnow");
        let (mine, _) = Session::open_in_store(&store, "mine").unwrap();
        let (theirs, _) = Session::open_in_store(&store, "theirs").unwrap();

        let before = mine.change_stamp();
        assert!(before.is_some(), "a file-backed session has a stamp");

        // someone else's session changing must not look like ours changing,
        // even though it moves the mtime of the file we share
        theirs.upsert_query("not mine", "SELECT 1").unwrap();
        assert_eq!(mine.change_stamp(), before, "another session's write leaked through");

        // our own change does move it
        mine.upsert_query("mine", "SELECT 2").unwrap();
        let after = mine.change_stamp();
        assert_ne!(after, before, "our own write did not register");

        // and a quiet file keeps reporting the same stamp
        assert_eq!(mine.change_stamp(), after);
    }

    #[test]
    fn a_registered_session_is_listed_by_its_path() {
        let store = temp_path("sessions.sqlnow");
        let own_file = temp_path("analysis.sqlnow");
        let session = Session::open(&own_file).unwrap();
        session.upsert_query("kept", "SELECT 1").unwrap();

        register_session(&store, session.id(), &own_file, session.changed_at()).unwrap();
        let listed = list_sessions(&store).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, session.id());
        assert_eq!(listed[0].key, None, "a registered session has no input key");
        assert!(listed[0].path.as_deref().unwrap().ends_with("analysis.sqlnow"));
        // the contents stay in that file, so the store holds none of them
        assert_eq!(listed[0].queries, 0);

        // the store's copy of when it last changed tracks the session itself,
        // rather than being frozen at whenever the row was first written
        let registry_changed_at = |store: &std::path::Path| -> i64 {
            let conn = Connection::open(store).unwrap();
            conn.query_row("SELECT epoch_us(changed_at)::BIGINT FROM sessions", [], |row| {
                row.get(0)
            })
            .unwrap()
        };
        let first = registry_changed_at(&store);
        assert_eq!(Some(first), session.changed_at());

        session.upsert_query("later", "SELECT 2").unwrap();
        let changed = session.changed_at();
        assert!(changed > Some(first), "the session itself must have moved on");
        register_session(&store, session.id(), &own_file, changed).unwrap();
        assert_eq!(Some(registry_changed_at(&store)), changed);

        // registering again after a move updates the pointer in place
        let moved = temp_path("moved.sqlnow");
        std::fs::rename(&own_file, &moved).unwrap();
        register_session(&store, session.id(), &moved, None).unwrap();
        let listed = list_sessions(&store).unwrap();
        assert_eq!(listed.len(), 1, "a moved session must not leave a duplicate");
        assert!(listed[0].path.as_deref().unwrap().ends_with("moved.sqlnow"));
        // and passing nothing leaves the stored timestamp alone
        assert_eq!(Some(registry_changed_at(&store)), changed);
    }

    #[test]
    fn open_refuses_an_existing_non_session_database() {
        let path = temp_path("data.duckdb");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE plants(name TEXT);").unwrap();
        }
        let err = match Session::open(&path) {
            Err(err) => err,
            Ok(_) => panic!("expected open to refuse a non-session database"),
        };
        assert!(err.to_string().contains("not a sqlnow session file"), "{}", err);

        // an existing but empty duckdb file is fine to initialise
        let empty = temp_path("empty.duckdb");
        {
            Connection::open(&empty).unwrap();
        }
        assert!(Session::open(&empty).is_ok());
    }

    #[test]
    fn name_validation() {
        assert!(validate_name("top emitters").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name(" padded ").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name(&"x".repeat(101)).is_err());
    }
}
