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

const SIDECAR_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS queries(pos INTEGER NOT NULL, name TEXT PRIMARY KEY, sql TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS history(\"at\" TIMESTAMP NOT NULL DEFAULT now(), sql TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS inputs(kind TEXT NOT NULL, name TEXT NOT NULL, uri TEXT NOT NULL, tables TEXT[]);
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
    /// Open (or create) a sidecar session database. Legacy line-format
    /// sidecars are transparently upgraded to the database format.
    pub fn open(path: &Path) -> Result<Session> {
        if path.exists() && sniff_db_type(&path.to_string_lossy()) != Some(DbType::DuckDb) {
            upgrade_legacy_sidecar(path)?;
        }
        let conn = open_with_retry(path).map_err(|e| eyre::eyre!("{}", e))?;
        conn.execute_batch(SIDECAR_SCHEMA)?;
        let id = ensure_id(&conn)?;
        drop(conn);
        Ok(Session {
            id,
            store: Store::File(path.to_path_buf()),
        })
    }

    /// A session that lives only in memory: same behaviour, nothing on disk.
    pub fn in_memory() -> Result<Session> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SIDECAR_SCHEMA)?;
        let id = ensure_id(&conn)?;
        Ok(Session {
            id,
            store: Store::Memory(conn),
        })
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

    /// Cheap change indicator for file-backed sessions: the sidecar's mtime.
    /// Reads do not touch it; every write does (duckdb checkpoints on the
    /// per-operation connection close). None for in-memory sessions.
    pub fn change_stamp(&self) -> Option<std::time::SystemTime> {
        match &self.store {
            Store::File(path) => std::fs::metadata(path).and_then(|m| m.modified()).ok(),
            Store::Memory(_) => None,
        }
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
            let mut stmt = conn.prepare("SELECT name, sql FROM queries ORDER BY pos, name")?;
            let rows = stmt.query_map([], |row| {
                Ok(StoredQuery {
                    name: row.get(0)?,
                    sql: row.get(1)?,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    pub fn get_query(&self, name: &str) -> std::result::Result<StoredQuery, SessionError> {
        self.with_conn(|conn| get_query_on(conn, name))
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
                    if get_query_on(conn, name).is_ok() {
                        return Err(SessionError::Conflict(format!("query \"{}\" already exists", name)));
                    }
                    name.to_string()
                }
                None => next_query_name_on(conn)?,
            };
            conn.execute(
                "INSERT INTO queries(pos, name, sql)
                 SELECT coalesce(max(pos), 0) + 1, ?, ? FROM queries",
                params![name, sql],
            )?;
            Ok(StoredQuery { name, sql: sql.to_string() })
        })
    }

    /// Insert or overwrite a query by name (used for CLI seeding — CLI wins).
    /// Overwritten SQL is preserved in history.
    pub fn upsert_query(&self, name: &str, sql: &str) -> std::result::Result<(), SessionError> {
        validate_name(name)?;
        self.with_conn(|conn| {
            if let Ok(existing) = get_query_on(conn, name) {
                if existing.sql != sql && !existing.sql.trim().is_empty() {
                    append_history_on(conn, &existing.sql)?;
                }
            }
            let updated = conn.execute("UPDATE queries SET sql = ? WHERE name = ?", params![sql, name])?;
            if updated == 0 {
                conn.execute(
                    "INSERT INTO queries(pos, name, sql)
                     SELECT coalesce(max(pos), 0) + 1, ?, ? FROM queries",
                    params![name, sql],
                )?;
            }
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
            let mut current = get_query_on(conn, name)?;
            if let Some(sql) = new_sql {
                let clobbering = base_sql.map(|base| base != current.sql).unwrap_or(true);
                if clobbering && sql != current.sql && !current.sql.trim().is_empty() {
                    append_history_on(conn, &current.sql)?;
                }
                conn.execute("UPDATE queries SET sql = ? WHERE name = ?", params![sql, name])?;
                current.sql = sql.to_string();
            }
            if let Some(new_name) = new_name {
                if new_name != name {
                    validate_name(new_name)?;
                    if get_query_on(conn, new_name).is_ok() {
                        return Err(SessionError::Conflict(format!(
                            "query \"{}\" already exists",
                            new_name
                        )));
                    }
                    conn.execute_batch("BEGIN")?;
                    conn.execute("UPDATE queries SET name = ? WHERE name = ?", params![new_name, name])?;
                    conn.execute(
                        "UPDATE meta SET value = ? WHERE key = 'open' AND value = ?",
                        params![new_name, name],
                    )?;
                    conn.execute_batch("COMMIT")?;
                    current.name = new_name.to_string();
                }
            }
            Ok(current)
        })
    }

    /// Delete a query; its sql is preserved in history. Clears `open` if it
    /// pointed at the deleted query.
    pub fn delete_query(&self, name: &str) -> std::result::Result<(), SessionError> {
        self.with_conn(|conn| {
            let query = get_query_on(conn, name)?;
            conn.execute_batch("BEGIN")?;
            if !query.sql.trim().is_empty() {
                append_history_on(conn, &query.sql)?;
            }
            conn.execute("DELETE FROM queries WHERE name = ?", params![name])?;
            conn.execute("DELETE FROM meta WHERE key = 'open' AND value = ?", params![name])?;
            conn.execute_batch("COMMIT")?;
            Ok(())
        })
    }

    pub fn append_history(&self, sql: &str) -> std::result::Result<(), SessionError> {
        if sql.trim().is_empty() {
            return Ok(());
        }
        self.with_conn(|conn| append_history_on(conn, sql))
    }

    /// Newest first. `limit == 0` returns everything — history is uncapped.
    pub fn list_history(&self, limit: usize) -> std::result::Result<Vec<HistoryEntry>, SessionError> {
        self.with_conn(|conn| {
            let mut sql = "SELECT strftime(\"at\", '%Y-%m-%d %H:%M:%S'), sql
                           FROM history ORDER BY \"at\" DESC, rowid DESC"
                .to_string();
            if limit > 0 {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], |row| {
                Ok(HistoryEntry {
                    at: row.get(0)?,
                    sql: row.get(1)?,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    pub fn open_query(&self) -> std::result::Result<Option<String>, SessionError> {
        self.with_conn(|conn| get_meta_on(conn, "open"))
    }

    pub fn set_open(&self, name: Option<&str>) -> std::result::Result<(), SessionError> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM meta WHERE key = 'open'", [])?;
            if let Some(name) = name {
                conn.execute("INSERT INTO meta(key, value) VALUES ('open', ?)", params![name])?;
            }
            Ok(())
        })
    }

    pub fn list_inputs(&self) -> std::result::Result<Vec<(String, Input)>, SessionError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT kind, name, uri, coalesce(array_to_string(tables, ','), '') FROM inputs",
            )?;
            let rows = stmt.query_map([], |row| {
                let kind: String = row.get(0)?;
                let name: String = row.get(1)?;
                let uri: String = row.get(2)?;
                let tables: String = row.get(3)?;
                Ok((
                    kind,
                    Input {
                        name,
                        uri,
                        tables: if tables.is_empty() {
                            vec![]
                        } else {
                            tables.split(',').map(|s| s.to_string()).collect()
                        },
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
            conn.execute("DELETE FROM inputs", [])?;
            for (kind, input) in entries {
                let tables = input.tables.join(",");
                conn.execute(
                    "INSERT INTO inputs(kind, name, uri, tables)
                     VALUES (?, ?, ?, CASE WHEN ? = '' THEN NULL ELSE string_split(?, ',') END)",
                    params![kind, input.name, absolute_uri(&input.uri), tables, tables],
                )?;
            }
            conn.execute_batch("COMMIT")?;
            Ok(())
        })
    }

    /// Run arbitrary SQL against the sidecar (the `sqlnow exec` path).
    /// Single statements return rows; multi-statement batches return an
    /// empty result.
    pub fn raw_sql(&self, sql: &str) -> std::result::Result<TableData, SessionError> {
        self.with_conn(|conn| {
            match run_query(sql, conn, usize::MAX) {
                Ok(table_data) => Ok(table_data),
                Err(prepare_error) => {
                    // multi-statement input (or a genuine error, which will
                    // surface identically here)
                    conn.execute_batch(sql)
                        .map_err(|_| SessionError::Db(prepare_error.to_string()))?;
                    Ok(TableData { headers: vec![], rows: vec![] })
                }
            }
        })
    }
}

fn ensure_id(conn: &Connection) -> Result<String> {
    if let Some(id) = get_meta_on(conn, "id").map_err(|e| eyre::eyre!("{}", e))? {
        return Ok(id);
    }
    let id = random_id();
    conn.execute("INSERT INTO meta(key, value) VALUES ('id', ?)", params![id])?;
    Ok(id)
}

fn get_meta_on(conn: &Connection, key: &str) -> std::result::Result<Option<String>, SessionError> {
    let mut stmt = conn.prepare("SELECT value FROM meta WHERE key = ?")?;
    let mut rows = stmt.query_map(params![key], |row| row.get::<_, String>(0))?;
    match rows.next() {
        Some(Ok(value)) => Ok(Some(value)),
        _ => Ok(None),
    }
}

fn get_query_on(conn: &Connection, name: &str) -> std::result::Result<StoredQuery, SessionError> {
    let mut stmt = conn.prepare("SELECT name, sql FROM queries WHERE name = ?")?;
    let mut rows = stmt.query_map(params![name], |row| {
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

fn next_query_name_on(conn: &Connection) -> std::result::Result<String, SessionError> {
    let mut stmt = conn.prepare("SELECT name FROM queries")?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
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

fn append_history_on(conn: &Connection, sql: &str) -> std::result::Result<(), SessionError> {
    // identical sql just refreshes its place in history; nothing is capped,
    // every distinct query ever run stays retrievable
    conn.execute("DELETE FROM history WHERE trim(sql) = trim(?)", params![sql])?;
    conn.execute("INSERT INTO history(sql) VALUES (?)", params![sql])?;
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
        conn.execute_batch(SIDECAR_SCHEMA)?;
        let id = id.unwrap_or_else(random_id);
        conn.execute("INSERT INTO meta(key, value) VALUES ('id', ?)", params![id])?;
        for (kind, input) in &entries {
            let tables = input.tables.join(",");
            conn.execute(
                "INSERT INTO inputs(kind, name, uri, tables)
                 VALUES (?, ?, ?, CASE WHEN ? = '' THEN NULL ELSE string_split(?, ',') END)",
                params![kind, input.name, absolute_uri(&input.uri), tables, tables],
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

        let mut input = input_into_parts(spec.trim());

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

/// Parse an input spec: `name=uri#table1,table2`, each part optional except uri.
pub fn input_into_parts(input: &str) -> Input {
    let mut name = "".to_owned();
    let uri: String;
    let mut hash = Vec::new();

    let not_name: String;

    match input.split_once('=') {
        Some((start, end)) => {
            name = start.to_owned();
            not_name = end.to_owned();
        }
        None => {
            not_name = input.to_owned();
        }
    }

    match not_name.rsplit_once('#') {
        Some((start, end)) => {
            uri = start.to_owned();

            if !end.is_empty() {
                let mut reader = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(end.as_bytes());

                for record in reader.records() {
                    let record = record.unwrap();
                    for field in record.iter() {
                        hash.push(field.to_owned());
                    }
                    break;
                }
            }
        }
        None => {
            uri = not_name.to_owned();
        }
    }

    Input {
        name,
        uri,
        tables: hash,
    }
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
                    tables: vec!["a".to_string(), "b".to_string()],
                },
            )])
            .unwrap();
        let inputs = session.list_inputs().unwrap();
        assert_eq!(inputs[0].1.tables, vec!["a", "b"]);
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

    #[test]
    fn name_validation() {
        assert!(validate_name("top emitters").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name(" padded ").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name(&"x".repeat(101)).is_err());
    }
}
