//! Everything the `sqlnow` binary does apart from being a terminal program.
//!
//! The CLI and the desktop shell share this: argument parsing, input/session
//! wiring ([`prepare`]) and binding the HTTP server ([`serve`]). Each shell
//! only decides how the resulting URL is presented — a printed line and an
//! optional browser tab for the CLI, a native window for the desktop app.

use eyre::Result;
use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use libsqlnow::{
    default_name_and_check, exec_sql, get_app_data, input_into_parts, list_sessions, main_web,
    register_session,
    query_database,
    sidecar_path, sniff_db_type, validate_name, AppData, Config, DbType, Input, Session, StoredSession,
    TableData,
};
use actix_web::{App, HttpServer, dev::Server, web::Data};

/// The agent guide ships inside the binary so it is discoverable from the
/// CLI alone, with no repo checkout.
const AGENTS_MD: &str = include_str!("../../AGENTS.md");

const AFTER_HELP: &str = "For LLM agents: run `sqlnow --agents-help` for the full agent guide \
(launch recipes, HTTP API, session file format, querying from the command line).\n\
Also at: https://github.com/kindly/sqlnow/blob/main/AGENTS.md";

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None, after_help = AFTER_HELP)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Print the guide for LLM agents (AGENTS.md) and exit
    #[arg(long)]
    pub agents_help: bool,

    #[arg(short, long)]
    pub table: Option<Vec<String>>,

    #[arg(short, long)]
    pub view: Option<Vec<String>>,

    #[arg(short = 'x', long)]
    pub text: bool,

    #[arg(long)]
    pub drop: bool,

    /// DuckDB database to open as the main database. A DuckDB file given as
    /// the first positional argument is used the same way.
    #[arg(short, long)]
    pub db: Option<String>,

    /// Pre-defined query, repeatable: bare SQL (auto-named) or "name=SELECT ..."
    #[arg(short = 'q', long = "query")]
    pub query: Vec<String>,

    /// Pre-defined query read from a file, repeatable: "path.sql" (the file
    /// stem becomes the name) or "name=path.sql"
    #[arg(long = "query-file")]
    pub query_file: Vec<String>,

    /// Name the immediately preceding input or query. The value before
    /// --as is taken completely literally (no name=/# splitting), so any
    /// URI, path, or SQL works: -v 'postgresql://h/db?sslmode=disable' --as gem
    #[arg(long = "as", value_name = "NAME")]
    pub input_name: Vec<String>,

    /// Only expose matching tables from the immediately preceding database
    /// input; a fully-anchored regex, so plain names match exactly.
    /// Repeatable: --only orders --only 'entity_.*'
    #[arg(long = "only", value_name = "TABLE")]
    pub table_filter: Vec<String>,

    /// Never expose matching tables from the immediately preceding database
    /// input; a fully-anchored regex, applied after --only.
    /// Repeatable: --except audit_log --except '.*_secret'
    #[arg(long = "except", value_name = "TABLE")]
    pub table_exclude: Vec<String>,

    /// Open the browser on startup. With a name, also start on that query:
    /// --open "top customers"
    #[arg(long, num_args = 0..=1)]
    pub open: Option<Option<String>>,

    /// Resume a stored session. On its own, lists the recent ones and exits;
    /// with a position from that list (1 is the most recent) or an id, opens
    /// that session and replays its inputs: --resume 2
    #[arg(long, num_args = 0..=1, value_name = "N|ID")]
    pub resume: Option<Option<String>>,

    /// Keep this run out of the session list. Only applies when the session
    /// lives in a file of its own (a database sidecar or a named .sqlnow):
    /// the session works as usual, but --resume will not be able to find it.
    #[arg(long)]
    pub no_register: bool,

    /// Port to serve on (also: PORT env var) [default: 8080]
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Address to bind (also: HOST env var) [default: 127.0.0.1]
    #[arg(long)]
    pub host: Option<String>,

    pub files: Option<Vec<String>>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Run SQL against a DuckDB database file (the main database sqlnow
    /// created or was pointed at) — like the duckdb CLI, but with sidecar
    /// attaches replayed so cross-database queries keep working:
    ///   sqlnow sql data.duckdb "SELECT count(*) FROM sales"
    Sql {
        /// Path to the DuckDB database file
        database: String,
        /// SQL to run (multiple statements allowed; only a single statement returns rows)
        sql: String,
        /// Output format
        #[arg(short, long, value_enum, default_value_t = SqlFormat::Box)]
        format: SqlFormat,
        /// Maximum rows returned (default: all)
        #[arg(short, long)]
        limit: Option<usize>,
    },
    /// Run SQL against a session (.sqlnow) database — a file you name or the
    /// session store. It is created with the schema if it does not exist, so
    /// agents can seed queries without any duckdb installation:
    ///   sqlnow exec session.sqlnow "INSERT INTO queries(session, pos, name, sql) SELECT id, 1, 'peek', 'SELECT 1' FROM sessions"
    Exec {
        /// Path to the session file
        session: String,
        /// SQL to run (multiple statements allowed; only a single statement returns rows)
        sql: String,
        /// Output format
        #[arg(short, long, value_enum, default_value_t = SqlFormat::Csv)]
        format: SqlFormat,
    },
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum SqlFormat {
    /// duckdb-style table
    Box,
    Csv,
    /// array of objects
    Json,
    /// one object per line
    Jsonl,
}

// foo.xlsx
// postgresql://user:password@localhost:5432/dbname
// sqlite://path/to/db.sqlite
// moo.parquet
// *
// moo.csv

/// A plain path (no name=, #tables, or scheme) whose file is a DuckDB
/// database. Given as the first positional argument, it becomes the main
/// database like --db. A missing file only qualifies with an unambiguous
/// DuckDB extension, matching --db's create-if-absent behaviour; .db files
/// are shared with sqlite so an existing file's header decides.
fn main_duckdb_candidate(file: &str) -> bool {
    if file.contains('=') || file.contains('#') || file.contains("://") {
        return false;
    }
    let exists = std::path::Path::new(file).exists();
    if file.ends_with(".duckdb") || file.ends_with(".ddb") {
        return !exists || sniff_db_type(file) == Some(DbType::DuckDb);
    }
    if file.ends_with(".db") {
        return exists && sniff_db_type(file) == Some(DbType::DuckDb);
    }
    false
}

/// Anything starting with one of these is treated as bare SQL by -q, never
/// split at '=' (so `-q "SELECT * FROM t WHERE a=1"` cannot be mangled).
const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "WITH", "FROM", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "ATTACH",
    "DETACH", "PRAGMA", "DESCRIBE", "DESC", "SHOW", "SUMMARIZE", "EXPLAIN", "COPY", "CALL", "SET",
    "RESET", "VALUES", "TABLE", "PIVOT", "UNPIVOT", "INSTALL", "LOAD", "BEGIN", "COMMIT",
    "VACUUM", "ANALYZE", "CHECKPOINT", "EXPORT", "IMPORT", "TRUNCATE", "USE",
];

fn starts_with_sql_keyword(spec: &str) -> bool {
    let first_word = spec
        .trim_start()
        .trim_start_matches('(')
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_uppercase();
    SQL_KEYWORDS.contains(&first_word.as_str())
}

/// "name=SELECT ..." when the spec does not itself start with a SQL keyword
/// and the part before the first '=' is a valid query name; otherwise the
/// whole spec is SQL. (A query whose *name* starts with a SQL keyword can't
/// be expressed this way — use --as, which takes the SQL literally.)
fn parse_query_spec(spec: &str) -> (Option<String>, String) {
    if starts_with_sql_keyword(spec) {
        return (None, spec.to_string());
    }
    if let Some((name, sql)) = spec.split_once('=') {
        if validate_name(name).is_ok() {
            return (Some(name.to_string()), sql.to_string());
        }
    }
    (None, spec.to_string())
}

/// "name=path.sql" or "path.sql" (name defaults to the file stem). A spec
/// that names an existing file is always just a path.
fn parse_query_file_spec(spec: &str) -> Result<(String, String)> {
    let (name, path) = match spec.split_once('=') {
        Some((name, path)) if !std::path::Path::new(spec).exists() && validate_name(name).is_ok() => {
            (Some(name.to_string()), path.to_string())
        }
        _ => (None, spec.to_string()),
    };
    let sql = std::fs::read_to_string(&path)
        .map_err(|e| eyre::eyre!("Cannot read query file {}: {}", path, e))?;
    let name = match name {
        Some(name) => name,
        None => PathBuf::from(&path)
            .file_stem()
            .ok_or_else(|| eyre::eyre!("Cannot derive a query name from {}", path))?
            .to_string_lossy()
            .to_string(),
    };
    Ok((name, sql))
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum EntryKind {
    View,
    Table,
    File,
    Query,
    QueryFile,
}

#[derive(Debug, Clone)]
struct PlannedEntry {
    kind: EntryKind,
    value: String,
    /// From --as: when set, `value` is taken completely literally.
    name: Option<String>,
    /// From --only.
    tables: Vec<String>,
    /// From --except.
    except: Vec<String>,
}

/// Reconstruct the command line in order and attach each --as / --only to
/// the input or query immediately before it. clap records argument indices,
/// so the association is exact, not guessed.
fn planned_entries(matches: &clap::ArgMatches) -> Result<Vec<PlannedEntry>> {
    enum Token {
        Entry(EntryKind, String),
        As(String),
        Tables(String),
        Except(String),
    }

    let mut tokens: Vec<(usize, Token)> = vec![];
    let mut collect = |id: &str, make: &dyn Fn(String) -> Token| {
        if let Some(values) = matches.get_many::<String>(id) {
            let indices = matches.indices_of(id).expect("indices exist when values do");
            for (index, value) in indices.zip(values) {
                tokens.push((index, make(value.clone())));
            }
        }
    };
    collect("view", &|v| Token::Entry(EntryKind::View, v));
    collect("table", &|v| Token::Entry(EntryKind::Table, v));
    collect("files", &|v| Token::Entry(EntryKind::File, v));
    collect("query", &|v| Token::Entry(EntryKind::Query, v));
    collect("query_file", &|v| Token::Entry(EntryKind::QueryFile, v));
    collect("input_name", &|v| Token::As(v));
    collect("table_filter", &|v| Token::Tables(v));
    collect("table_exclude", &|v| Token::Except(v));
    tokens.sort_by_key(|(index, _)| *index);

    let mut entries: Vec<PlannedEntry> = vec![];
    for (_, token) in tokens {
        match token {
            Token::Entry(kind, value) => entries.push(PlannedEntry {
                kind,
                value,
                name: None,
                tables: vec![],
                except: vec![],
            }),
            Token::As(name) => {
                let entry = entries.last_mut().ok_or_else(|| {
                    eyre::eyre!("--as \"{}\" must come after the input or query it names", name)
                })?;
                if entry.name.is_some() {
                    return Err(eyre::eyre!(
                        "\"{}\" already has a name; --as \"{}\" has nothing to apply to",
                        entry.value, name
                    ));
                }
                validate_name(&name).map_err(|e| eyre::eyre!("--as \"{}\": {}", name, e))?;
                entry.name = Some(name);
            }
            Token::Tables(table) => {
                let entry = entries.last_mut().ok_or_else(|| {
                    eyre::eyre!("--only must come after the database input it filters")
                })?;
                if matches!(entry.kind, EntryKind::Query | EntryKind::QueryFile) {
                    return Err(eyre::eyre!("--only cannot apply to a query"));
                }
                // one table name per flag, taken literally
                entry.tables.push(table);
            }
            Token::Except(table) => {
                let entry = entries.last_mut().ok_or_else(|| {
                    eyre::eyre!("--except must come after the database input it filters")
                })?;
                if matches!(entry.kind, EntryKind::Query | EntryKind::QueryFile) {
                    return Err(eyre::eyre!("--except cannot apply to a query"));
                }
                entry.except.push(table);
            }
        }
    }
    Ok(entries)
}

pub fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{:02X}", b),
        })
        .collect()
}

/// Deep link for the query the session opens on, if there is one.
pub fn query_url(base_url: &str, open_query: Option<&str>) -> Option<String> {
    open_query.map(|name| format!("{}/queries/{}", base_url, percent_encode(name)))
}

fn json_value(row: &[String], headers: &[String]) -> String {
    let mut out = String::from("{");
    for (i, header) in headers.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::to_string(header).expect("string serializes"));
        out.push(':');
        out.push_str(&serde_json::to_string(&row[i]).expect("string serializes"));
    }
    out.push('}');
    out
}

fn print_box(table: &TableData) {
    const MAX_CELL: usize = 80;
    let clean = |value: &str| {
        let value = value.replace('\n', "\\n");
        if value.chars().count() > MAX_CELL {
            let truncated: String = value.chars().take(MAX_CELL - 1).collect();
            format!("{}…", truncated)
        } else {
            value
        }
    };

    let mut widths: Vec<usize> = table.headers.iter().map(|h| clean(h).chars().count()).collect();
    let rows: Vec<Vec<String>> = table
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, cell)| {
                    let cell = clean(cell);
                    widths[i] = widths[i].max(cell.chars().count());
                    cell
                })
                .collect()
        })
        .collect();

    let line = |left: &str, mid: &str, right: &str| {
        let segments: Vec<String> = widths.iter().map(|w| "─".repeat(w + 2)).collect();
        println!("{}{}{}", left, segments.join(mid), right);
    };
    let row_line = |cells: &[String]| {
        let padded: Vec<String> = cells
            .iter()
            .enumerate()
            .map(|(i, cell)| format!(" {}{} ", cell, " ".repeat(widths[i] - cell.chars().count())))
            .collect();
        println!("│{}│", padded.join("│"));
    };

    line("┌", "┬", "┐");
    row_line(&table.headers.iter().map(|h| clean(h)).collect::<Vec<_>>());
    line("├", "┼", "┤");
    for row in &rows {
        row_line(row);
    }
    line("└", "┴", "┘");
    let count = table.rows.len();
    println!("({} {})", count, if count == 1 { "row" } else { "rows" });
}

fn print_table(table: &TableData, format: SqlFormat) -> Result<()> {
    if table.headers.is_empty() {
        return Ok(());
    }
    match format {
        SqlFormat::Box => print_box(table),
        SqlFormat::Csv => {
            let mut writer = csv::Writer::from_writer(std::io::stdout());
            writer.write_record(&table.headers)?;
            for row in &table.rows {
                writer.write_record(row)?;
            }
            writer.flush()?;
        }
        SqlFormat::Json => {
            let body: Vec<String> = table.rows.iter().map(|row| json_value(row, &table.headers)).collect();
            println!("[{}]", body.join(","));
        }
        SqlFormat::Jsonl => {
            for row in &table.rows {
                println!("{}", json_value(row, &table.headers));
            }
        }
    }
    Ok(())
}

fn run_exec(session_path: &str, sql: &str, format: SqlFormat) -> Result<()> {
    // deliberately not scoped to one session: `exec` has to be able to inspect
    // and repair the store, which holds many
    let table_data = exec_sql(std::path::Path::new(session_path), sql)?;
    print_table(&table_data, format)
}

fn run_sql(db_path: &str, sql: &str, format: SqlFormat, limit: Option<usize>) -> Result<()> {
    let table_data = query_database(db_path, sql, limit.unwrap_or(usize::MAX))?;
    print_table(&table_data, format)
}

/// Parse the process arguments. The `ArgMatches` come back alongside `Cli`
/// because argument *order* is what binds `--as` / `--only` / `--except` to
/// the input they modify.
pub fn parse_args() -> Result<(Cli, clap::ArgMatches)> {
    let matches = <Cli as CommandFactory>::command().get_matches();
    let cli = <Cli as FromArgMatches>::from_arg_matches(&matches)
        .map_err(|e| eyre::eyre!("{}", e))?;
    Ok((cli, matches))
}

/// The modes that print to stdout and exit instead of serving: --agents-help
/// and the `sql` / `exec` subcommands. Returns true when one of them ran, in
/// which case there is nothing to serve.
pub fn run_immediate(cli: &Cli) -> Result<bool> {
    if cli.agents_help {
        print!("{}", AGENTS_MD);
        return Ok(true);
    }
    if let Some(None) = &cli.resume {
        print_recent_sessions()?;
        return Ok(true);
    }
    match &cli.command {
        Some(Command::Exec { session, sql, format }) => {
            run_exec(session, sql, *format)?;
            Ok(true)
        }
        Some(Command::Sql { database, sql, format, limit }) => {
            run_sql(database, sql, *format, *limit)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// The session file this run is anchored to, which outlives it: the sidecar
/// next to the main database, else the first .sqlnow named on the command
/// line. `None` means the run is not anchored to a file, and its session
/// lives in the store under the user's config directory instead.
fn kept_anchor(db: Option<&String>, sidecar_files: &[String]) -> Option<PathBuf> {
    if let Some(db) = db {
        return Some(sidecar_path(db));
    }
    sidecar_files.first().map(PathBuf::from)
}

/// How many sessions the `--resume` listing shows. Nothing is ever deleted;
/// older ones stay reachable by id.
const LISTING_LIMIT: usize = 20;

/// `<config dir>/sqlnow/sessions.sqlnow`: one database holding every session
/// that is not anchored to a file of its own. `None` when there is no such
/// directory to use, or it cannot be created — a run then falls back to an
/// in-memory session rather than failing.
fn store_path() -> Option<PathBuf> {
    let base = if cfg!(windows) {
        env::var_os("APPDATA").map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        env::var_os("HOME")
            .map(|home| PathBuf::from(home).join("Library").join("Application Support"))
    } else {
        env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            // the spec says a relative XDG_CONFIG_HOME is to be ignored
            .filter(|path| path.is_absolute())
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
    };
    let dir = base?.join("sqlnow");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir.join("sessions.sqlnow"))
}

/// Stable 64-bit FNV-1a. This keys stored sessions, so identical inputs have
/// to hash identically in every build: the standard library's hashers are
/// either seeded per process or unspecified across versions, which would
/// orphan every stored session on an upgrade.
fn digest(parts: &[String]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        // the trailing NUL keeps ["ab", "c"] distinct from ["a", "bc"]
        for byte in part.as_bytes().iter().chain(std::iter::once(&0u8)) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("{:016x}", hash)
}

/// Name for a run's own session: a digest of the inputs it was given, so the
/// same command comes back to the same session. Argument order does not
/// change it, and existing paths are canonicalised so `./data.parquet` and
/// its absolute form share one session.
fn session_key(views: &[Input], tables: &[Input]) -> String {
    let mut parts: Vec<String> = views
        .iter()
        .map(|input| ("view", input))
        .chain(tables.iter().map(|input| ("table", input)))
        .map(|(kind, input)| {
            let uri = std::fs::canonicalize(&input.uri)
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_else(|_| input.uri.clone());
            format!(
                "{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}",
                kind,
                input.name,
                uri,
                input.tables.join(","),
                input.except.join(",")
            )
        })
        .collect();
    parts.sort();
    digest(&parts)
}

/// The stored sessions, most recently used first — one connection and one
/// query however many there are. Reading them touches nothing, so a listing
/// does not disturb the order.
fn recent_sessions() -> Vec<StoredSession> {
    match store_path() {
        Some(store) => list_sessions(&store).unwrap_or_default(),
        None => vec![],
    }
}

/// What a session is, for the listing: the file it lives in when it has one
/// of its own, otherwise the inputs it was created for.
fn session_label(session: &StoredSession) -> String {
    if let Some(path) = &session.path {
        let label = shorten_home(path);
        // a stat is cheap enough to do per row, and a session whose file has
        // gone is worth saying so rather than dropping from the list
        return match std::path::Path::new(path).exists() {
            true => label,
            false => format!("{} (missing)", label),
        };
    }
    if session.inputs.is_empty() {
        return "(no inputs recorded)".to_string();
    }
    session.inputs.iter().map(|uri| shorten_home(uri)).collect::<Vec<_>>().join(", ")
}

fn shorten_home(path: &str) -> String {
    match env::var_os("HOME") {
        Some(home) if !home.is_empty() => {
            let home = home.to_string_lossy().into_owned();
            match path.strip_prefix(&home) {
                Some(rest) => format!("~{}", rest),
                None => path.to_string(),
            }
        }
        _ => path.to_string(),
    }
}

/// Coarse "how long ago", which avoids needing calendar arithmetic for a
/// listing where the exact timestamp does not matter.
fn age(seconds: i64) -> String {
    match seconds.max(0) {
        0..=59 => "just now".to_string(),
        60..=3_599 => format!("{}m ago", seconds / 60),
        3_600..=86_399 => format!("{}h ago", seconds / 3_600),
        86_400..=2_591_999 => format!("{}d ago", seconds / 86_400),
        _ => format!("{}w ago", seconds / 604_800),
    }
}

/// `--resume` with no value: show what there is to resume.
fn print_recent_sessions() -> Result<()> {
    let sessions = recent_sessions();
    if sessions.is_empty() {
        println!(
            "No stored sessions yet — a run without a database file keeps one automatically."
        );
        return Ok(());
    }
    // nothing is ever deleted, so a long-lived store can hold thousands: show
    // the recent ones and leave the rest reachable by id
    let total = sessions.len();
    let shown = &sessions[..total.min(LISTING_LIMIT)];

    let positions: Vec<String> = (1..=shown.len()).map(|n| n.to_string()).collect();
    let ids: Vec<String> =
        shown.iter().map(|session| session.id.chars().take(8).collect()).collect();
    let ages: Vec<String> = shown.iter().map(|session| age(session.age_seconds)).collect();
    // a registered session's queries live in its own file, not here, so the
    // store cannot count them — "-" rather than a misleading 0
    let counts: Vec<String> = shown
        .iter()
        .map(|session| match session.path {
            Some(_) => "-".to_string(),
            None => session.queries.to_string(),
        })
        .collect();

    let width = |header: &str, values: &[String]| {
        values
            .iter()
            .map(|value| value.chars().count())
            .chain(std::iter::once(header.chars().count()))
            .max()
            .unwrap_or(0)
    };
    // the session column runs to the end of the line: it is what identifies a
    // session, so it is the one that must not be truncated
    let (pos_w, id_w, age_w, count_w) = (
        width("#", &positions),
        width("id", &ids),
        width("used", &ages),
        width("queries", &counts),
    );

    println!(
        "{:>pos_w$}  {:<id_w$}  {:<age_w$}  {:>count_w$}  {}",
        "#", "id", "used", "queries", "session"
    );
    for (index, session) in shown.iter().enumerate() {
        println!(
            "{:>pos_w$}  {:<id_w$}  {:<age_w$}  {:>count_w$}  {}",
            positions[index],
            ids[index],
            ages[index],
            counts[index],
            session_label(session)
        );
    }
    if total > shown.len() {
        println!(
            "\n{} of {} shown — older ones are still there, resume them by id",
            shown.len(),
            total
        );
    }
    println!(
        "\nResume one with `sqlnow --resume 1`, or by id: `sqlnow --resume {}`",
        ids[0]
    );
    Ok(())
}

/// Where a `--resume` value pointed. A session recorded with a file of its own
/// is opened exactly as naming that file would be; everything else lives in
/// the store and is opened by id.
enum ResumeTarget {
    /// Held in the store itself, by id.
    Stored(String),
    /// A session file of its own, opened as such.
    File(PathBuf),
    /// A main database. Its sidecar holds the session, but opening the sidecar
    /// alone is not enough: with a main database only the attaches are
    /// recorded there, because file views and tables live in the database.
    Database(PathBuf),
}

/// The database a sidecar belongs to, when there is one.
///
/// [`sidecar_path`] appends `.sqlnow` to the database's path, so stripping it
/// again gives the database back. Used to reopen a session recorded before the
/// store started remembering the database, and to make sense of a sidecar named
/// on the command line.
fn database_beside(sidecar: &Path) -> Option<PathBuf> {
    let text = sidecar.to_string_lossy();
    let anchor = PathBuf::from(text.strip_suffix(".sqlnow")?);
    let is_duckdb = anchor.exists()
        && sniff_db_type(&anchor.to_string_lossy()) == Some(DbType::DuckDb);
    is_duckdb.then_some(anchor)
}

/// Resolve `--resume <value>`: a position in the listing (1 is most recent)
/// or an id, which may be shortened as long as it stays unambiguous.
fn resolve_resume(value: &str) -> Result<ResumeTarget> {
    let sessions = recent_sessions();
    if sessions.is_empty() {
        return Err(eyre::eyre!(
            "there are no stored sessions to resume (`sqlnow --resume` lists them)"
        ));
    }
    if value == "0" {
        return Err(eyre::eyre!("--resume counts from 1, not 0"));
    }
    let target = |session: &StoredSession| match &session.path {
        Some(path) => {
            let path = PathBuf::from(path);
            let path = database_beside(&path).unwrap_or(path);
            // opening a missing path would silently create an empty session
            // there, which is not what resuming one means
            if !path.exists() {
                let store = store_path().unwrap_or_else(|| PathBuf::from("<store>"));
                return Err(eyre::eyre!(
                    "session {} lives in {}, which no longer exists — restore it, or drop the \
                     entry:\n  sqlnow exec {} \"DELETE FROM sessions WHERE id = '{}'\"",
                    session.id.chars().take(8).collect::<String>(),
                    path.display(),
                    store.display(),
                    session.id
                ));
            }
            match path.extension().and_then(|ext| ext.to_str()) {
                Some("sqlnow") => Ok(ResumeTarget::File(path)),
                _ => Ok(ResumeTarget::Database(path)),
            }
        }
        None => Ok(ResumeTarget::Stored(session.id.clone())),
    };
    // ids are 16 hex digits, so a short all-digit value is read as a position
    // first — but an id can begin with digits, so a position that does not
    // exist falls through to matching ids rather than failing outright
    if value.len() < 16 {
        if let Ok(position) = value.parse::<usize>() {
            if let Some(session) = position.checked_sub(1).and_then(|index| sessions.get(index)) {
                return target(session);
            }
        }
    }
    let matched: Vec<&StoredSession> =
        sessions.iter().filter(|session| session.id.starts_with(value)).collect();
    match matched.as_slice() {
        [session] => target(session),
        [] => Err(eyre::eyre!(
            "--resume {}: no session is at that position ({} stored) and none has an id \
             starting with it (`sqlnow --resume` lists them)",
            value,
            sessions.len()
        )),
        several => Err(eyre::eyre!(
            "--resume {}: {} sessions have ids starting with that, use more of the id",
            value,
            several.len()
        )),
    }
}

/// A session that is attached and ready to be served.
/// Records that a session has been used, at the moment a run stops using it.
///
/// Sessions are listed by when they were last used, and from the outside that
/// means when you closed the window — not when you opened it. Without this, a
/// session you worked in all afternoon sorts below one you glanced at first
/// thing in the morning.
pub struct Closer {
    session: Arc<Mutex<Session>>,
    /// For a session in a file of its own: the store to refresh, and what its
    /// row there points at. A session held in the store needs neither, because
    /// the row being touched is already the one the listing reads.
    registry: Option<(PathBuf, PathBuf)>,
}

impl Closer {
    /// Best effort: a session that cannot be touched on the way out is only
    /// mis-sorted in a listing, which is no reason to fail a run that worked.
    pub fn mark_used(&self) {
        let session = match self.session.lock() {
            Ok(session) => session,
            Err(_) => return,
        };
        if let Err(e) = session.touch_used() {
            eprintln!("note: could not record the session as used ({})", e);
            return;
        }
        if let Some((store, reopen)) = &self.registry {
            let _ = register_session(store, session.id(), reopen);
        }
    }
}

pub struct Prepared {
    /// DuckDB connection state and tabs, ready to hand to [`serve`].
    pub app_data: AppData,
    /// The query the UI should open on, if the session has one.
    pub open_query: Option<String>,
    /// From --host / HOST, defaulting to loopback.
    pub host: String,
    /// From --port / PORT. `None` when neither was given, so each shell picks
    /// its own default: 8080 for the CLI, an ephemeral port for the desktop
    /// app, where a fixed port would collide between windows.
    pub port: Option<u16>,
    /// Call [`Closer::mark_used`] when the run ends.
    pub closer: Closer,
}

/// Turn parsed arguments into an attached, queryable session: resolve inputs,
/// anchor and merge the session sidecar, seed pre-defined queries, and open
/// DuckDB. Everything the CLI did before starting the HTTP server.
pub async fn prepare(cli: &Cli, matches: &clap::ArgMatches) -> Result<Prepared> {
    let mut views = vec![];
    let mut tables = vec![];
    let mut sidecar_files: Vec<String> = vec![];
    let mut db = cli.db.clone();
    // (name, value, from_file) — applied to the session once it is anchored
    let mut planned_queries: Vec<(Option<String>, String, bool)> = vec![];

    let mut first_file_seen = false;
    for entry in planned_entries(matches)? {
        match entry.kind {
            EntryKind::Query => planned_queries.push((entry.name, entry.value, false)),
            EntryKind::QueryFile => planned_queries.push((entry.name, entry.value, true)),
            EntryKind::View | EntryKind::Table | EntryKind::File => {
                // --as means the value is literal; otherwise the name=uri#tables
                // shorthand applies (guarded: it never splits existing paths,
                // URIs, or anything whose left side could not be a name)
                let mut input = match &entry.name {
                    Some(name) => Input {
                        name: name.clone(),
                        uri: entry.value.clone(),
                        tables: entry.tables.clone(),
                        except: entry.except.clone(),
                    },
                    None => {
                        let mut input = input_into_parts(&entry.value)?;
                        input.tables.extend(entry.tables.clone());
                        input.except.extend(entry.except.clone());
                        input
                    }
                };

                if entry.kind == EntryKind::File {
                    let is_first = !first_file_seen;
                    first_file_seen = true;
                    // the first plain positional may be the main duckdb database
                    if is_first
                        && db.is_none()
                        && input.name.is_empty()
                        && input.tables.is_empty()
                        && main_duckdb_candidate(&input.uri)
                    {
                        db = Some(input.uri);
                        continue;
                    }
                    if input.uri.ends_with(".sqlnow") {
                        if !input.name.is_empty() || !input.tables.is_empty() {
                            return Err(eyre::eyre!(
                                "session file {} cannot take --as or --only",
                                input.uri
                            ));
                        }
                        sidecar_files.push(input.uri);
                        continue;
                    }
                }

                default_name_and_check(&mut input)?;
                let loads_as_table = entry.kind == EntryKind::Table
                    || (entry.kind == EntryKind::File
                        && (input.uri.ends_with(".xlsx")
                            || input.uri.ends_with(".json")
                            || input.uri.ends_with(".jsonl")));
                if loads_as_table {
                    tables.push(input);
                } else {
                    views.push(input);
                }
            }
        }
    }

    // --resume <n|id> anchors the run on a stored session, exactly as naming
    // its .sqlnow file would: the session's inputs replay and everything is
    // written back to it.
    let mut resume_id: Option<String> = None;
    if let Some(Some(value)) = &cli.resume {
        match resolve_resume(value)? {
            // a session with a file of its own opens exactly as naming that
            // file would; the closing note reports a stored one by id
            ResumeTarget::File(path) => {
                println!("Resuming session {}", path.display());
                sidecar_files.insert(0, path.to_string_lossy().into_owned());
            }
            ResumeTarget::Database(path) => {
                println!("Resuming session on {}", path.display());
                db = Some(path.to_string_lossy().into_owned());
            }
            ResumeTarget::Stored(id) => resume_id = Some(id),
        }
    }

    // --- session anchoring ---
    let kept = kept_anchor(db.as_ref(), &sidecar_files);

    // A run the user did not anchor keeps its session in the store, keyed by
    // the set of inputs it was given: run the same command again and the same
    // session comes back, queries and history included. Nothing is deleted.
    let mut resumed_queries: Option<usize> = None;
    let session = match (&kept, &resume_id) {
        (Some(path), _) => Session::open(path)?,
        (None, Some(id)) => {
            let store = store_path()
                .ok_or_else(|| eyre::eyre!("no config directory holding the session store"))?;
            let session = Session::open_in_store_by_id(&store, id)?;
            resumed_queries = Some(session.list_queries()?.len());
            session
        }
        (None, None) => match store_path() {
            Some(store) => {
                let (session, created) =
                    Session::open_in_store(&store, &session_key(&views, &tables))?;
                if !created {
                    resumed_queries = Some(session.list_queries()?.len());
                }
                session
            }
            None => {
                eprintln!(
                    "note: no writable config directory for the session store, \
                     continuing in memory"
                );
                Session::in_memory()?
            }
        },
    };

    // A session in a file of its own is recorded in the store, so `--resume`
    // lists it alongside the stored ones. What is recorded is what has to be
    // opened to get the session back — the database when there is one, since
    // its sidecar records only the attaches — rather than where the session's
    // own rows happen to live. The pointer is advisory: the session itself
    // works whether or not the store can be written.
    let reopen = db.clone().map(PathBuf::from).or_else(|| kept.clone());
    let mut registry = None;
    if let (false, Some(path), Some(store)) = (cli.no_register, reopen, store_path()) {
        if let Err(e) = register_session(&store, session.id(), &path) {
            eprintln!("note: could not add {} to the session list ({})", path.display(), e);
        }
        registry = Some((store, path));
    }


    // A resumed session is only usable if everything it recorded is still
    // reachable: quietly dropping an input would leave the session looking
    // complete when a table it remembers is missing. This covers both the
    // session found by its inputs and one named with --resume.
    let strict_stored_inputs = resumed_queries.is_some() || matches!(&cli.resume, Some(Some(_)));

    // inputs recorded in the anchor session from previous runs
    let mut stored_entries: Vec<(String, Input)> = session.list_inputs()?;

    // any further .sqlnow files contribute their inputs and queries, which
    // are merged into (and persisted in) the anchor
    for file in sidecar_files.iter() {
        let path = PathBuf::from(file);
        if Some(&path) == kept.as_ref() {
            continue;
        }
        let other = Session::open(&path)?;
        stored_entries.extend(other.list_inputs()?);
        for query in other.list_queries()? {
            if session.get_query(&query.name).is_err() {
                session.upsert_query(&query.name, &query.sql)?;
            }
        }
    }

    // inputs given on the command line win over stored entries
    for (kind, mut input) in stored_entries {
        if views.iter().chain(tables.iter()).any(|i| i.name == input.name) {
            continue;
        }
        if let Err(e) = default_name_and_check(&mut input) {
            if strict_stored_inputs {
                let path = session.path().expect("a resumed session is file-backed");
                return Err(eyre::eyre!(
                    "{}\nThis session records the {} \"{}\" ({}), which cannot be used now. \
                     Put it back, or drop it from the session:\n  \
                     sqlnow exec {} \"DELETE FROM inputs WHERE session = '{}' AND name = '{}'\"",
                    e,
                    kind,
                    input.name,
                    input.uri,
                    path.display(),
                    session.id(),
                    input.name
                ));
            }
            eprintln!("Skipping stored input {}: {}", input.name, e);
            continue;
        }
        if kind == "table" {
            tables.push(input);
        } else {
            views.push(input);
        }
    }

    // pre-defined queries from the command line (they overwrite same-named
    // queries already in the session; overwritten SQL is kept in history)
    for (name, value, from_file) in planned_queries {
        if from_file {
            let (name, sql) = match name {
                Some(name) => {
                    let sql = std::fs::read_to_string(&value)
                        .map_err(|e| eyre::eyre!("Cannot read query file {}: {}", value, e))?;
                    (name, sql)
                }
                None => parse_query_file_spec(&value)?,
            };
            session.upsert_query(&name, &sql)?;
        } else {
            match (name, value) {
                (Some(name), sql) => session.upsert_query(&name, &sql)?,
                (None, spec) => match parse_query_spec(&spec) {
                    (Some(name), sql) => session.upsert_query(&name, &sql)?,
                    (None, sql) => {
                        session.create_query(None, &sql)?;
                    }
                },
            }
        }
    }

    // --open <name> overrides the session's stored open query
    if let Some(Some(name)) = &cli.open {
        if session.get_query(name).is_ok() {
            session.set_open(Some(name))?;
        } else {
            eprintln!("warning: --open query \"{}\" does not exist, ignoring", name);
        }
    }
    let open_query = match session.open_query()? {
        Some(name) if session.get_query(&name).is_ok() => Some(name),
        Some(name) => {
            eprintln!("warning: open query \"{}\" does not exist, ignoring", name);
            None
        }
        None => None,
    };

    // Browser state (column widths, prefs) is scoped to the session id, which
    // is stable across runs, so those settings come back with the session
    // rather than resetting on every launch.
    let persistent = session.is_persistent();
    let scope = persistent.then(|| session.id().to_string());
    // kept for the closing note, which runs after the session is shared out
    let session_id_for_note = session.id().to_string();

    let config = Config {
        database: db.clone(),
        views: views.clone(),
        drop: cli.drop,
        all_text: cli.text,
        tables: tables.clone(),
        scope,
    };

    let session = Arc::new(Mutex::new(session));
    let closer = Closer { session: session.clone(), registry };

    let app_data = {
        let session = session.clone();
        tokio::task::spawn_blocking(move || get_app_data(config, session)).await??
    };

    // Record inputs only once everything attached successfully. With a main
    // db, file views/tables are persisted inside the duckdb file itself, so
    // only database attaches need recording; otherwise everything does.
    //
    // Done even for a session that will not outlive the run, because the
    // server reads these back to know which inputs are databases — that is
    // where their --only/--except filters and display names come from.
    {
        let entries: Vec<(String, Input)> = if db.is_some() {
            views
                .iter()
                .filter(|i| i.is_database())
                .map(|i| ("view".to_string(), (*i).clone()))
                .collect()
        } else {
            views
                .iter()
                .map(|i| ("view".to_string(), i.clone()))
                .chain(tables.iter().map(|i| ("table".to_string(), i.clone())))
                .collect()
        };
        session
            .lock()
            .expect("session lock")
            .set_inputs(&entries)?;
    }

    // a run that landed in the store: name the session it is in, since the
    // same inputs will come back to it and `--resume` can find it
    if kept.is_none() {
        match (persistent, resumed_queries) {
            (false, _) => println!(
                "note: session not persisted — no writable config directory for the \
                 session store, so queries and history last only for this run"
            ),
            (true, Some(count)) => println!(
                "note: resumed session {} ({} saved {})",
                session_id_for_note,
                count,
                if count == 1 { "query" } else { "queries" }
            ),
            (true, None) => println!(
                "note: session {} — the same inputs resume it next time, \
                 or find it with `sqlnow --resume`",
                session_id_for_note
            ),
        }
    }

    // precedence: flag, then env var, then default
    let host = cli.host.clone()
        .or_else(|| env::var("HOST").ok())
        .unwrap_or_else(|| "127.0.0.1".into());

    let port = cli.port
        .or_else(|| env::var("PORT").ok().and_then(|val| val.parse().ok()));

    Ok(Prepared { app_data, open_query, host, port, closer })
}

/// Bind the HTTP server and return it alongside the address it actually got.
/// Binding happens before the server is awaited so callers can fail loudly on
/// a taken port instead of announcing a URL that does not answer, and so a
/// `port` of 0 can be resolved to the real port for the URL.
pub fn serve(app_data: AppData, host: &str, port: u16) -> Result<(Server, SocketAddr)> {
    let workers: usize = env::var("WORKERS")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(1);

    let server = HttpServer::new(move || {
        App::new()
            .configure(main_web)
            .app_data(Data::new(app_data.clone()))
    })
    .bind((host, port))
    .map_err(|e| eyre::eyre!("Could not bind http://{}:{}: {}", host, port, e))?
    .workers(workers);

    let addr = *server
        .addrs()
        .first()
        .ok_or_else(|| eyre::eyre!("Server bound no address"))?;

    Ok((server.run(), addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(argv: &[&str]) -> Result<Vec<PlannedEntry>> {
        let matches = <Cli as CommandFactory>::command().get_matches_from(argv);
        planned_entries(&matches)
    }

    #[test]
    fn sql_containing_equals_is_never_split() {
        let (name, sql) = parse_query_spec("SELECT * FROM t WHERE a=1");
        assert_eq!(name, None);
        assert_eq!(sql, "SELECT * FROM t WHERE a=1");

        let (name, sql) = parse_query_spec("update t set a=1");
        assert_eq!(name, None);
        assert_eq!(sql, "update t set a=1");

        // FROM-first and parenthesised forms too
        assert_eq!(parse_query_spec("from t select a=1").0, None);
        assert_eq!(parse_query_spec("(select 1=1)").0, None);
    }

    #[test]
    fn named_query_sugar_still_works() {
        let (name, sql) = parse_query_spec("top units=SELECT * FROM t");
        assert_eq!(name.as_deref(), Some("top units"));
        assert_eq!(sql, "SELECT * FROM t");
    }

    #[test]
    fn uris_with_equals_are_never_split() {
        let input = input_into_parts("postgresql://localhost/db?sslmode=disable").unwrap();
        assert_eq!(input.name, "");
        assert_eq!(input.uri, "postgresql://localhost/db?sslmode=disable");

        // named form still splits
        let input = input_into_parts("pg=postgresql://localhost/db?sslmode=disable").unwrap();
        assert_eq!(input.name, "pg");
        assert_eq!(input.uri, "postgresql://localhost/db?sslmode=disable");
    }

    #[test]
    fn existing_paths_are_taken_literally() {
        let dir = std::env::temp_dir().join(format!("sqlnow-cli-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let weird = dir.join("report=2024#final.csv");
        std::fs::write(&weird, "a\n1\n").unwrap();

        let spec = weird.to_string_lossy().to_string();
        let input = input_into_parts(&spec).unwrap();
        assert_eq!(input.uri, spec);
        assert_eq!(input.name, "");
        assert!(input.tables.is_empty());
    }

    #[test]
    fn table_filter_sugar_still_works() {
        let input = input_into_parts("db=some.sqlite#a,b").unwrap();
        assert_eq!(input.name, "db");
        assert_eq!(input.uri, "some.sqlite");
        assert_eq!(input.tables, vec!["a", "b"]);
    }

    #[test]
    fn as_attaches_to_the_preceding_entry() {
        let planned = entries(&[
            "sqlnow",
            "-v", "postgresql://h/db?sslmode=disable", "--as", "gem",
            "-q", "SELECT a=1", "--as", "top units",
            "-v", "other.sqlite", "--only", "t1", "--only", "weird,name",
        ])
        .unwrap();
        assert_eq!(planned.len(), 3);
        assert_eq!(planned[0].name.as_deref(), Some("gem"));
        assert_eq!(planned[0].value, "postgresql://h/db?sslmode=disable");
        assert_eq!(planned[1].name.as_deref(), Some("top units"));
        assert_eq!(planned[1].value, "SELECT a=1");
        assert_eq!(planned[2].name, None);
        // each --only value is literal — commas are not delimiters
        assert_eq!(planned[2].tables, vec!["t1", "weird,name"]);
    }

    #[test]
    fn as_without_target_is_an_error() {
        assert!(entries(&["sqlnow", "--as", "gem"]).is_err());
        assert!(entries(&["sqlnow", "-v", "a.csv", "--as", "x", "--as", "y"]).is_err());
        assert!(entries(&["sqlnow", "-q", "SELECT 1", "--only", "t"]).is_err());
        assert!(entries(&["sqlnow", "-q", "SELECT 1", "--except", "t"]).is_err());
        assert!(entries(&["sqlnow", "--except", "t"]).is_err());
    }

    #[test]
    fn a_main_db_anchors_the_session_next_to_the_database() {
        let db = "data/plants.duckdb".to_string();
        assert_eq!(
            kept_anchor(Some(&db), &[]),
            Some(PathBuf::from("data/plants.duckdb.sqlnow"))
        );
        // the db's own sidecar wins over any .sqlnow also named
        let files = ["elsewhere.sqlnow".to_string()];
        assert_eq!(
            kept_anchor(Some(&db), &files),
            Some(PathBuf::from("data/plants.duckdb.sqlnow"))
        );
    }

    #[test]
    fn a_sidecar_resolves_back_to_its_database() {
        let dir = std::env::temp_dir().join(format!("sqlnow-beside-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        // a real duckdb file and the sidecar beside it
        let db = dir.join("plants.duckdb");
        libsqlnow::exec_sql(&db, "SELECT 1").ok();
        let sidecar = dir.join("plants.duckdb.sqlnow");
        std::fs::write(&sidecar, "").unwrap();
        // resuming the sidecar has to open the database: with a main database
        // the sidecar records only attaches, so on its own it has no tables
        assert_eq!(database_beside(&sidecar), Some(db));

        // a session file that is not a sidecar stays what it is
        let standalone = dir.join("analysis.sqlnow");
        std::fs::write(&standalone, "").unwrap();
        assert_eq!(database_beside(&standalone), None);

        // and neither is anything whose "database" is missing or not duckdb
        let orphan = dir.join("gone.duckdb.sqlnow");
        std::fs::write(&orphan, "").unwrap();
        assert_eq!(database_beside(&orphan), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_session_file_anchors_when_there_is_no_main_database() {
        // the first .sqlnow on the command line anchors; later ones merge in
        let files = ["first.sqlnow".to_string(), "second.sqlnow".to_string()];
        assert_eq!(kept_anchor(None, &files), Some(PathBuf::from("first.sqlnow")));
    }

    #[test]
    fn an_unanchored_run_uses_the_session_store() {
        // no database file and no .sqlnow: the session lives in the store
        // under the config directory, named after the inputs
        assert_eq!(kept_anchor(None, &[]), None);
    }

    fn input(name: &str, uri: &str) -> Input {
        Input { name: name.to_string(), uri: uri.to_string(), tables: vec![], except: vec![] }
    }

    #[test]
    fn the_session_key_does_not_depend_on_argument_order() {
        let a = input("a", "postgresql://h/one");
        let b = input("b", "postgresql://h/two");
        assert_eq!(
            session_key(&[a.clone(), b.clone()], &[]),
            session_key(&[b, a], &[])
        );
    }

    #[test]
    fn different_inputs_get_different_sessions() {
        let one = session_key(&[input("a", "postgresql://h/one")], &[]);
        let two = session_key(&[input("a", "postgresql://h/two")], &[]);
        let renamed = session_key(&[input("b", "postgresql://h/one")], &[]);
        // the same uri loaded as a table is a different catalog, so a
        // different session
        let as_table = session_key(&[], &[input("a", "postgresql://h/one")]);
        assert_ne!(one, two);
        assert_ne!(one, renamed);
        assert_ne!(one, as_table);

        let mut filtered = input("a", "postgresql://h/one");
        filtered.tables = vec!["orders".to_string()];
        assert_ne!(one, session_key(&[filtered], &[]));
    }

    #[test]
    fn equivalent_paths_share_one_session() {
        let dir = std::env::temp_dir().join(format!("sqlnow-key-test-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("data")).unwrap();
        let direct = dir.join("data").join("plants.csv");
        std::fs::write(&direct, "name\nPlant A\n").unwrap();
        let indirect = dir.join("data").join("..").join("data").join("plants.csv");

        assert_eq!(
            session_key(&[input("plants", direct.to_str().unwrap())], &[]),
            session_key(&[input("plants", indirect.to_str().unwrap())], &[])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_digest_is_stable_across_releases() {
        // session files are named with this, so a change here orphans every
        // stored session: update the constant only on purpose
        assert_eq!(digest(&["view\u{1}a\u{1}b\u{1}\u{1}".to_string()]), "0caa3c8c11530f6d");
    }

    #[test]
    fn except_attaches_to_the_preceding_input() {
        let planned = entries(&[
            "sqlnow",
            "-v", "app.sqlite", "--only", "orders", "--except", "audit_log",
        ])
        .unwrap();
        assert_eq!(planned[0].tables, vec!["orders"]);
        assert_eq!(planned[0].except, vec!["audit_log"]);
    }
}
