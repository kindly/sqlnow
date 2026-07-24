use eyre::Result;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use libsqlnow::{
    default_name_and_check, get_app_data, input_into_parts, main_web, parse_table_filter,
    query_database, sidecar_path, sniff_db_type, validate_name, Config, DbType, Input, Session,
    TableData,
};
use actix_web::{App, HttpServer, web::Data};

/// The agent guide ships inside the binary so it is discoverable from the
/// CLI alone, with no repo checkout.
const AGENTS_MD: &str = include_str!("../../AGENTS.md");

const AFTER_HELP: &str = "For LLM agents: run `sqlnow --agents-help` for the full agent guide \
(launch recipes, HTTP API, session file format, querying from the command line).\n\
Also at: https://github.com/kindly/sqlnow/blob/main/AGENTS.md";

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None, after_help = AFTER_HELP)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Print the guide for LLM agents (AGENTS.md) and exit
    #[arg(long)]
    agents_help: bool,

    #[arg(short, long)]
    table: Option<Vec<String>>,

    #[arg(short, long)]
    view: Option<Vec<String>>,

    #[arg(short = 'x', long)]
    text: bool,

    #[arg(long)]
    drop: bool,

    /// DuckDB database to open as the main database. A DuckDB file given as
    /// the first positional argument is used the same way.
    #[arg(short, long)]
    db: Option<String>,

    /// Save all inputs to <name>.sqlnow, replay them with: sqlnow <name>.sqlnow
    #[arg(short, long)]
    save: Option<String>,

    /// Pre-defined query, repeatable: bare SQL (auto-named) or "name=SELECT ..."
    #[arg(short = 'q', long = "query")]
    query: Vec<String>,

    /// Pre-defined query read from a file, repeatable: "path.sql" (the file
    /// stem becomes the name) or "name=path.sql"
    #[arg(long = "query-file")]
    query_file: Vec<String>,

    /// Name the immediately preceding input or query. The value before
    /// --as is taken completely literally (no name=/# splitting), so any
    /// URI, path, or SQL works: -v 'postgresql://h/db?sslmode=disable' --as gem
    #[arg(long = "as", value_name = "NAME")]
    input_name: Vec<String>,

    /// Table filter for the immediately preceding database input,
    /// repeatable: --tables t1,t2
    #[arg(long = "tables", value_name = "TABLES")]
    table_filter: Vec<String>,

    /// Open the browser on startup. With a name, also start on that query:
    /// --open "top customers"
    #[arg(long, num_args = 0..=1)]
    open: Option<Option<String>>,

    /// Port to serve on (also: PORT env var) [default: 8080]
    #[arg(short, long)]
    port: Option<u16>,

    /// Address to bind (also: HOST env var) [default: 127.0.0.1]
    #[arg(long)]
    host: Option<String>,

    files: Option<Vec<String>>,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
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
    /// Run SQL against a session (.sqlnow) file. The file is created with
    /// the session schema if it does not exist, so agents can seed queries
    /// without any duckdb installation:
    ///   sqlnow exec session.sqlnow "INSERT INTO queries(pos, name, sql) VALUES (1, 'peek', 'SELECT 1')"
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
enum SqlFormat {
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
    /// From --tables.
    tables: Vec<String>,
}

/// Reconstruct the command line in order and attach each --as / --tables to
/// the input or query immediately before it. clap records argument indices,
/// so the association is exact, not guessed.
fn planned_entries(matches: &clap::ArgMatches) -> Result<Vec<PlannedEntry>> {
    enum Token {
        Entry(EntryKind, String),
        As(String),
        Tables(String),
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
    tokens.sort_by_key(|(index, _)| *index);

    let mut entries: Vec<PlannedEntry> = vec![];
    for (_, token) in tokens {
        match token {
            Token::Entry(kind, value) => entries.push(PlannedEntry {
                kind,
                value,
                name: None,
                tables: vec![],
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
            Token::Tables(list) => {
                let entry = entries.last_mut().ok_or_else(|| {
                    eyre::eyre!("--tables must come after the database input it filters")
                })?;
                if matches!(entry.kind, EntryKind::Query | EntryKind::QueryFile) {
                    return Err(eyre::eyre!("--tables cannot apply to a query"));
                }
                entry.tables.extend(parse_table_filter(&list));
            }
        }
    }
    Ok(entries)
}

fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{:02X}", b),
        })
        .collect()
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
            "-v", "other.sqlite", "--tables", "t1,t2",
        ])
        .unwrap();
        assert_eq!(planned.len(), 3);
        assert_eq!(planned[0].name.as_deref(), Some("gem"));
        assert_eq!(planned[0].value, "postgresql://h/db?sslmode=disable");
        assert_eq!(planned[1].name.as_deref(), Some("top units"));
        assert_eq!(planned[1].value, "SELECT a=1");
        assert_eq!(planned[2].name, None);
        assert_eq!(planned[2].tables, vec!["t1", "t2"]);
    }

    #[test]
    fn as_without_target_is_an_error() {
        assert!(entries(&["sqlnow", "--as", "gem"]).is_err());
        assert!(entries(&["sqlnow", "-v", "a.csv", "--as", "x", "--as", "y"]).is_err());
        assert!(entries(&["sqlnow", "-q", "SELECT 1", "--tables", "t"]).is_err());
    }
}

fn run_exec(session_path: &str, sql: &str, format: SqlFormat) -> Result<()> {
    let session = Session::open(std::path::Path::new(session_path))?;
    let table_data = session.raw_sql(sql)?;
    print_table(&table_data, format)
}

fn run_sql(db_path: &str, sql: &str, format: SqlFormat, limit: Option<usize>) -> Result<()> {
    let table_data = query_database(db_path, sql, limit.unwrap_or(usize::MAX))?;
    print_table(&table_data, format)
}

#[actix_web::main]
async fn main() -> Result<()> {
    let matches = <Cli as CommandFactory>::command().get_matches();
    let cli = <Cli as FromArgMatches>::from_arg_matches(&matches)
        .map_err(|e| eyre::eyre!("{}", e))?;

    if cli.agents_help {
        print!("{}", AGENTS_MD);
        return Ok(());
    }

    match &cli.command {
        Some(Command::Exec { session, sql, format }) => {
            return run_exec(session, sql, *format);
        }
        Some(Command::Sql { database, sql, format, limit }) => {
            return run_sql(database, sql, *format, *limit);
        }
        None => {}
    }

    let mut views = vec![];
    let mut tables = vec![];
    let mut sidecar_files: Vec<String> = vec![];
    let mut db = cli.db.clone();
    // (name, value, from_file) — applied to the session once it is anchored
    let mut planned_queries: Vec<(Option<String>, String, bool)> = vec![];

    let mut first_file_seen = false;
    for entry in planned_entries(&matches)? {
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
                    },
                    None => {
                        let mut input = input_into_parts(&entry.value)?;
                        input.tables.extend(entry.tables.clone());
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
                                "session file {} cannot take --as or --tables",
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

    // --- session anchoring ---
    // The session (queries, history) lives in exactly one sidecar database:
    // the main db's auto sidecar, else the --save file, else the first
    // .sqlnow given on the command line, else in memory only.
    let anchor_path: Option<PathBuf> = if let Some(db) = &db {
        Some(sidecar_path(db))
    } else if let Some(save) = &cli.save {
        let name = if save.ends_with(".sqlnow") {
            save.clone()
        } else {
            format!("{}.sqlnow", save)
        };
        Some(PathBuf::from(name))
    } else {
        sidecar_files.first().map(PathBuf::from)
    };

    let session = match &anchor_path {
        Some(path) => Session::open(path)?,
        None => Session::in_memory()?,
    };

    // inputs recorded in the anchor session from previous runs
    let mut stored_entries: Vec<(String, Input)> = session.list_inputs()?;

    // any further .sqlnow files contribute their inputs and queries, which
    // are merged into (and persisted in) the anchor
    for file in sidecar_files.iter() {
        let path = PathBuf::from(file);
        if Some(&path) == anchor_path.as_ref() {
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

    // browser state (query history, prefs) is scoped to persisted sessions
    let scope = anchor_path.as_ref().map(|_| session.id().to_string());

    let config = Config {
        database: db.clone(),
        views: views.clone(),
        drop: cli.drop,
        all_text: cli.text,
        tables: tables.clone(),
        scope,
    };

    let session = Arc::new(Mutex::new(session));

    let app_data = {
        let session = session.clone();
        tokio::task::spawn_blocking(move || get_app_data(config, session)).await??
    };

    // record inputs only once everything attached successfully. With a main
    // db, file views/tables are persisted inside the duckdb file itself, so
    // only database attaches need recording; otherwise everything does.
    if anchor_path.is_some() {
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

    // --save alongside a main db: the db sidecar anchors the session, but a
    // replayable inputs file is still written
    if db.is_some() {
        if let Some(save) = &cli.save {
            let name = if save.ends_with(".sqlnow") {
                save.clone()
            } else {
                format!("{}.sqlnow", save)
            };
            let save_session = Session::open(std::path::Path::new(&name))?;
            let entries: Vec<(String, Input)> = views
                .iter()
                .map(|i| ("view".to_string(), i.clone()))
                .chain(tables.iter().map(|i| ("table".to_string(), i.clone())))
                .collect();
            save_session.set_inputs(&entries)?;
            println!("Saved inputs to {}, replay with: sqlnow {}", name, name);
        }
    } else if cli.save.is_some() {
        let path = anchor_path.as_ref().expect("save anchors the session");
        println!(
            "Saved session to {}, replay with: sqlnow {}",
            path.display(),
            path.display()
        );
    }

    if anchor_path.is_none() {
        println!("note: session not persisted — use --save <name> to keep queries and history");
    }

    // precedence: flag, then env var, then default
    let host = cli.host.clone()
        .or_else(|| env::var("HOST").ok())
        .unwrap_or_else(|| "127.0.0.1".into());

    let port: u16 = cli.port
        .or_else(|| env::var("PORT").ok().and_then(|val| val.parse().ok()))
        .unwrap_or(8080);

    let workers: usize = match env::var("WORKERS") {
        Ok(val) => {
            match val.parse::<usize>() {
                Ok(val) => val,
                Err(_) => 1
            }
        }
        Err(_) => 1
    };

    let base_url = format!("http://{}:{}", host, port);

    // bind before announcing anything: a failed bind (port already in use)
    // must not print "Server running" or open a browser tab at a dead URL
    let server = HttpServer::new(move || {
      App::new()
          .configure(main_web)
          .app_data(Data::new(app_data.clone()))
      })
      .bind((host.clone(), port.clone()))
      .map_err(|e| eyre::eyre!("Could not bind {}: {}", base_url, e))?
      .workers(workers)
      .run();

    println!("Server running on {}", base_url);

    let deep_url = open_query
        .as_ref()
        .map(|name| format!("{}/queries/{}", base_url, percent_encode(name)));
    if let (Some(name), Some(url)) = (&open_query, &deep_url) {
        println!("Open query \"{}\": {}", name, url);
    }

    if cli.open.is_some() {
        let target = deep_url.clone().unwrap_or_else(|| base_url.clone());
        if let Err(e) = open::that_detached(&target) {
            eprintln!("Could not open the browser: {}", e);
        }
    }

    server.await?;

    Ok(())
}
