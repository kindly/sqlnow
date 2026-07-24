use eyre::Result;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use clap::{Parser, Subcommand, ValueEnum};
use libsqlnow::{
    default_name_and_check, get_app_data, input_into_parts, main_web, query_database,
    sidecar_path, sniff_db_type, validate_name, Config, DbType, Input, Session, TableData,
};
use actix_web::{App, HttpServer, web::Data};

/// The agent guide ships inside the binary so it is discoverable from the
/// CLI alone, with no repo checkout.
const AGENTS_MD: &str = include_str!("../../AGENTS.md");

const AFTER_HELP: &str = "For LLM agents: run `sqlnow --agents-help` for the full agent guide \
(launch recipes, HTTP API, session file format, querying from the command line).\n\
Also at: https://github.com/kindly/querier/blob/main/AGENTS.md";

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

    /// Pre-defined query, repeatable: "name=SELECT ..." or bare SQL
    /// (the name is auto-generated when omitted)
    #[arg(short = 'q', long = "query")]
    query: Vec<String>,

    /// Pre-defined query read from a file, repeatable: "name=path.sql" or
    /// "path.sql" (the file stem becomes the name)
    #[arg(long = "query-file")]
    query_file: Vec<String>,

    /// Open the browser on startup. With a name, also start on that query:
    /// --open "top customers"
    #[arg(long, num_args = 0..=1)]
    open: Option<Option<String>>,

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

/// "name=SELECT ..." when the part before the first '=' is a valid query
/// name, otherwise the whole spec is SQL.
fn parse_query_spec(spec: &str) -> (Option<String>, String) {
    if let Some((name, sql)) = spec.split_once('=') {
        if validate_name(name).is_ok() {
            return (Some(name.to_string()), sql.to_string());
        }
    }
    (None, spec.to_string())
}

/// "name=path.sql" or "path.sql" (name defaults to the file stem).
fn parse_query_file_spec(spec: &str) -> Result<(String, String)> {
    let (name, path) = match spec.split_once('=') {
        Some((name, path)) if validate_name(name).is_ok() => (Some(name.to_string()), path.to_string()),
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
    let cli = Cli::parse();

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

    if let Some(cli_views) = &cli.view {
        for file in cli_views.iter() {
            let mut input = input_into_parts(file);
            default_name_and_check(&mut input)?;
            views.push(input);
        }
    }

    if let Some(cli_tables) = &cli.table {
        for file in cli_tables.iter() {
            let mut input = input_into_parts(file);
            default_name_and_check(&mut input)?;
            tables.push(input);
        }
    }

    let mut sidecar_files = vec![];
    let mut db = cli.db.clone();

    if let Some(cli_files) = &cli.files {
        for (i, file) in cli_files.iter().enumerate() {
            // the first positional argument may be the main duckdb database
            if i == 0 && db.is_none() && main_duckdb_candidate(file) {
                db = Some(file.clone());
                continue;
            }
            if file.ends_with(".sqlnow") {
                sidecar_files.push(file.clone());
                continue;
            }
            let mut input = input_into_parts(file);
            default_name_and_check(&mut input)?;
            if file.ends_with(".xlsx") || file.ends_with(".json") || file.ends_with(".jsonl") {
                tables.push(input);
            } else {
                views.push(input);
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
    // queries already in the session)
    for spec in cli.query.iter() {
        match parse_query_spec(spec) {
            (Some(name), sql) => session.upsert_query(&name, &sql)?,
            (None, sql) => {
                session.create_query(None, &sql)?;
            }
        }
    }
    for spec in cli.query_file.iter() {
        let (name, sql) = parse_query_file_spec(spec)?;
        session.upsert_query(&name, &sql)?;
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

    let host = match env::var("HOST") {
        Ok(val) => val,
        Err(_) => "127.0.0.1".into(),
    };

    let port: u16 = match env::var("PORT") {
        Ok(val) => {
            match val.parse::<u16>() {
                Ok(val) => val,
                Err(_) => 8080
            }
        }
        Err(_) => 8080
    };

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
    println!("Server running on {}", base_url);

    let deep_url = open_query
        .as_ref()
        .map(|name| format!("{}/queries/{}", base_url, percent_encode(name)));
    if let (Some(name), Some(url)) = (&open_query, &deep_url) {
        println!("Open query \"{}\": {}", name, url);
    }

    if cli.open.is_some() {
        let target = deep_url.clone().unwrap_or_else(|| base_url.clone());
        if let Err(e) = open::that(&target) {
            eprintln!("Could not open the browser: {}", e);
        }
    }

    HttpServer::new(move || {
      App::new()
          .configure(main_web)
          .app_data(Data::new(app_data.clone()))
      })
      .bind((host.clone(), port.clone()))?
      .workers(workers)
      .run()
      .await?;

    Ok(())
}
