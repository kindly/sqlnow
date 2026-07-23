use eyre::Result;
use std::env;
use clap::Parser;
use libsqlnow::Config;
use libsqlnow::{main_web, get_app_data, Input};
use actix_web::{App, HttpServer, web::Data};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    table: Option<Vec<String>>,

    #[arg(short, long)]
    view: Option<Vec<String>>,

    #[arg(short = 'x', long)]
    text: bool,

    #[arg(long)]
    drop: bool,

    #[arg(short, long)]
    db: Option<String>,

    /// Save all inputs to <name>.sqlnow, replay them with: sqlnow <name>.sqlnow
    #[arg(short, long)]
    save: Option<String>,

    files: Option<Vec<String>>,
}

// foo.xlsx
// postgresql://user:password@localhost:5432/dbname
// sqlite://path/to/db.sqlite
// moo.parquet
// *
// moo.csv

fn input_into_parts(input: &str) -> Input {
    let mut name = "".to_owned();
    let uri: String;
    let mut hash = Vec::new();

    let not_name: String;

    match input.split_once('='){
        Some((start, end)) => {
            name = start.to_owned();
            not_name = end.to_owned();
        },
        None => {
            not_name = input.to_owned();
        }
    }

    match not_name.rsplit_once('#') {
        Some((start, end)) => {
            uri = start.to_owned();

            if !end.is_empty(){
                let mut reader = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(end.as_bytes());

                for record in reader.records() {
                    let record = record.unwrap();
                    for field in record.iter() {
                        hash.push(field.to_owned());
                    }
                    break
                }

            }
        },
        None => {
            uri = not_name.to_owned();
        }
    }

    return Input {
        name,
        uri,
        tables: hash
    };

}

fn default_name_and_check(input: &mut Input) -> Result<()> {
    let local = input.uri.ends_with(".parquet")
        || input.uri.ends_with(".csv")
        || input.uri.ends_with(".db")
        || input.uri.ends_with(".sqlite")
        || input.uri.starts_with("sqlite://");
    if !local {
        return Ok(());
    }

    let path = input.uri.strip_prefix("sqlite://").unwrap_or(&input.uri);
    let path_buf = std::path::PathBuf::from(path);

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

fn local_db_path(uri: &str) -> Option<String> {
    if let Some(path) = uri.strip_prefix("sqlite://") {
        return Some(path.to_string());
    }
    if !uri.contains("://") && (uri.ends_with(".db") || uri.ends_with(".sqlite")) {
        return Some(uri.to_string());
    }
    None
}

fn absolute_uri(uri: &str) -> String {
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

fn sidecar_path(anchor: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.sqlnow", anchor))
}

fn random_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time after epoch")
        .as_nanos();
    hasher.write_u128(nanos);
    format!("{:016x}", hasher.finish())
}

fn read_sidecar(path: &std::path::Path) -> Result<(Option<String>, Vec<(String, Input)>)> {
    let content = std::fs::read_to_string(path)?;
    let dir = path.parent().unwrap_or(std::path::Path::new("."));
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
            let local_buf = std::path::PathBuf::from(&local);
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

fn write_sidecar(path: &std::path::Path, id: &str, entries: &[(String, Input)]) -> Result<()> {
    let mut content = String::from(
        "# sqlnow auto-attach file. Inputs listed here are attached on startup.\n\
         # Format: view|table name=uri#table1,table2\n",
    );
    content.push_str(&format!("id {}\n", id));
    for (kind, input) in entries {
        content.push_str(&format!("{} {}={}", kind, input.name, absolute_uri(&input.uri)));
        if !input.tables.is_empty() {
            content.push_str(&format!("#{}", input.tables.join(",")));
        }
        content.push('\n');
    }
    std::fs::write(path, content)?;
    Ok(())
}

#[actix_web::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut views = vec![];
    let mut tables = vec![];

    if let Some(cli_views) = cli.view {
        for file in cli_views.iter() {
            let mut input = input_into_parts(file);
            default_name_and_check(&mut input)?;
            views.push(input);
        }
    }

    if let Some(cli_tables) = cli.table {
        for file in cli_tables.iter() {
            let mut input = input_into_parts(file);
            default_name_and_check(&mut input)?;
            tables.push(input);
        }
    }

    let mut sidecar_files = vec![];

    if let Some(cli_files) = cli.files {
        for file in cli_files.iter() {
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

    // a "<dbfile>.sqlnow" sidecar next to the --db database is read automatically
    if let Some(db) = &cli.db {
        let sidecar = sidecar_path(db);
        if sidecar.exists() {
            sidecar_files.push(sidecar.to_string_lossy().to_string());
        }
    }

    let mut replayed_id = None;
    let mut db_sidecar_id = None;

    for file in sidecar_files.iter() {
        let (id, entries) = read_sidecar(std::path::Path::new(file))?;
        let is_db_sidecar =
            cli.db.as_ref().map(|db| sidecar_path(db).to_string_lossy() == *file.as_str()).unwrap_or(false);
        if is_db_sidecar {
            db_sidecar_id = id;
        } else if replayed_id.is_none() {
            replayed_id = id;
        }
        for (kind, input) in entries {
            // inputs given on the command line win over sidecar entries
            if views.iter().chain(tables.iter()).any(|i| i.name == input.name) {
                continue;
            }
            if kind == "table" {
                tables.push(input);
            } else {
                views.push(input);
            }
        }
    }

    // with --db, file views and tables are persisted inside the duckdb file
    // itself; database attaches are the only thing lost between runs, so they
    // are saved to the sidecar automatically. The sidecar also holds a random
    // id so browser state (query history) can be scoped to this database.
    let auto_sidecar: Option<(std::path::PathBuf, String, Vec<(String, Input)>)> = cli.db.as_ref().map(|db| {
        let entries = views
            .iter()
            .filter(|i| i.is_database())
            .map(|i| ("view".to_string(), (*i).clone()))
            .collect();
        let id = db_sidecar_id.clone().unwrap_or_else(random_id);
        (sidecar_path(db), id, entries)
    });

    // --save <name>: record every input, so `sqlnow <name>.sqlnow` replays this command line
    let save_sidecar: Option<(std::path::PathBuf, String, Vec<(String, Input)>)> = cli.save.as_ref().map(|name| {
        let path = if name.ends_with(".sqlnow") {
            name.clone()
        } else {
            format!("{}.sqlnow", name)
        };
        let path = std::path::PathBuf::from(path);
        // keep the id stable when re-saving over an existing session file
        let id = match path.exists() {
            true => read_sidecar(&path).ok().and_then(|(id, _)| id),
            false => None,
        }
        .unwrap_or_else(random_id);
        let mut entries: Vec<_> = views.iter().map(|i| ("view".to_string(), i.clone())).collect();
        entries.extend(tables.iter().map(|i| ("table".to_string(), i.clone())));
        (path, id, entries)
    });

    // the id scopes browser-side state: --db wins, then --save, then a replayed
    // session file; plain in-memory runs share the unscoped state as before
    let scope = auto_sidecar
        .as_ref()
        .map(|(_, id, _)| id.clone())
        .or_else(|| save_sidecar.as_ref().map(|(_, id, _)| id.clone()))
        .or(replayed_id);


    let config = Config {
        database: cli.db,
        views,
        drop: cli.drop,
        all_text: cli.text,
        tables,
        scope,
    };

    let app_data = tokio::task::spawn_blocking(||
        get_app_data(config)
    ).await??;

    // only save sidecars once everything attached successfully
    if let Some((path, id, entries)) = auto_sidecar {
        write_sidecar(&path, &id, &entries)?;
    }
    if let Some((path, id, entries)) = save_sidecar {
        write_sidecar(&path, &id, &entries)?;
        println!("Saved inputs to {}, replay with: sqlnow {}", path.display(), path.display());
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

    //open::that(format!("http://{}:{}", host, port))?;
    println!("Server running on http://{}:{}", host, port);

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
