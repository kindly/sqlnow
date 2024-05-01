use eyre::Result;
use std::env;
use clap::Parser;
use libquerier::Config;
use libquerier::{main_web, get_app_data, Input};
use actix_web::{App, HttpServer, web::Data};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    table: Option<Vec<String>>,

    #[arg(short, long)]
    view: Option<Vec<String>>,

    #[arg(long)]
    drop: bool,

    db: Option<String>,
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

#[actix_web::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut views = vec![];
    let mut tables = vec![];

    if let Some(cli_views) = cli.view {
        for file in cli_views.iter() {
            let mut input = input_into_parts(file);

            if file.ends_with(".parquet") || file.ends_with(".csv") {
                let path_buf = std::path::PathBuf::from(&input.uri);
                if !input.uri.starts_with("s3://") && !path_buf.exists() {
                    return Err(eyre::eyre!("File {} does not exist", input.uri))
                }

                if input.name.is_empty() {
                    input.name = path_buf.file_stem().expect("is file").to_string_lossy().to_string();
                }
            }

            views.push(input);
        }
    }

    if let Some(cli_tables) = cli.table {
        for file in cli_tables.iter() {
            let mut input = input_into_parts(file);

            if file.ends_with(".parquet") || file.ends_with(".csv") {
                let path_buf = std::path::PathBuf::from(&input.uri);
                if !input.uri.starts_with("s3://") && !path_buf.exists() {
                    return Err(eyre::eyre!("File {} does not exist", input.uri))
                }

                if input.name.is_empty() {
                    input.name = path_buf.file_stem().expect("is file").to_string_lossy().to_string();
                }
            }
            tables.push(input);
        }
    }


    let config = Config {
        database: cli.db,
        views,
        drop: cli.drop,
        tables,
    };

    let app_data = tokio::task::spawn_blocking(||
        get_app_data(config)
    ).await??;

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
