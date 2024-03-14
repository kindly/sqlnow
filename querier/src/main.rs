use eyre::Result;
use std::env;
use clap::Parser;
use libquerier::{Config, ExternalDatabase, DbType};
use libquerier::{main_web, get_app_data, };
use actix_web::{App, HttpServer, web::Data};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    table: Option<Vec<String>>,

    #[arg(short, long)]
    view: Option<Vec<String>>,

    #[arg(short, long)]
    postgres: Option<Vec<String>>,

    #[arg(short, long)]
    sqlite: Option<Vec<String>>,

    #[arg(long)]
    drop: bool,

    db: Option<String>,
}

#[actix_web::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut view_files = vec![];

    if let Some(cli_files) = cli.view {
        for file in cli_files.iter() {
            if !file.ends_with(".parquet") && !file.ends_with(".csv") {
                return Err(eyre::eyre!("File {} is not a parquet or csv file", file))
            }

            let mut path= file.to_owned();
            let mut table_name = "".to_owned();

            if let Some(index)  = file.find("=") {
                if file.len() > index+1 {
                    table_name = file.get(0..index).expect("checked length").to_owned();
                    path = file.get(index+1..).expect("checked length").to_owned();
                }
            }

            if path.starts_with("s3://") {
                let fake_file = file.get(5..).unwrap();
                let path_buf = std::path::PathBuf::from(fake_file);
                if table_name.is_empty() {
                    table_name = path_buf.file_stem().expect("is file").to_string_lossy().to_string();
                }
                view_files.push((table_name, path));
            } else {
                let path_buf = std::path::PathBuf::from(&path);
                if !path_buf.exists() {
                    return Err(eyre::eyre!("File {} does not exist", path))
                }

                if table_name.is_empty() {
                    table_name = path_buf.file_stem().expect("is file").to_string_lossy().to_string();
                }

                view_files.push((table_name, path));
            }
        }
    }

    let mut external_databases = vec![];

    if let Some(databases) = cli.postgres {
        for database in databases.iter() {
            if let Some(index)  = database.find("=") {
                if database.len() > index+1 {
                    let database_name = database.get(0..index).expect("checked length").to_owned();
                    let connection_string = database.get(index+1..).expect("checked length").to_owned();
                    external_databases.push(ExternalDatabase {
                        name: database_name,
                        connection_string,
                        db_type: DbType::Postgres,
                    });
                } else {
                    return Err(eyre::eyre!("Database should be in the format name=connection_string"))}
            } else {
                return Err(eyre::eyre!("Database should be in the format name=connection_string"))
            }
        }
    }

    if let Some(databases) = cli.sqlite {
        for database in databases.iter() {
            if let Some(index)  = database.find("=") {
                if database.len() > index+1 {
                    let database_name = database.get(0..index).expect("checked length").to_owned();
                    let connection_string = database.get(index+1..).expect("checked length").to_owned();
                    external_databases.push(ExternalDatabase {
                        name: database_name,
                        connection_string,
                        db_type: DbType::Sqlite,
                    });
                } else {
                    return Err(eyre::eyre!("Database should be in the format name=connection_string"))}
            } else {
                return Err(eyre::eyre!("Database should be in the format name=connection_string"))
            }
        }
    }

    let config = Config {
        database: cli.db,
        view_files,
        drop: cli.drop,
        table_files: vec![],
        external_databases
    };

    let app_data = get_app_data(config).await?;

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

    open::that(format!("http://{}:{}", host, port))?;

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
