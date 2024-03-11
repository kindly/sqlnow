use eyre::Result;
use std::env;
use clap::Parser;
use libquerier::Config;
use libquerier::{main_web, get_app_data};
use actix_web::{App, HttpServer, web::Data};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    db_file: Option<String>,

    #[arg(short, long)]
    named_files: Option<Vec<String>>,
    files: Option<Vec<String>>,
}

#[actix_web::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut files = vec![];

    if let Some(cli_files) = cli.files {
        for file in cli_files.iter() {
            if !file.ends_with(".parquet") && !file.ends_with(".csv") {
                return Err(eyre::eyre!("File {} is not a parquet or csv file", file))
            }
            if file.starts_with("s3://") {
                let fake_file = file.get(5..).unwrap();
                let path_buf = std::path::PathBuf::from(fake_file);
                let table_name = path_buf.file_stem().expect("is file").to_string_lossy().to_string();
                files.push((table_name, file.clone()));
            } else {
                let path_buf = std::path::PathBuf::from(file);
                if !path_buf.exists() {
                    return Err(eyre::eyre!("File {} does not exist", file))
                }

                let table_name = path_buf.file_stem().expect("is file").to_string_lossy().to_string();

                files.push((table_name, file.clone()));
            }
        }
    }

    if let Some(named_files) = cli.named_files {
        println!("named_files: {:?}", named_files);
    }

    // if let Some(db_file) = cli.db_file {
    //     println!("db_file: {:?}", db_file);
    // }

    let config = Config {
        files
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

    //open::that(format!("http://{}:{}", host, port))?;
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
