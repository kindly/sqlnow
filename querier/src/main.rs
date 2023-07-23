use eyre::Result;
use std::env;
use clap::Parser;
use libquerier::Config;
use libquerier::{main_web, get_app_data};
use actix_web::{App, HttpServer, web::Data};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Name of the person to greet
    files: Vec<String>,
}

#[actix_web::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut files = vec![];

    for file in cli.files.iter() {
        if !file.ends_with(".parquet") {
            return Err(eyre::eyre!("File {} is not a parquet file", file))
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

    HttpServer::new(move || {
      App::new()
          .configure(main_web)
          .app_data(Data::new(app_data.clone()))
      })
      .bind((host, port))?
      .run()
      .await?;
    //
    // Ok(())

    Ok(())
}
