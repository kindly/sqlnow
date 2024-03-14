use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{
    error::Error, get, post, web, web::ServiceConfig, Either, HttpResponse, Responder,
};
use arrow::record_batch::RecordBatch;
use async_stream::stream;
use csv::ReaderBuilder;
use duckdb::{params, Connection};
use eyre::Result;
use include_dir::{include_dir, Dir, DirEntry};
use minijinja::filters::length;
use minijinja::{context, Environment};
use serde::Serialize;
use std::collections::HashMap;
use std::{sync::Arc, vec};
use tokio::sync::Mutex;

static TEMPLATE_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/templates");

#[derive(Debug, Clone)]
pub enum DbType {
    Postgres,
    Sqlite,
    Mysql,
}

#[derive(Debug, Clone)]
pub struct ExternalDatabase {
    pub db_type: DbType,
    pub name: String,
    pub connection_string: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database: Option<String>,
    pub view_files: Vec<(String, String)>,
    pub table_files: Vec<(String, String)>,
    pub external_databases: Vec<ExternalDatabase>,
    pub drop:bool,
}

#[derive(Debug, Clone, Serialize)]
struct DBTable {
    catalog: String,
    schema: String,
    name: String,
    //table_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableMeta {
    catalog: String,
    schema: String,
    name: String,
    db_name: String,
    schema_display_name: String,
    fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableData {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Clone)]
pub struct AppData {
    pub config: Config,
    pub connection: Arc<Mutex<Connection>>,
    pub schemas: Vec<TableMeta>,
    pub env: Environment<'static>,
}

pub async fn get_app_data(config: Config) -> Result<AppData> {
    
    let mut env = Environment::new();

    for glob in ["**/*.html", "**/*.sql"] {
        for entry in TEMPLATE_DIR.find(glob).expect("template dir should exist") {
            if let DirEntry::File(file) = entry {
                let content = file.contents_utf8().expect("utf8 file");
                let path = file.path();
                env.add_template_owned(path.to_string_lossy(), content)?;
            }
        }
    }

    //let connection = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let connection = match config.database.clone() {
        Some(db) => {
            Connection::open(db).unwrap()
        }
        None => {
            Connection::open_in_memory().unwrap()
        }
    };

    //Arc::new(Mutex::new(Connection::open(db).unwrap()))

    connection
        .execute_batch(
            "INSTALL parquet; LOAD parquet; 
                 INSTALL httpfs; LOAD https; 
                 INSTALL aws; LOAD aws; 
                 INSTALL postgres; LOAD postgres;
                 INSTALL sqlite; LOAD sqlite;
                 INSTALL mysql; LOAD mysql;
                 ",
        )?;

    if config.drop {
        for (name, _) in config.table_files.iter().chain(config.view_files.iter()) {
            match connection.execute_batch(&format!("DROP TABLE IF EXISTS \"{}\";", name)) {
                Ok(_) => {}
                Err(e) => {if !e.to_string().contains("Catalog Error") {
                    return Err(e.into());
                }}
            }
            match connection.execute_batch(&format!("DROP VIEW IF EXISTS \"{}\";", name)) {
                Ok(_) => {}
                Err(e) => {if !e.to_string().contains("Catalog Error") {
                    return Err(e.into());
                }}
            }
        }
    }

    for external_database in config.external_databases.iter() {
        let db_type_string = match external_database.db_type {
            DbType::Postgres => "POSTGRES",
            DbType::Sqlite => "SQLITE",
            DbType::Mysql => "MYSQL",
        };
        let sql = format!("ATTACH '{}' AS {} (TYPE {})", external_database.connection_string, external_database.name, db_type_string);
        connection.execute_batch(&sql)?;
    }

    let external_database_names = config
        .external_databases
        .iter()
        .map(|db| db.name.clone())
        .collect::<Vec<String>>();

    for (name, file) in config.view_files.iter() {
        if file.ends_with(".csv") {
            connection
                .execute_batch(&format!(
                    "CREATE VIEW IF NOT EXISTS \"{}\" AS SELECT * FROM read_csv('{}', header = true);",
                    name, file
                ))?;
        } else if file.ends_with(".parquet") {
            connection
                .execute_batch(&format!(
                    "CREATE VIEW IF NOT EXISTS \"{}\" AS SELECT * FROM read_parquet('{}');",
                    name, file
                ))?;
        }
    }

    for (name, file) in config.table_files.iter() {
        if file.ends_with(".csv") {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS \"{}\" AS SELECT * FROM read_csv('{}', header = true);",
                    name, file
                ))?
        } else if file.ends_with(".parquet") {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS \"{}\" AS SELECT * FROM read_parquet('{}');",
                    name, file
                ))?
        }
    }


    let mut schemas = vec![];

    let mut prepared = connection
        .prepare("select table_catalog, table_schema, table_name from information_schema.tables 
                       where table_schema not in ('information_schema', 'pg_catalog')")?;

    let mapped_rows = prepared.query_map([], |row| {
        Ok(DBTable {
            schema: row.get(1)?,
            name: row.get(2)?,
            catalog: row.get(0)?,
        })
    })?;
    
    for row in mapped_rows {
        let t = row.expect("should be able to get table");
        let mut fields = vec![];

        let mut prepared = connection
            .prepare("select column_name, data_type from information_schema.columns where table_catalog = ? and table_schema = ? and table_name = ?")?;
        let iter = prepared
            .query_map(params![t.catalog, t.schema, t.name], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;

        for value in iter {
            let (field_name, type_): (String, String) = value?;
            fields.push((field_name, type_));
        }

        let external_database = external_database_names.contains(&t.catalog);

        let schema = if t.schema == "main" && !external_database {
            "".to_string()
        } else {
            t.schema
        };

        let schema_display_name = if external_database {
            format!("{}.{}", t.catalog, schema)
        } else {
            schema.clone()
        };

        let mut db_name = String::new();
        if external_database {
            db_name.push_str(&format!("\"{}\".", t.catalog));
        }
        if external_database || !schema.is_empty() {
            db_name.push_str(&format!("\"{}\".", schema));
        }
        db_name.push_str(&format!("\"{}\"", t.name));


        if fields.is_empty() {
            continue;
        }

        schemas.push(TableMeta {
            catalog: t.catalog,
            schema,
            name: t.name,
            db_name,
            schema_display_name,
            fields,
        });
    };

    println!("schemas: {:?}", schemas);
    if schemas.len() == 0 {
        return Err(eyre::eyre!("No tables found"));
    }

    Ok(AppData {
        config,
        connection: Arc::new(Mutex::new(connection)),
        schemas,
        env,
    })
}

pub fn main_web(service_config: &mut ServiceConfig) {
    service_config.service(ui).service(post_sql);
}

fn run_query(sql: &str, conn: &Connection, display_limit: usize) -> Result<TableData> {
    let mut headers: Vec<String> = vec![];
    let mut rows: Vec<Vec<String>> = vec![];

    let mut prepared = conn.prepare(sql)?;

    let results = prepared.query_arrow(params![])?;

    let csv_data = vec![];

    let mut csv_writer = arrow_csv::writer::WriterBuilder::new()
        .with_header(true)
        .build(csv_data);

    let mut total_lines = 0;

    for batch in results {
        total_lines += batch.num_rows();
        csv_writer.write(&batch).unwrap();
        if total_lines >= display_limit {
            break;
        }
    }

    let csv_data = csv_writer.into_inner();

    let mut reader = ReaderBuilder::new().from_reader(csv_data.as_slice());

    for (row_number, row) in reader.records().enumerate() {
        if row_number >= display_limit {
            break;
        }
        let data = row
            .unwrap()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        rows.push(data);
    }

    for header in reader.headers().unwrap() {
        headers.push(header.to_string());
    }

    Ok(TableData { headers, rows })
}

#[get("/")]
async fn ui(app_data: web::Data<AppData>) -> Result<impl Responder, Error> {
    let tmpl = app_data
        .env
        .get_template("layout.html")
        .expect("template exists");

    let current_schema = app_data.schemas.get(0).expect("at least one table should exists");

    let sql = generate_sql(&app_data, &current_schema, SqlType::SelectFields);

    let conn = app_data.connection.lock().await;

    let table_data = run_query(&sql.as_str(), &conn, 500).map_err(ErrorInternalServerError)?;

    let res = tmpl
        .render(&context! {
            current_schema => current_schema,
            schemas => app_data.schemas.clone(),
            table_data => table_data,
            sql => sql,
            display_limit => "500",
        })
        .map_err(|e| ErrorInternalServerError(e))?;

    Ok(HttpResponse::Ok().body(res))
}

#[derive(PartialEq, Copy, Clone)]
enum SqlType {
    SelectStar,
    SelectFields,
}

fn generate_sql(app_data: &AppData, schema: &TableMeta, sql_type: SqlType) -> String {
    let template = match sql_type {
        SqlType::SelectStar => "select_star.sql",
        SqlType::SelectFields => "table_schema.sql",
    };

    let sql_tmpl = app_data
        .env
        .get_template(template)
        .expect("template exists");

    sql_tmpl
        .render(&context! {
            schema  => schema,
        })
        .expect("should render")
}

#[post("/")]
async fn post_sql(
    app_data: web::Data<AppData>,
    q: web::Form<HashMap<String, String>>,
) -> Result<Either<impl Responder, impl Responder>, Error> {
    let form = q.clone();

    let current_table = form
        .get("current_table")
        .ok_or(ErrorBadRequest("current_table not found"))?;

    let current_schema = app_data
        .schemas
        .iter()
        .find(|t| t.db_name == *current_table)
        .unwrap();

    let display_limit = form
        .get("display_limit")
        .unwrap_or(&"1000".to_string())
        .parse::<usize>()
        .unwrap_or(1000);

    let mut other_sql = HashMap::new();
    for (key, value) in form.iter() {
        if key.starts_with("sql-") {
            other_sql.insert(key.to_owned(), value.to_owned());
        }
    }

    let mut sql = match other_sql.remove(&format!("sql-{current_table}")) {
        Some(sql) => sql.to_owned(),
        None => generate_sql(&app_data, current_schema, SqlType::SelectFields),
    };

    if let Some(new_sql) = form.get("new_sql") {
        if new_sql == "select_star" {
            sql = generate_sql(&app_data, current_schema, SqlType::SelectStar);
        } else if new_sql == "select_fields" {
            sql = generate_sql(&app_data, current_schema, SqlType::SelectFields);
        }
    }

    let output_format = if form.contains_key("jsonl") {
        OutputFormat::JSON
    } else if form.contains_key("tab") {
        OutputFormat::TSV
    } else if form.contains_key("csv") {
        OutputFormat::CSV
    } else {
        OutputFormat::WEB
    };

    if output_format != OutputFormat::WEB {
        match outputs(app_data, sql, output_format).await {
            Ok(res) => return Ok(Either::Right(res)),
            Err(e) => return Err(e),
        }
    }

    let tmpl = app_data
        .env
        .get_template("layout.html")
        .expect("template exists");

    let conn = app_data.connection.lock().await;

    let other_sql_list: Vec<(String, String)> = other_sql
        .iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

    let table_data = run_query(&sql.as_str(), &conn, display_limit);

    if table_data.is_err() {
        let res = tmpl
            .render(&context! {
                current_schema => current_schema,
                other_sql => other_sql_list,
                schemas => app_data.schemas.clone(),
                table_data => TableData { headers: vec![], rows: vec![] },
                display_limit => display_limit.to_string(),
                sql => sql,
                sql_error => table_data.unwrap_err().to_string(),
            })
            .map_err(|e| ErrorInternalServerError(e))?;

        return Ok(Either::Left(HttpResponse::Ok().body(res)));
    }

    let res = tmpl
        .render(&context! {
            current_schema => current_schema,
            other_sql => other_sql_list,
            schemas => app_data.schemas.clone(),
            table_data => table_data.unwrap(),
            display_limit => display_limit.to_string(),
            sql => sql,
        })
        .map_err(|e| ErrorInternalServerError(e))?;

    Ok(Either::Left(HttpResponse::Ok().body(res)))
}

#[derive(PartialEq, Copy, Clone)]
enum OutputFormat {
    CSV,
    TSV,
    JSON,
    WEB,
}

async fn outputs(
    app_data: web::Data<AppData>,
    sql: String,
    output: OutputFormat,
) -> Result<impl Responder, Error> {
    // if q.sql.is_none() || q.sql.as_ref().unwrap().is_empty() {
    //     return Ok(HttpResponse::Ok().json(json!({"error": "no sql"})));
    // }

    let output_stream = stream! {
        let conn = app_data.connection.lock().await;
        let mut prepared = conn.prepare(&sql).unwrap();
        let mut has_headers = true;

        for batch in prepared.query_arrow([]).unwrap() {
            let buf = Vec::new();
            match output {
                OutputFormat::CSV => {
                    let mut writer = arrow_csv::WriterBuilder::new().with_header(has_headers).build(buf);
                    has_headers = false;
                    let record_batch = RecordBatch::from(batch);
                    writer.write(&record_batch).map_err(ErrorInternalServerError)?;
                    let buf = writer.into_inner();
                    yield Ok::<actix_web::web::Bytes, Error>(actix_web::web::Bytes::from(buf));
                }
                OutputFormat::TSV => {
                    let mut writer = arrow_csv::WriterBuilder::new().with_header(has_headers).with_delimiter(b'\t').build(buf);
                    has_headers = false;
                    let record_batch = RecordBatch::from(batch);
                    writer.write(&record_batch).map_err(ErrorInternalServerError)?;
                    let buf = writer.into_inner();
                    yield Ok::<actix_web::web::Bytes, Error>(actix_web::web::Bytes::from(buf));
                }
                OutputFormat::JSON => {
                    let mut writer = arrow_json::LineDelimitedWriter::new(buf);
                    writer.write(&batch).map_err(ErrorInternalServerError)?;
                    let buf = writer.into_inner();
                    yield Ok::<actix_web::web::Bytes, Error>(actix_web::web::Bytes::from(buf));
                }
                OutputFormat::WEB => {}
            }
        }
    };

    let content_disposition = match output {
        OutputFormat::CSV => "attachment; filename=download.csv",
        OutputFormat::TSV => "attachment; filename=download.tsv",
        OutputFormat::JSON => "attachment; filename=download.json",
        OutputFormat::WEB => "",
    };

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Disposition", content_disposition))
        .streaming(Box::pin(output_stream)))
}
