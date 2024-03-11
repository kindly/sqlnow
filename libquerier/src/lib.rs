use actix_web::{error::Error, get, post, web, web::ServiceConfig, HttpResponse, Responder, Either};
use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use arrow::record_batch::RecordBatch;
use async_stream::stream;
use csv::ReaderBuilder;
use duckdb::{params, Connection};
use eyre::Result;
use include_dir::{include_dir, Dir, DirEntry};
use minijinja::{context, Environment};
use serde::Serialize;
use std::{sync::Arc, vec};
use tokio::sync::Mutex;
use std::collections::HashMap;

static TEMPLATE_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/templates");

#[derive(Debug, Clone)]
pub struct Config {
    pub files: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableMeta {
    name: String,
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

    for glob in vec![
        "**/*.html",
        "**/*.sql",
    ] { 
        for entry in TEMPLATE_DIR
            .find(glob)
            .expect("template dir should exist")
        {
            if let DirEntry::File(file) = entry {
                let content = file.contents_utf8().expect("utf8 file");
                let path = file.path();
                env.add_template_owned(path.to_string_lossy(), content)?;
            }
        }
    }


    //let connection = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let connection = Connection::open_in_memory().unwrap();

    connection
        .execute_batch("INSTALL parquet; LOAD parquet; INSTALL httpfs; LOAD https;")
        .unwrap();

    for (name, file) in config.files.iter() {
        if file.ends_with(".csv") {
            connection
                .execute_batch(&format!(
                    "CREATE VIEW \"{}\" AS SELECT * FROM read_csv('{}');",
                    name, file
                ))
                .unwrap();
        } else if file.ends_with(".parquet") {
            connection
                .execute_batch(&format!(
                    "CREATE VIEW \"{}\" AS SELECT * FROM read_parquet('{}');",
                    name, file
                ))
                .unwrap();
        }
    }

    let mut schemas = vec![];

    for (name, _) in config.files.iter() {
        let mut fields = vec![];

        let mut prepared = connection
            .prepare("select name, type from pragma_table_info(?)")
            .unwrap();
        let iter = prepared
            .query_map(params![name], |row| {
                Ok((row.get(0).unwrap(), row.get(1).unwrap()))
            })
            .unwrap();

        for value in iter {
            let (field_name, type_): (String, String) = value.unwrap();
            fields.push((field_name, type_));
        }

        schemas.push(TableMeta {
            name: name.into(),
            fields,
        });
    }

    Ok(AppData {
        config,
        connection: Arc::new(Mutex::new(connection)),
        schemas,
        env,
    })
}

pub fn main_web(service_config: &mut ServiceConfig) {
    service_config
        .service(ui)
        .service(post_sql);
}

fn run_query(sql: &str, conn: &Connection, display_limit: usize) -> Result<TableData> {
    let mut headers: Vec<String> = vec![];
    let mut rows: Vec<Vec<String>> = vec![];

    let mut prepared = conn.prepare(sql)?;

    let results = prepared.query_arrow(params![]).unwrap();

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

    let tables = app_data
        .schemas
        .iter()
        .map(|t| t.name.clone())
        .collect::<Vec<String>>();

    let current_schema = app_data.schemas.get(0).expect("at least onn table exists");

    let sql = generate_sql(&app_data, &current_schema, SqlType::SelectFields);

    let conn = app_data.connection.lock().await;

    let table_data = run_query(&sql.as_str(), &conn, 1000).map_err(ErrorInternalServerError)?;

    let res = tmpl
        .render(&context! {
            current_schema => current_schema,
            tables => tables,
            table_data => table_data,
            sql => sql,
            display_limit => "1000",
        })
        .map_err(|e| ErrorInternalServerError(e))?;

    Ok(HttpResponse::Ok().body(res))

}

#[derive(PartialEq, Copy, Clone)]
enum SqlType {
    SelectStar,
    SelectFields
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
    }).expect("should render")
}

#[post("/")]
async fn post_sql(app_data: web::Data<AppData>, q: web::Form<HashMap<String, String>>) -> Result<Either<impl Responder, impl Responder>, Error> {

    let form = q.clone();

    let tables = app_data
        .schemas
        .iter()
        .map(|t| t.name.clone())
        .collect::<Vec<String>>();

    let current_table = form.get("current_table").ok_or(ErrorBadRequest("current_table not found"))?;
    let current_schema = app_data.schemas.iter().find(|t| t.name == *current_table).unwrap();

    let display_limit = form.get("display_limit").unwrap_or(&"1000".to_string()).parse::<usize>().unwrap_or(1000);

    let mut other_sql = HashMap::new();
    for (key, value) in form.iter() {
        if key.starts_with("sql-") {
            other_sql.insert(key.to_owned(), value.to_owned());
        }
    }

    let mut sql = match other_sql.remove(&format!("sql-{current_table}")) {
        Some(sql) => sql.to_owned(),
        None => {
            generate_sql(&app_data, &current_schema, SqlType::SelectFields)
        }
    };

    if let Some(new_sql) = form.get("new_sql") {
        if new_sql == "select_star" {
            sql = generate_sql(&app_data, &current_schema, SqlType::SelectStar);
        } else if new_sql == "select_fields" {
            sql = generate_sql(&app_data, &current_schema, SqlType::SelectFields);
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
            Err(e) => return Err(e)
        }
    }

    let tmpl = app_data
        .env
        .get_template("layout.html")
        .expect("template exists");


    let conn = app_data.connection.lock().await;

    let other_sql_list: Vec<(String, String)> = other_sql.iter().map(
        |(k, v)| (k.to_owned(), v.to_owned())).collect();

    let table_data = run_query(&sql.as_str(), &conn, display_limit);

    if table_data.is_err() {
        let res = tmpl
            .render(&context! {
                current_schema => current_schema,
                other_sql => other_sql_list,
                tables => tables,
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
            tables => tables,
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
    WEB
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

    Ok(HttpResponse::Ok().insert_header(
        ("Content-Disposition", content_disposition)
    ).streaming(Box::pin(output_stream)))
}
