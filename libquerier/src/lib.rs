use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{
    error::Error, get, post, web, web::ServiceConfig, Either, HttpResponse, Responder, web::Bytes
};
use async_stream::stream;
use csv::WriterBuilder;
use duckdb::{params, Connection};
use eyre::Result;
use include_dir::{include_dir, Dir, DirEntry};
use minijinja::{context, Environment};
use serde::{Deserialize, Serialize};
use serde_json::{self, json};
use std::collections::HashMap;
use std::{sync::Arc, vec};
use tokio::sync::Mutex;
use duckdb::types::ValueRef;

static TEMPLATE_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/templates");
static STATIC_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/static");

#[derive(Debug, Clone, Copy)]
pub enum DbType {
    Postgres,
    Sqlite,
}


#[derive(Debug, Clone)]
pub struct Input {
    pub name: String,
    pub uri: String,
    pub tables: Vec<String>,
}

impl Input {
    pub fn is_database(&self) -> bool {
        self.uri.starts_with("postgresql://") || self.uri.starts_with("sqlite://")
    }
    pub fn db_type(&self) -> DbType {
        if self.uri.starts_with("postgresql://") {
            DbType::Postgres
        } else if self.uri.starts_with("sqlite://") {
            DbType::Sqlite
        } else {
            unreachable!()
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database: Option<String>,
    pub views: Vec<Input>,
    pub tables: Vec<Input>,
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
pub struct Tab {
    name: String,
    tab_type: String,
    schema: Option<TableMeta>,
    section: Option<String>,
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
    pub tabs: Vec<Tab>,
    pub sections: Vec<String>,
    pub env: Environment<'static>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct History {
    pub hash: HashMap<String, String>,
    pub history: Vec<String>,
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
                 SET GLOBAL sqlite_all_varchar = true;
                 ",
        )?;

    if config.drop {
        for input in config.tables.iter().chain(config.views.iter()) {
            if input.is_database() {
                continue;
            }
            match connection.execute_batch(&format!("DROP TABLE IF EXISTS \"{}\";", input.name)) {
                Ok(_) => {}
                Err(e) => {if !e.to_string().contains("Catalog Error") {
                    return Err(e.into());
                }}
            }
            match connection.execute_batch(&format!("DROP VIEW IF EXISTS \"{}\";", input.name)) {
                Ok(_) => {}
                Err(e) => {if !e.to_string().contains("Catalog Error") {
                    return Err(e.into());
                }}
            }
        }
    }

    let mut external_database_map = HashMap::new();

    for input in config.views.iter() {
        if input.is_database() {
            let mut connection_string = input.uri.clone(); 
            let db_type_string = if input.uri.starts_with("postgresql://") {
                "POSTGRES"
            } else if input.uri.starts_with("sqlite://") {
                connection_string = input.uri.replace("sqlite://", "");
                "SQLITE"
            } else {
                return Err(eyre::eyre!("Database type not supported"));
            };

            let sql = format!("ATTACH '{}' AS {} (TYPE {})", connection_string, input.name, db_type_string);
            connection.execute_batch(&sql)?;
            
            external_database_map.insert(input.name.clone(), input.clone());
        } else {
            if input.uri.ends_with(".csv") {
                connection
                    .execute_batch(&format!(
                        "CREATE VIEW IF NOT EXISTS \"{}\" AS SELECT * FROM read_csv('{}', header = true);",
                        input.name, input.uri 
                    ))?;
            } else if input.uri.ends_with(".parquet") {
                connection
                    .execute_batch(&format!(
                        "CREATE VIEW IF NOT EXISTS \"{}\" AS SELECT * FROM read_parquet('{}');",
                        input.name, input.uri
                    ))?;
            }
        }
    }


    for input in config.tables.iter() {
        if input.is_database() {
            return Err(eyre::eyre!("External database not yet supported for tables"));
        }
        if input.uri.ends_with(".csv") {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS \"{}\" AS SELECT * FROM read_csv('{}', header = true);",
                    input.name, input.uri
                ))?
        } else if input.uri.ends_with(".parquet") {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS \"{}\" AS SELECT * FROM read_parquet('{}');",
                    input.name, input.uri
                ))?
        }
    }


    let mut tabs = vec![];

    tabs.push(Tab{
        name: "query".to_string(),
        tab_type: "query".to_string(),
        schema: None,
        section: None,
    });

    tabs.push(Tab{
        name: "history".to_string(),
        tab_type: "history".to_string(),
        schema: None,
        section: None,
    });

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

        let external_database = external_database_map.get(&t.catalog);

        if let Some(external_database) = external_database {
            if !external_database.tables.is_empty() {
                if t.catalog == external_database.name {
                    if !external_database.tables.contains(&t.name) {
                        continue;
                    }
                } 
            }
        }

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

        let schema = if t.schema == "main" && !external_database.is_some() {
            "".to_string()
        } else {
            t.schema
        };

        let schema_display_name = if external_database.is_some() {
            match external_database.unwrap().db_type() {
                DbType::Postgres => {
                    if schema == "public" {
                        t.catalog.clone()
                    } else {
                        format!("{}.{}", t.catalog, schema)
                    }
                }
                DbType::Sqlite => {
                    if schema == "main" {
                        t.catalog.clone()
                    } else {
                        format!("{}.{}", t.catalog, schema)
                    }
                }
            }
        } else {
            schema.clone()
        };

        let mut db_name = String::new();
        if external_database.is_some() {
            db_name.push_str(&format!("\"{}\".", t.catalog));
        }

        if external_database.is_some() || !schema.is_empty() {

            let db_type = external_database.unwrap().db_type();

            match db_type {
                DbType::Postgres => {
                    if schema != "public" {
                        db_name.push_str(&format!("\"{}\".", schema));
                    }
                }
                DbType::Sqlite => {
                    if schema != "main" {
                        db_name.push_str(&format!("\"{}\".", schema));
                    }
                }
            }

        }
        db_name.push_str(&format!("\"{}\"", t.name));


        if fields.is_empty() {
            continue;
        }

        let section = if schema_display_name.is_empty() {
            None
        } else {
            Some(schema_display_name.clone())
        };

        let table_meta = TableMeta {
            catalog: t.catalog,
            schema,
            name: t.name,
            db_name,
            schema_display_name,
            fields,
        };

        tabs.push(Tab {
            name: table_meta.db_name.clone().replace("\"", ""),
            tab_type: "table".to_string(),
            schema: Some(table_meta),
            section
        });
    };

    if tabs.len() == 1 {
        return Err(eyre::eyre!("No tables found"));
    }

    let mut section_list = tabs.iter().filter_map(|t| t.section.clone()).collect::<Vec<String>>();
    section_list.sort();
    section_list.dedup();

    Ok(AppData {
        config,
        connection: Arc::new(Mutex::new(connection)),
        tabs,
        sections: section_list,
        env,
    })
}

pub fn main_web(service_config: &mut ServiceConfig) {
    service_config
       .service(sql_query)
       .service(ui)
       .service(post_sql)
       .service(static_files)
       .service(tables)
       .service(table)
       .service(outputs);
}

fn process_row(row: &duckdb::Row, headers: &Vec<String>) -> Vec<String> {
    let mut data = vec![];
    for i in 0..headers.len() {
        let value =  match row.get_ref(i).unwrap() {
            ValueRef::Null => "".to_string(),
            ValueRef::Boolean(bool) => bool.to_string(),
            ValueRef::TinyInt(int) => int.to_string(),
            ValueRef::SmallInt(int) => int.to_string(),
            ValueRef::Int(int) => int.to_string(),
            ValueRef::BigInt(int) => int.to_string(),
            ValueRef::HugeInt(int) => int.to_string(),
            ValueRef::UTinyInt(int) => int.to_string(),
            ValueRef::USmallInt(int) => int.to_string(),
            ValueRef::UInt(int) => int.to_string(),
            ValueRef::UBigInt(int) => int.to_string(),
            ValueRef::Float(float) => float.to_string(),
            ValueRef::Double(double) => double.to_string(),
            ValueRef::Decimal(decimal) => decimal.to_string(),
            ValueRef::Timestamp(_, b) => b.to_string(),
            ValueRef::Text(text) => String::from_utf8_lossy(text).to_string(),
            ValueRef::Blob(blob) => String::from_utf8_lossy(blob).to_string(),
            ValueRef::Date32(date) => date.to_string(),
            ValueRef::Time64(_, b) => b.to_string(),
        };
        data.push(value)
    }
    data
}

fn run_query(sql: &str, conn: &Connection, display_limit: usize) -> Result<TableData> {
    let mut headers: Vec<String> = vec![];
    let mut rows: Vec<Vec<String>> = vec![];

    let mut prepared = conn.prepare(sql)?;

    let mut db_rows = prepared.query([])?;

    let statement = db_rows.as_ref().expect("should be able to get rows");

    headers.extend(statement.column_names());

    let mut count: usize = 0;

    while let Some(row) = db_rows.next()? {
        rows.push(process_row(&row, &headers));
        count += 1;
        if count >= display_limit {
            break;
        }
    }

    Ok(TableData { headers, rows: rows })
}

#[get("/static/{filename:.*}")]
async fn static_files(filename: web::Path<String>) -> Result<impl Responder, Error> {
    let data = STATIC_DIR.get_file(filename.as_str()).ok_or(ErrorBadRequest("file not found"))?;
    let contents = data.contents();

    let content_type = if filename.as_str().ends_with(".css") {
        "text/css"
    } else if filename.as_str().ends_with(".js") {
        "application/javascript"
    } else if filename.as_str().ends_with(".wasm") {
        "application/wasm"
    } else {
        "text/html"
    };

    return Ok(
        HttpResponse::Ok()
            .append_header(("Content-Type", content_type))
            //.append_header(("Cache-Control", "max-age=31536000"))
            .body(contents)
    );
}

#[get("/")]
async fn ui(app_data: web::Data<AppData>) -> Result<impl Responder, Error> {
    let tmpl = app_data
        .env
        .get_template("index.html")
        .expect("template exists");
    let res = tmpl.render(&context! {}).map_err(|e| ErrorInternalServerError(e))?;

    Ok(HttpResponse::Ok().body(res))
}

#[post("/tables.json")]
async fn tables(app_data: web::Data<AppData>) -> Result<impl Responder, Error> {
    let table_tabs = app_data.tabs.iter().filter(
        |t| t.tab_type == "table"
    ).collect::<Vec<&Tab>>();
    let output = json!({
        "tables": table_tabs,
        "sections": app_data.sections.clone()
    });

    Ok(HttpResponse::Ok().json(output))
}


#[derive(Debug, Clone, Deserialize, Serialize)]
struct TableRequest {
    name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TableResponse {
    table: String,
    select_star: String,
    select_fields: String,
}

#[post("/table.json")]
async fn table(app_data: web::Data<AppData>, post_data: web::Form<TableRequest>) -> Result<impl Responder, Error> {
    let table = app_data.tabs.iter().find(|t| t.name == post_data.name).ok_or(ErrorBadRequest("table not found"))?;

    let select_star = generate_sql(&app_data, table.schema.as_ref().expect("checked"), SqlType::SelectStar);
    let select_fields = generate_sql(&app_data, table.schema.as_ref().expect("checked"), SqlType::SelectFields);

    Ok(HttpResponse::Ok().json(TableResponse {
        table: table.name.clone(),
        select_star,
        select_fields,
    }))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SqlRequest {
    sql: String,
    display_limit: String,
}

#[derive(Debug, Clone, Serialize)]
struct SqlResponse {
    error: Option<String>,
    table_data: TableData,
}

#[post("/query.json")]
async fn sql_query(app_data: web::Data<AppData>, post_data: web::Form<SqlRequest>) -> Result<impl Responder, Error> {
    let sql = post_data.sql.clone();

    let conn = app_data.connection.lock().await;

    let table_data = if sql.is_empty() {
        Ok(TableData { headers: vec![], rows: vec![] })
    } else {
        run_query(&sql.as_str(), &conn, post_data.display_limit.parse().unwrap_or(500))
    };

    match table_data {
        Ok(table_data) => {
            Ok(HttpResponse::Ok().json(SqlResponse {
                error: None,
                table_data,
            }))
        }
        Err(e) => {
            Ok(HttpResponse::Ok().json(SqlResponse {
                error: Some(e.to_string()),
                table_data: TableData { headers: vec![], rows: vec![] },
            }))
        }
    }


}


#[post("/outputs")]
async fn outputs(
    app_data: web::Data<AppData>,
    q: web::Form<HashMap<String, String>>,
) -> Result<impl Responder, Error> {
    let form = q.clone();

    let sql = form
        .get("sql")
        .ok_or(ErrorBadRequest("sql not found"))?
        .to_owned();

    let output_format = if form.contains_key("jsonl") {
        OutputFormat::JSON
    } else if form.contains_key("tab") {
        OutputFormat::TSV
    } else if form.contains_key("csv") {
        OutputFormat::CSV
    } else {
        OutputFormat::CSV
    };

    match output_stream(app_data, sql, output_format).await {
        Ok(res) => return Ok(res),
        Err(e) => return Err(e),
    }

}

// #[get("/")]
// async fn ui(app_data: web::Data<AppData>) -> Result<impl Responder, Error> {
//     let tmpl = app_data
//         .env
//         .get_template("layout.html")
//         .expect("template exists");

//     let current_tab = app_data.tabs.get(0).expect("at least one table should exists");

//     let sql = "";

//     let res = tmpl
//         .render(&context! {
//             current_tab => current_tab,
//             tabs => app_data.tabs,
//             sql => sql,
//             display_limit => "500",
//             sections => app_data.sections
//         })
//         .map_err(|e| ErrorInternalServerError(e))?;

//     Ok(HttpResponse::Ok().body(res))
// }

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

    let current_tab_name = form
        .get("current_tab")
        .ok_or(ErrorBadRequest("current_tab not found"))?;

    let current_tab = app_data
        .tabs
        .iter()
        .find(|t| t.name == *current_tab_name)
        .unwrap();

    let display_limit = form
        .get("display_limit")
        .unwrap_or(&"500".to_string())
        .parse::<usize>()
        .unwrap_or(1000);

    let mut other_sql = HashMap::new();
    for (key, value) in form.iter() {
        if key.starts_with("sql-") {
            other_sql.insert(key.to_owned(), value.to_owned());
        }
    }

    let mut sql = match other_sql.remove(&format!("sql-{current_tab_name}")) {
        Some(sql) => sql.to_owned(),
        None => {
            if let Some(schema) = current_tab.schema.as_ref() {
                generate_sql(&app_data, schema, SqlType::SelectFields)
            } else { 
                "".to_owned()
            }
        },
    };

    if let Some(new_sql) = form.get("new_sql") {
        if current_tab.schema.is_some() {
            if new_sql == "select_star" {
                sql = generate_sql(&app_data, &current_tab.schema.as_ref().expect("checked"), SqlType::SelectStar);
            } else if new_sql == "select_fields" {
                sql = generate_sql(&app_data, &current_tab.schema.as_ref().expect("checked"), SqlType::SelectFields);
            }
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
        match output_stream(app_data, sql, output_format).await {
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

    if current_tab.tab_type == "history" {

        let history_json = form.get("history").ok_or(ErrorBadRequest("history not found"))?;
        let history = serde_json::from_str::<History>(history_json).map_err(|e| ErrorBadRequest(format!("Bad JSON: {e}")))?;

        let mut sql_history = vec![];
        for hash in history.history {
            if let Some(sql) = history.hash.get(&hash) {
                sql_history.push(sql.clone());
            }
        }

        let res = tmpl
            .render(&context! {
                current_tab => current_tab,
                other_sql => other_sql_list,
                tabs => app_data.tabs,
                display_limit => display_limit.to_string(),
                sections => app_data.sections,
                history => sql_history,
            })
            .map_err(|e| ErrorInternalServerError(e))?;
        return Ok(Either::Left(HttpResponse::Ok().body(res)));
    }

    let table_data = if sql.is_empty() {
        Ok(TableData { headers: vec![], rows: vec![] })
    } else {
        run_query(&sql.as_str(), &conn, display_limit)
    };

    if table_data.is_err() {
        let res = tmpl
            .render(&context! {
                current_tab => current_tab,
                other_sql => other_sql_list,
                tabs => app_data.tabs.clone(),
                table_data => TableData { headers: vec![], rows: vec![] },
                display_limit => display_limit.to_string(),
                sql => sql,
                sql_error => table_data.unwrap_err().to_string(),
                sections => app_data.sections
            })
            .map_err(|e| ErrorInternalServerError(e))?;

        return Ok(Either::Left(HttpResponse::Ok().body(res)));
    }

    let res = tmpl
        .render(&context! {
            current_tab => current_tab,
            other_sql => other_sql_list,
            tabs => app_data.tabs.clone(),
            table_data => table_data.unwrap(),
            display_limit => display_limit.to_string(),
            sql => sql,
            sections => app_data.sections
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

async fn output_stream(
    app_data: web::Data<AppData>,
    sql: String,
    output: OutputFormat,
) -> Result<impl Responder, Error> {

    let output_stream = stream! {

        let conn = app_data.connection.lock().await;
        let mut prepared = conn.prepare(&sql).unwrap();
        let mut db_rows = prepared.query([]).unwrap();

        let mut headers: Vec<String> = vec![];

        let statement = db_rows.as_ref().expect("should be able to get rows");

        headers.extend(statement.column_names());

        match output {
            OutputFormat::CSV => {
                let buf = Vec::new();
                let mut writer = WriterBuilder::new().from_writer(buf);
                writer.write_record(&headers).map_err(ErrorInternalServerError)?;
                let buf = writer.into_inner().map_err(ErrorInternalServerError)?;
                yield Ok::<Bytes, Error>(Bytes::from(buf));
            }
            _ => {}
        }

        while let Some(row) = db_rows.next().map_err(ErrorInternalServerError)? {
            let row = process_row(&row, &headers);
            let mut buf = Vec::new();
            match output {
                OutputFormat::CSV => {
                    let mut writer = WriterBuilder::new().from_writer(buf);
                    writer.write_record(row).map_err(ErrorInternalServerError)?;
                    let buf = writer.into_inner().map_err(ErrorInternalServerError)?;
                    yield Ok::<Bytes, Error>(Bytes::from(buf));
                }
                OutputFormat::TSV => {
                    let mut writer = WriterBuilder::new().delimiter(b'\t').from_writer(buf);
                    writer.write_record(row).map_err(ErrorInternalServerError)?;
                    let buf = writer.into_inner().map_err(ErrorInternalServerError)?;
                    yield Ok::<Bytes, Error>(Bytes::from(buf));
                }
                OutputFormat::JSON => {
                    let map = headers.iter().zip(row.iter()).collect::<HashMap<_, _>>();
                    serde_json::to_writer(&mut buf, &map).map_err(ErrorInternalServerError)?;
                    yield Ok::<Bytes, Error>(Bytes::from(buf));
                    yield Ok::<Bytes, Error>(Bytes::from("\n"));
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

    let content_type = match output {
        OutputFormat::CSV => "text/csv",
        OutputFormat::TSV => "text/tab-separated-values",
        OutputFormat::JSON => "application/json",
        OutputFormat::WEB => "text/html",
    };

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Disposition", content_disposition))
        .insert_header(("Content-Type", content_type))
        .streaming(Box::pin(output_stream)))
}
