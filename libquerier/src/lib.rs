use datafusion::prelude::*;
use actix_web::{get, web, HttpResponse, Responder, error::Error, web::ServiceConfig};
use std::sync::Arc;
use eyre::Result;
use minijinja::{Environment, context};
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use url;
use std::env::var;
use tokio::time::timeout;
use include_dir::{include_dir, Dir, DirEntry};
use serde_json::{json, Value};
use futures_util::StreamExt;
use async_stream::stream;
use csvs_convert::{Describer, DescriberOptions};

static TEMPLATE_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/templates");

#[derive(Debug, Clone, Deserialize)]
struct Params {
    sql: Option<String>
}

#[derive(Debug, Clone)]
pub struct Config {
    pub files: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableMeta {
    name: String,
    fields: Vec<(String, String)>,
} 

#[derive(Clone)]
pub struct AppData {
    pub config: Config,
    pub schemas: Vec<TableMeta>,
    pub ctx: SessionContext,
    pub env: Environment<'static>,
}


pub async fn get_app_data (config:Config) -> Result<AppData> {
    let mut env = Environment::new();

    for entry in TEMPLATE_DIR.find("**/*.html").expect("template dir should exist"){
        match entry {
            DirEntry::File(file) => {
                let content = file.contents_utf8().expect("utf8 file");
                let path = file.path();
                env.add_template_owned(path.to_string_lossy(), content)?;
            },
            _ => {}
        }
    };

    let mut cfg = datafusion::prelude::SessionConfig::new();
    cfg = cfg.set_bool("datafusion.optimizer.prefer_hash_join", false);
    cfg = cfg.set_u64("datafusion.execution.target_partitions", 1);

    let mut runtime_cfg = datafusion::execution::runtime_env::RuntimeConfig::new();
    // let memory_pool = datafusion::execution::memory_pool::FairSpillPool::new(1024 * 1024 * 1024 * 4);
    // runtime_cfg = runtime_cfg.with_memory_pool(Arc::new(memory_pool));
    //
    let workers = match var("WORKERS") {
        Ok(v) => {
            v.parse::<usize>()?
        },
        Err(_) => {
            1
        }
    };

    let memory_fraction = workers as f64 / 100.0;

    match var("MAX_MEMORY") {
        Ok(v) => {
            let max_memory = v.parse::<f64>()?;
            runtime_cfg = runtime_cfg.with_memory_limit((1024.0 * 1024.0 * 1024.0 * max_memory) as usize, memory_fraction);
        },
        Err(_) => {
            runtime_cfg = runtime_cfg.with_memory_limit((1024.0 * 1024.0 * 1024.0 * 4.0) as usize, memory_fraction);
        }
    }

    let runtime = datafusion::execution::runtime_env::RuntimeEnv::new(runtime_cfg)?;

    let mut domains = vec![];

    for (_, file) in config.files.iter() {
        if file.starts_with("s3") {
            let parsed = url::Url::parse(file)?;
            let domain = parsed[..url::Position::BeforePath].to_string();
            if !domains.contains(&domain) {
                domains.push(domain);
            }
        }
    }

    for domain in domains.iter() {
        if domain.starts_with("s3") {
            let obj_store = object_store::aws::AmazonS3Builder::from_env().with_url(domain).build()?;
            runtime.register_object_store(&url::Url::parse(domain)?, Arc::new(obj_store));
        }
    }
    //runtime.register_object_store(, object_store)

    let ctx = SessionContext::with_config_rt(cfg, runtime.into());

    let mut schemas = vec![];

    for (name, file) in config.files.iter() {
        if file.ends_with(".csv") {
            ctx.register_csv(&name, &file, CsvReadOptions::default()).await?;
        } else {
            ctx.register_parquet(&name, &file, ParquetReadOptions::default()).await?;
        }
        

        let table = ctx.table_provider(name).await.unwrap();
        let schema = table.schema();
        let table_meta = TableMeta {
            name: name.clone(),
            fields: schema.fields().iter().map(|f| {
                (f.data_type().to_string(),
                f.name().clone())
            }).collect(),
        };
        schemas.push(table_meta);
    }


    Ok(AppData {
        config,
        schemas,
        ctx,
        env,
    })
}

pub async fn get_stats(app_data: &AppData) -> Vec<Value> {

    let mut table_stats = vec![];

    for table in app_data.schemas.iter() {
        let dataframe = app_data.ctx.table(&table.name).await.expect("table should exist");
        let mut fields = vec![];
        let mut describers = vec![]; 
        for field in dataframe.schema().fields() {
            fields.push(field.name().clone());
            describers.push(Describer::new_with_options(DescriberOptions::builder().stats(true).build()));
        }

        let mut stream = dataframe.execute_stream().await.expect("stream should exist");
        let mut csv_data = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result.unwrap();

            let mut writer = arrow_csv::WriterBuilder::new().has_headers(false).build(csv_data);
            writer.write(&batch).unwrap();
            csv_data = writer.into_inner();
            let mut reader = ReaderBuilder::new().from_reader(csv_data.as_slice());
            for row in reader.records() {
                let row = row.unwrap();
                for (num, cell) in row.iter().enumerate() {
                    describers[num].process(cell)
                }
            }
        }

        let mut field_stats = vec![];
        let mut field_types = vec![];
        for mut describer in describers {
            field_stats.push(describer.stats());
            field_types.push(describer.guess_type());
        }
        table_stats.push(json!({"name": table.name, "stats": field_stats, "fields": fields, "types": field_types}));
    }

    return table_stats
}


pub fn main_web(service_config: &mut ServiceConfig) {
    service_config
        .service(ui)
        .service(json_results)
        .service(csv_results);
}


#[get("/")]
async fn ui(app_data:  web::Data<AppData>, q: web::Query<Params>
) -> Result<impl Responder, Error> {

    let mut headers = vec![];
    let mut rows = vec![];

    let tmpl = app_data.env.get_template("table.html").expect("template exists");

    if q.sql.is_none() || q.sql.as_ref().unwrap().is_empty() {
        let res = tmpl.render(&context! {
            schemas => app_data.schemas,
        }).map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
        
        return Ok(HttpResponse::Ok().body(res))
    }

    if let Some(sql) = &q.sql {

        let df_result = app_data.ctx.sql(sql).await;

        if let Err(e) = df_result.as_ref() {
            let res = tmpl.render(&context! {
                schemas => app_data.schemas,
                error => e.to_string(),
                sql => q.sql.clone().unwrap_or("".to_string()),
            }).map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
            return Ok(HttpResponse::Ok().body(res))
        }

        let filtered = df_result.expect("just checked").limit(0, Some(1000)).unwrap();

        //let batches = filtered.collect().await.map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(2000), 
            filtered.collect()
        ).await.map_err(
                |e| actix_web::error::ErrorInternalServerError(e)
            )?
            .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;


        let csv_data = vec![];

        let mut csv_writer = arrow_csv::writer::WriterBuilder::new().has_headers(true).build(csv_data);

        for batch in batches {
            csv_writer.write(&batch).unwrap();
            // if headers.is_empty() {
            //     for field in batch.schema().fields() {
            //         headers.push(field.name().clone());
            //     }
            // }
            // batch.columns().iter().for_each(|col| {
            //     
            // });
            //
            // let res = arrow_json::writer::record_batches_to_json_rows(&[&batch]).unwrap();
            // output.extend(res);
        }

        let csv_data = csv_writer.into_inner();


        let mut reader = ReaderBuilder::new().from_reader(csv_data.as_slice());

        for row in reader.records() {
            let data = row.unwrap().iter().map(|s| s.to_string()).collect::<Vec<String>>();
            rows.push(data);
        }

        for header in reader.headers().unwrap() {
            headers.push(header.to_string());
        };
    }

    
    let res = tmpl.render(&context! {
        schemas => app_data.schemas,
        headers => headers,
        rows => rows,
        sql => q.sql.clone().unwrap_or("".to_string()),
    }).map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    
    Ok(HttpResponse::Ok().body(res))
    
}


#[get("/results.json")]
async fn json_results(app_data:  web::Data<AppData>, q: web::Query<Params>
) -> Result<impl Responder, Error> {

    if q.sql.is_none() || q.sql.as_ref().unwrap().is_empty() {
        return Ok(HttpResponse::Ok().json(json!({"error": "no sql"})))
    }

    let sql = q.sql.as_ref().expect("just checked");

    let df_result = app_data.ctx.sql(&sql.clone()).await;

    if let Err(e) = df_result.as_ref() {
        return Ok(HttpResponse::Ok().json(json!({"error": e.to_string()})))
    }
    let df_stream = df_result.expect("just checked").execute_stream().await.unwrap();

    let mut stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(df_stream.schema(), df_stream);

    let output_stream = stream! {
        loop {
            let batch = timeout(
                std::time::Duration::from_secs(20), 
                    stream.next()
                ).await.map_err(
                    |e| actix_web::error::ErrorInternalServerError(e)
                )?;

            if batch.is_none() {
                break;
            }
            let batch = batch.expect("batch exists").map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
            let buf = Vec::new();
            let mut writer = arrow_json::LineDelimitedWriter::new(buf);

            writer.write_batches(&vec![&batch]).map_err(|e| actix_web::error::ErrorInternalServerError(e))?;

            writer.finish().map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
            let buf = writer.into_inner();
            yield Ok::<actix_web::web::Bytes, Error>(actix_web::web::Bytes::from(buf));
        }
        
        // for await batch in stream {
        //     let batch = batch.unwrap();
        //     let buf = Vec::new();
        //     let mut writer = arrow_json::LineDelimitedWriter::new(buf);
        //     writer.write_batches(&vec![&batch]).map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
        //     writer.finish().map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
        //     let buf = writer.into_inner();
        //     yield Ok::<actix_web::web::Bytes, Error>(actix_web::web::Bytes::from(buf));
        // }
    };
    
    Ok(HttpResponse::Ok().streaming(Box::pin(output_stream)))
}


#[get("/results.csv")]
async fn csv_results(app_data:  web::Data<AppData>, q: web::Query<Params>
) -> Result<impl Responder, Error> {

    if q.sql.is_none() || q.sql.as_ref().unwrap().is_empty() {
        return Ok(HttpResponse::Ok().json(json!({"error": "no sql"})))
    }

    let sql = q.sql.as_ref().expect("just checked");

    let df_result = app_data.ctx.sql(&sql.clone()).await;

    if let Err(e) = df_result.as_ref() {
        return Ok(HttpResponse::Ok().json(json!({"error": e.to_string()})))
    }
    let df_stream = df_result.expect("just checked").execute_stream().await.unwrap();

    let mut stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(df_stream.schema(), df_stream);

    let output_stream = stream! {
        let mut has_headers = true;
        loop {
            let batch = timeout(
                std::time::Duration::from_secs(20), 
                    stream.next()
                ).await.map_err(
                    |e| actix_web::error::ErrorInternalServerError(e)
                )?;

            if batch.is_none() {
                break;
            }
            let batch = batch.expect("batch exists").map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
            let buf = Vec::new();
            let mut writer = arrow_csv::WriterBuilder::new().has_headers(has_headers).build(buf);
            has_headers = false;
            writer.write(&batch).map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
            let buf = writer.into_inner();
            yield Ok::<actix_web::web::Bytes, Error>(actix_web::web::Bytes::from(buf));
        }
    };
    
    Ok(HttpResponse::Ok().streaming(Box::pin(output_stream)))
}
