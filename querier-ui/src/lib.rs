mod utils;
use std::{collections::HashMap, vec};
use std::hash::{Hash, Hasher};

use minijinja::{context, Environment};
use include_dir::{include_dir, Dir, DirEntry};
use lazy_static::lazy_static;
use web_sys::{console, HtmlElement, HtmlInputElement, HtmlTextAreaElement};
use matchit::Router;
use std::collections::hash_map::DefaultHasher;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use js_sys::Object;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = vim)]
    fn open(options: &Object);
}

pub fn run_vim_open() {
    let options = js_sys::Object::new();
    js_sys::Reflect::set(&options, &"debug".into(), &false.into()).unwrap();
    open(&options);
}

static TEMPLATE_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/templates");


pub fn create_router() -> Router<String> {
    let mut router = Router::new();

    router.insert("/", "home".into()).unwrap();
    router.insert("/query", "query".into()).unwrap();
    router.insert("/history", "history".into()).unwrap();
    router.insert("/table/{name}", "table".into()).unwrap();
    router
}

fn concat(value: String, other: String) -> String {
    format!("{}{}", value, other)
}


lazy_static! {
    static ref ROUTER: Router<String> = create_router();
}

lazy_static! {
    static ref ENV: Environment<'static>  = {
        let mut env = Environment::new();
        env.add_filter("concat", concat);

        for glob in ["**/*.html", "**/*.sql"] {
            for entry in TEMPLATE_DIR.find(glob).expect("template dir should exist") {
                if let DirEntry::File(file) = entry {
                    let content = file.contents_utf8().expect("utf8 file");
                    let path = file.path();
                    let res = env.add_template_owned(path.to_string_lossy(), content);
                    match res {
                        Ok(_) => {
                            console::log_1(&"Added template".into());
                        }
                        Err(e) => {
                            console::log_1(&e.to_string().into());
                        }
                    }
                }
            }
        }
        env
    };
}

#[derive(Debug)]
struct Command {
    target: Option<String>,
    path: Option<String>,
    action: Option<String>,
    args: HashMap<String, String>,
}

impl Command {
    fn from_json(json: String) -> Result<Self, JsValue> {
        let value = serde_json::from_str(&json).map_err(|e| e.to_string())?;
        if let serde_json::Value::Object(map) = value {
            let target = map.get("target").and_then(|v| v.as_str()).map(|s| s.to_string());
            let path = map.get("path").and_then(|v| v.as_str()).map(|s| s.to_string());
            let action = map.get("action").and_then(|v| v.as_str()).map(|s| s.to_string());
            let mut args = HashMap::new();
            for (k, v) in map.iter() {
                if k != "target" && k != "path" && k != "action" {
                    args.insert(k.to_string(), v.as_str().unwrap_or("").to_string());
                }
            }

            Ok(Command {
                target,
                path,
                action,
                args,
            })
        } else {
            Err("Failed to convert to Command".into())
        }
    }
}


#[wasm_bindgen]
pub async fn render(element: JsValue) -> Result<(), JsValue>{
    utils::set_panic_hook();

    let document = web_sys::window().unwrap().document().unwrap();

    let Ok(html_element) = element.dyn_into::<HtmlElement>() else {
        console::log_1(&"Failed to convert to HtmlElement ".into());
        return Err("Failed to convert to HtmlElement".into())
    };

    let attributes = html_element.attributes();

    let mut commands = vec![];

    for i in 0..attributes.length() {
        let attr = attributes.item(i).unwrap();
        let name = attr.name();
        let value = attr.value();

        console::log_1(&value.clone().into());

        if name.starts_with("data-command") {
            commands.push(Command::from_json(value)?);
        }
    }

    console::log_1(&format!("Commands: {:?}", commands).into());

    for command in commands {

        let target_element  = if let Some(target) = &command.target  {
            document.query_selector(&target)?.unwrap().dyn_into::<HtmlElement>()?
        } else {
            html_element.clone()
        };

        if let Some(path) = &command.path {
            let m: matchit::Match<'_, '_, &String> = ROUTER.at(path.as_str()).unwrap();
            let route_params: HashMap<String, String> = m.params.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();

            let mut args = command.args.clone();
            args.extend(route_params.clone());

            let rendered = match m.value.as_str() {
                "home" => {
                    home().await?
                }
                "query" => {
                     query(args).await?
                }
                "history" => {
                    history()
                }
                "table" => {
                    table(args).await?
                }
                _ => {
                    console::log_1(&"No match".into());
                    "".to_string()
                }
            };
            target_element.set_outer_html(&rendered);
        } 

        if let Some(action) = &command.action {
            match action.as_str() {
                "select_tab" => {
                    select_tab(&target_element);
                }
                "get_results" => {
                    let result = get_results(command.args).await?;
                    target_element.set_outer_html(&result);
                }
                _ => {}
            };
        } 

    }
    run_vim_open();

    Ok(())

}



#[derive(serde::Deserialize)]
struct Queries {
    select_star: String,
    select_fields: String,
    select_fields_type: String,
}

async fn table(args: HashMap<String, String>) -> Result<String, JsValue> {

    let name = args.get("name").unwrap();

    let tmpl = ENV 
        .get_template("table.html")
        .expect("template exists");
    
    console::log_1(&format!("{name} started").into());

    let window = web_sys::window().unwrap();
    let location = window.location();

    let mut sql = window
        .local_storage().unwrap().unwrap()
        .get_item(&format!("querier-sql-table-{name}")).unwrap().unwrap_or("".to_string());

    if sql.is_empty() || args.contains_key("new_sql") {

        let client = reqwest::Client::new();
        let response = client.post(format!("{}/table.json", location.origin().unwrap()))
            .form(&[("name", name)])
            .send().await.unwrap();    
        let queries: Queries = response.json().await.map_err(|e| e.to_string())?;

        sql = queries.select_fields.clone();

        if let Some(search) = args.get("new_sql") {
            match search.as_str() {
                "select_star" => {
                    sql = queries.select_star.clone();
                }
                "select_fields" => {
                    sql = queries.select_fields.clone();
                }
                "select_fields_type" => {
                    sql = queries.select_fields_type.clone();
                }
                _ => {}
            }
            let local_storage = window.local_storage().unwrap().unwrap();

            let existing_sql = local_storage.get_item(&format!("querier-sql-table-{name}")).unwrap().unwrap_or("".to_string());
            if !existing_sql.is_empty() {
                add_to_history(existing_sql);
            }
            local_storage.set_item(&format!("querier-sql-table-{name}"), &sql).unwrap();
        }
    }

    let res = tmpl
        .render(&context! {
            sql => sql,
            page_type => "table",
            display_limit => "500",
            name => name,
        }).unwrap();

    return Ok(res)
}

async fn query(args: HashMap<String, String>) -> Result<String, JsValue> {
    let tmpl = ENV 
        .get_template("table.html")
        .expect("template exists");

    let window = web_sys::window().unwrap();

    let storage_sql = window
        .local_storage().unwrap().unwrap()
        .get_item(&format!("querier-sql-query-query")).unwrap().unwrap_or("".to_string());

    let sql = match args.get("sql") {
        Some(sql) => {
            window.local_storage().unwrap().unwrap().set_item(&format!("querier-sql-query-query"), &sql).unwrap();
            add_to_history(storage_sql.clone());
            sql.clone()
        }
        None => storage_sql   
    };

    let res = tmpl
        .render(&context! {
            sql => sql,
            page_type => "query",
            display_limit => "500",
            name => "query",
        }).unwrap();

    return Ok(res)
}

fn add_to_history(sql: String) {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    let hash = format!("{:x}", hasher.finish());

    let window = web_sys::window().unwrap();
    let local_storage = window.local_storage().unwrap().unwrap();

    local_storage.set_item(&format!("querier-history-{hash}"), &sql).unwrap();

    let history_list = local_storage.get_item("querier-history-list").unwrap().unwrap_or("".to_string());

    let mut history: Vec<String> = history_list.split(",").map(|s| s.to_string()).filter(|x| !x.is_empty()).collect();

    history.iter().position(|s| s == &hash).map(|i| history.remove(i));

    history.insert(0, hash.clone());

    local_storage.set_item("querier-history-list", &history.join(",")).unwrap();
}

async fn get_results(args: HashMap<String, String>) -> Result<String, JsValue> {
    let window = web_sys::window().unwrap();
    let document = window.document().unwrap();
    let location = window.location();

    let template = ENV.get_template("results.html").unwrap();
    let client = reqwest::Client::new();

    let name = args.get("name").unwrap_or(&"query".to_string()).to_owned();
    let page_type = args.get("page_type").unwrap_or(&"query".to_string()).to_owned();

    let sql = document.query_selector("#sql")?.unwrap().dyn_into::<HtmlTextAreaElement>()?.value();

    window.local_storage().unwrap().unwrap().set_item(&format!("querier-sql-{page_type}-{name}"), &sql).unwrap();

    add_to_history(sql.clone());

    let display_limit = document.query_selector("#display_limit")?.unwrap().dyn_into::<HtmlInputElement>()?.value();

    let response = client.post(format!("{}/query.json", location.origin().unwrap()))
        .form(&[("sql", sql), ("display_limit", display_limit)])
        .send().await.unwrap();

    let results: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;

    Ok(template.render(&context! {
        error => results["error"].as_str().unwrap_or(""),
        table_data => results.get("table_data"),
    }).unwrap())
}

fn history() -> String {
    let template = ENV.get_template("history.html").unwrap();

    let window = web_sys::window().unwrap();
    let local_storage = window.local_storage().unwrap().unwrap();

    let history_list = local_storage.get_item(&"querier-history-list").unwrap().unwrap_or("".to_string());
    let history_hash: Vec<String> = history_list.split(",").map(|s| s.to_string()).filter(|x| !x.is_empty()).collect();

    let mut history: Vec<String> = vec![];
    for hash in history_hash {
        let sql = local_storage.get_item(&format!("querier-history-{hash}")).unwrap().unwrap_or("".to_string());
        if sql.is_empty() {
            continue;
        }
        history.push(sql);
    }

    template.render(&context! {
        history => history,
    }).unwrap()
}

fn select_tab(target_element: &HtmlElement) {
    let document = web_sys::window().unwrap().document().unwrap();

    if let Ok(elements) = document.query_selector_all(".main_tab") {
        for i in 0..elements.length() {
            if let Some(element) = elements.item(i) {
                let element = element.dyn_into::<web_sys::Element>().unwrap();
                element.class_list().remove_1("active").unwrap();
            }
        }
    }
    target_element.class_list().add_1("active").unwrap();
}

async fn home() -> Result<String, JsValue> {
    let tmpl = ENV 
        .get_template("layout.html").expect("template exists");


    let client = reqwest::Client::new();

    let window = web_sys::window().unwrap();
    let location = window.location();

    let response = client.post(format!("{}/tables.json" ,location.origin().unwrap()))
            .send().await.unwrap();
    
    let tables: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;

    let sql = window
        .local_storage().unwrap().unwrap()
        .get_item(&format!("querier-sql-query-query")).unwrap().unwrap_or("".to_string());

    let res = tmpl
        .render(&context! {
            sql => sql,
            name => "query",
            page_type => "query",
            display_limit => "500",
            tables => tables,
        }).unwrap_or_else(|e| {
            console::log_1(&e.to_string().into());
            "".to_string()
        });

    return Ok(res)
}