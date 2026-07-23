//! JSON API for session queries and history.
//!
//! These endpoints are the live channel for both the UI and agents: an agent
//! can add or update queries on a running server with plain curl, and the UI
//! picks the changes up on its next fetch. Everything reads through to the
//! session sidecar database — there is no in-memory copy to go stale.

use crate::session::{Session, SessionError};
use crate::AppData;
use actix_web::{delete, get, post, put, web, web::ServiceConfig, HttpResponse};
use serde::{Deserialize, Serialize};

pub fn configure(service_config: &mut ServiceConfig) {
    service_config
        .service(list_queries)
        .service(create_query)
        .service(get_query)
        .service(update_query)
        .service(delete_query)
        .service(list_history);
}

fn error_response(e: SessionError) -> HttpResponse {
    let body = serde_json::json!({ "error": e.to_string() });
    match e {
        SessionError::NotFound(_) => HttpResponse::NotFound().json(body),
        SessionError::Conflict(_) => HttpResponse::Conflict().json(body),
        SessionError::Invalid(_) => HttpResponse::BadRequest().json(body),
        SessionError::Locked(_) => HttpResponse::ServiceUnavailable().json(body),
        SessionError::Db(_) => HttpResponse::InternalServerError().json(body),
    }
}

fn with_session<T: Serialize>(
    app_data: &AppData,
    f: impl FnOnce(&Session) -> Result<T, SessionError>,
    ok: impl FnOnce(T) -> HttpResponse,
) -> HttpResponse {
    let session = match app_data.session.lock() {
        Ok(session) => session,
        Err(_) => {
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({ "error": "session lock poisoned" }))
        }
    };
    match f(&session) {
        Ok(value) => ok(value),
        Err(e) => error_response(e),
    }
}

#[derive(Deserialize)]
struct CreateQueryBody {
    name: Option<String>,
    sql: Option<String>,
}

#[derive(Deserialize)]
struct UpdateQueryBody {
    name: Option<String>,
    sql: Option<String>,
}

#[derive(Deserialize)]
struct HistoryParams {
    limit: Option<usize>,
}

#[get("/api/queries")]
async fn list_queries(app_data: web::Data<AppData>) -> HttpResponse {
    with_session(
        &app_data,
        |session| {
            let queries = session.list_queries()?;
            let open = session.open_query()?;
            Ok(serde_json::json!({ "open": open, "queries": queries }))
        },
        |body| HttpResponse::Ok().json(body),
    )
}

#[post("/api/queries")]
async fn create_query(
    app_data: web::Data<AppData>,
    body: web::Json<CreateQueryBody>,
) -> HttpResponse {
    with_session(
        &app_data,
        |session| session.create_query(body.name.as_deref(), body.sql.as_deref().unwrap_or("")),
        |query| HttpResponse::Created().json(query),
    )
}

#[get("/api/queries/{name}")]
async fn get_query(app_data: web::Data<AppData>, name: web::Path<String>) -> HttpResponse {
    with_session(
        &app_data,
        |session| session.get_query(&name),
        |query| HttpResponse::Ok().json(query),
    )
}

#[put("/api/queries/{name}")]
async fn update_query(
    app_data: web::Data<AppData>,
    name: web::Path<String>,
    body: web::Json<UpdateQueryBody>,
) -> HttpResponse {
    with_session(
        &app_data,
        |session| session.update_query(&name, body.sql.as_deref(), body.name.as_deref()),
        |query| HttpResponse::Ok().json(query),
    )
}

#[delete("/api/queries/{name}")]
async fn delete_query(app_data: web::Data<AppData>, name: web::Path<String>) -> HttpResponse {
    with_session(
        &app_data,
        |session| session.delete_query(&name),
        |_| HttpResponse::NoContent().finish(),
    )
}

#[get("/api/history")]
async fn list_history(
    app_data: web::Data<AppData>,
    params: web::Query<HistoryParams>,
) -> HttpResponse {
    with_session(
        &app_data,
        |session| {
            let history = session.list_history(params.limit.unwrap_or(0))?;
            Ok(serde_json::json!({ "history": history }))
        },
        |body| HttpResponse::Ok().json(body),
    )
}
