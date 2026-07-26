//! JSON API for session queries and history.
//!
//! These endpoints are the live channel for both the UI and agents: an agent
//! can add or update queries on a running server with plain curl, and the UI
//! picks the changes up on its next fetch. Everything reads through to the
//! session sidecar database — there is no in-memory copy to go stale.

use crate::session::{Session, SessionError};
use crate::AppData;
use actix_web::{delete, get, post, put, web, web::Bytes, web::ServiceConfig, HttpResponse};
use async_stream::stream;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

pub fn configure(service_config: &mut ServiceConfig) {
    service_config
        .service(list_queries)
        .service(create_query)
        .service(get_query)
        .service(update_query)
        .service(delete_query)
        .service(list_history)
        .service(describe_session)
        .service(list_inputs)
        .service(create_input)
        .service(delete_input)
        .service(events);
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

/// Like `with_session`, but bumps the change counter on success so the
/// /api/events stream notices even for in-memory sessions.
fn with_session_mut<T: Serialize>(
    app_data: &AppData,
    f: impl FnOnce(&Session) -> Result<T, SessionError>,
    ok: impl FnOnce(T) -> HttpResponse,
) -> HttpResponse {
    let response = with_session(app_data, f, ok);
    if response.status().is_success() {
        app_data.session_version.fetch_add(1, Ordering::Relaxed);
    }
    response
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
    /// The SQL this edit was based on. When absent or stale, the stored SQL
    /// is preserved in history before being overwritten.
    base_sql: Option<String>,
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
    with_session_mut(
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
    with_session_mut(
        &app_data,
        |session| {
            session.update_query(
                &name,
                body.sql.as_deref(),
                body.name.as_deref(),
                body.base_sql.as_deref(),
            )
        },
        |query| HttpResponse::Ok().json(query),
    )
}

#[delete("/api/queries/{name}")]
async fn delete_query(app_data: web::Data<AppData>, name: web::Path<String>) -> HttpResponse {
    with_session_mut(
        &app_data,
        |session| session.delete_query(&name),
        |_| HttpResponse::NoContent().finish(),
    )
}

#[derive(Debug, Clone, Deserialize)]
struct NewInput {
    /// A file path, or a `postgresql://` / `sqlite://` URI.
    uri: String,
    /// What to call it. Defaults to the file stem, as on the command line.
    #[serde(alias = "as")]
    name: Option<String>,
    /// "view" (the default: query the file where it lies) or "table" (read it
    /// into the database once).
    kind: Option<String>,
    /// For a database input: only expose these tables, and never these.
    #[serde(default)]
    only: Vec<String>,
    #[serde(default)]
    except: Vec<String>,
}

/// Which session this server is serving.
///
/// The listing pings this to tell a running session from a stale address left
/// by a killed process: an answer is not enough, it has to be the right id,
/// because the port may since have been taken by something else entirely.
#[get("/api/session")]
async fn describe_session(app_data: web::Data<AppData>) -> HttpResponse {
    with_session(
        &app_data,
        |session| {
            Ok(serde_json::json!({
                "id": session.id(),
                "open": session.open_query()?,
                "path": session.path().map(|path| path.display().to_string()),
                "db": app_data.db,
            }))
        },
        |body| HttpResponse::Ok().json(body),
    )
}

/// The inputs this session will replay on its next launch.
#[get("/api/inputs")]
async fn list_inputs(app_data: web::Data<AppData>) -> HttpResponse {
    let inputs = match app_data.session.lock() {
        Ok(session) => session.list_inputs(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };
    match inputs {
        Ok(inputs) => {
            let recorded: Vec<_> = inputs
                .iter()
                .map(|(kind, input)| {
                    serde_json::json!({
                        "kind": kind,
                        "name": input.name,
                        "uri": input.uri,
                        "only": input.tables,
                        "except": input.except,
                    })
                })
                .collect();
            HttpResponse::Ok().json(serde_json::json!({ "inputs": recorded }))
        }
        Err(e) => error_response(e),
    }
}

/// Attach a file or database to the running session.
///
/// The table appears in the UI without a restart, and is recorded so later
/// launches replay it. With a main database, a view or table created here
/// lives in that file, so only database attaches need recording.
#[post("/api/inputs")]
async fn create_input(app_data: web::Data<AppData>, body: web::Json<NewInput>) -> HttpResponse {
    let kind = body.kind.clone().unwrap_or_else(|| "view".to_string());
    if kind != "view" && kind != "table" {
        return HttpResponse::BadRequest()
            .json(serde_json::json!({ "error": "kind must be \"view\" or \"table\"" }));
    }

    let mut input = crate::Input {
        name: body.name.clone().unwrap_or_default(),
        uri: body.uri.clone(),
        tables: body.only.clone(),
        except: body.except.clone(),
    };
    // fills in a missing name from the file stem, and reports a path that is
    // not there before anything is attached
    if let Err(e) = crate::default_name_and_check(&mut input) {
        return HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() }));
    }

    if let Err(e) = crate::add_input(&app_data, &kind, &input).await {
        return HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() }));
    }

    // a file view or table with a main database persists in that database, so
    // only attaches have to be replayed
    if app_data.db.is_none() || input.is_database() {
        if let Ok(session) = app_data.session.lock() {
            if let Err(e) = session.add_input(&kind, &input) {
                eprintln!("Attached {} but could not record it: {}", input.name, e);
            }
        }
    }
    app_data.session_version.fetch_add(1, Ordering::Relaxed);

    HttpResponse::Created().json(serde_json::json!({
        "name": input.name,
        "kind": kind,
        "uri": input.uri,
    }))
}

/// Detach an input by name, dropping its view or table.
#[delete("/api/inputs/{name}")]
async fn delete_input(app_data: web::Data<AppData>, name: web::Path<String>) -> HttpResponse {
    if let Err(e) = crate::remove_input(&app_data, &name).await {
        return HttpResponse::NotFound().json(serde_json::json!({ "error": e.to_string() }));
    }
    if let Ok(session) = app_data.session.lock() {
        if let Err(e) = session.remove_input(&name) {
            eprintln!("Detached {} but could not forget it: {}", name, e);
        }
    }
    app_data.session_version.fetch_add(1, Ordering::Relaxed);
    HttpResponse::NoContent().finish()
}

/// This server's own writes (the counter) plus anyone else's (the session's
/// `changed_at`, which an external writer moves too).
fn session_stamp(app_data: &AppData) -> (u64, Option<i64>) {
    let version = app_data.session_version.load(Ordering::Relaxed);
    let changed = app_data
        .session
        .lock()
        .ok()
        .and_then(|session| session.change_stamp());
    (version, changed)
}

/// Server-sent events: emits `data: changed` (within ~1s) whenever the
/// session changes — through this API, through /query.json history, or
/// through an external writer touching the sidecar file.
#[get("/api/events")]
async fn events(app_data: web::Data<AppData>) -> HttpResponse {
    let event_stream = stream! {
        yield Ok::<Bytes, actix_web::Error>(Bytes::from_static(b"retry: 2000\n\n"));
        let mut last = session_stamp(&app_data);
        let mut quiet_ticks: u32 = 0;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            let now = session_stamp(&app_data);
            if now != last {
                last = now;
                quiet_ticks = 0;
                yield Ok(Bytes::from_static(b"data: changed\n\n"));
            } else {
                quiet_ticks += 1;
                if quiet_ticks >= 15 {
                    quiet_ticks = 0;
                    yield Ok(Bytes::from_static(b": keepalive\n\n"));
                }
            }
        }
    };
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .streaming(event_stream)
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
