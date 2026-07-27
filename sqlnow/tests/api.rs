//! The HTTP surface, as an agent or the UI sees it.

mod harness;
use harness::Workspace;
use serde_json::json;

#[test]
fn queries_can_be_added_renamed_and_deleted() {
    let space = Workspace::new("queries");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);

    let (status, created) =
        server.post_json("/api/queries", json!({"name": "by co2", "sql": "SELECT * FROM plants"}));
    assert_eq!(status, 201);
    assert_eq!(created["name"], "by co2");

    // the same name twice is a conflict, not a silent overwrite
    let (status, _) = server.post_json("/api/queries", json!({"name": "by co2", "sql": "SELECT 1"}));
    assert_eq!(status, 409);

    let (status, _) = server.put_json("/api/queries/by%20co2", json!({"name": "emitters"}));
    assert_eq!(status, 200);
    assert_eq!(server.query_names(), ["emitters"]);

    assert_eq!(server.delete("/api/queries/emitters"), 204);
    assert!(server.query_names().is_empty());

    // and the sql it held is still retrievable
    let history = server.get("/api/history");
    let kept: Vec<&str> =
        history["history"].as_array().unwrap().iter().map(|e| e["sql"].as_str().unwrap()).collect();
    assert!(kept.contains(&"SELECT * FROM plants"), "history holds {:?}", kept);
}

#[test]
fn a_run_reports_which_session_it_is_serving() {
    let space = Workspace::new("describe");
    let server = space.start(&["plants.duckdb", "-v", &space.csv("plants.csv").to_string_lossy()]);

    let described = server.get("/api/session");
    let id = space.exec_value(
        &space.path().join("plants.duckdb.sqlnow"),
        "SELECT id FROM sessions",
    );
    assert_eq!(described["id"], id);
    assert!(described["db"].as_str().unwrap().ends_with("plants.duckdb"));
    assert!(described["path"].as_str().unwrap().ends_with("plants.duckdb.sqlnow"));
}

#[test]
fn inputs_can_be_attached_and_detached_while_running() {
    let space = Workspace::new("inputs");
    let first = space.csv("plants.csv");
    let second = space.write("units.csv", "name,mw\nUnit 1,50\n");
    let server = space.start(&[&first.to_string_lossy()]);
    assert_eq!(server.tables(), ["plants"]);

    let (status, _) =
        server.post_json("/api/inputs", json!({"uri": second.to_string_lossy()}));
    assert_eq!(status, 201);
    assert_eq!(server.tables(), ["plants", "units"]);
    assert_eq!(server.query("SELECT count(*) FROM units")["table_data"]["rows"][0][0], "1");

    // and it is recorded, so a later run replays it
    let recorded = server.get("/api/inputs")["inputs"].as_array().unwrap().len();
    assert_eq!(recorded, 2);

    assert_eq!(server.delete("/api/inputs/units"), 204);
    assert_eq!(server.tables(), ["plants"]);
}

#[test]
fn attaching_reports_what_it_cannot_do() {
    let space = Workspace::new("attach-errors");
    let csv = space.csv("plants.csv");
    let server = space.start(&[&csv.to_string_lossy()]);

    // a name already taken
    let (status, body) = server.post_json("/api/inputs", json!({"uri": csv.to_string_lossy()}));
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("already attached"), "{}", body);

    // a path that is not there, reported before anything is attached
    let (status, body) = server.post_json("/api/inputs", json!({"uri": "/nowhere/gone.parquet"}));
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("does not exist"), "{}", body);
    assert_eq!(server.tables(), ["plants"]);

    // and detaching something that was never there
    assert_eq!(server.delete("/api/inputs/nothing"), 404);
}

#[test]
fn the_viewer_cannot_write_to_a_database() {
    let space = Workspace::new("read-only");
    let csv = space.csv("plants.csv");
    // -t loads the csv as a real table in the database, so the statements below
    // are aimed at data that genuinely could be destroyed
    let server = space.start(&["plants.duckdb", "-t", &csv.to_string_lossy()]);

    // reading is fine
    assert_eq!(server.query("SELECT count(*) FROM plants")["table_data"]["rows"][0][0], "2");

    // writing through the query editor is not, whatever the statement
    for sql in [
        "DELETE FROM plants",
        "INSERT INTO plants VALUES ('Plant C', 999)",
        "UPDATE plants SET co2 = 0",
        "DROP TABLE plants",
        "CREATE TABLE t(a INT)",
        "CREATE VIEW v AS SELECT 1",
    ] {
        let refused = server.query(sql);
        let error = refused["error"].as_str().unwrap_or_default();
        assert!(error.contains("read-only mode"), "{} was allowed: {}", sql, refused);
    }

    // and the data is exactly as it was
    assert_eq!(server.query("SELECT count(*) FROM plants")["table_data"]["rows"][0][0], "2");

    // but the API can still attach data, which is the one path that writes
    let more = space.write("units.csv", "name,mw\nUnit 1,50\n");
    let (status, _) = server.post_json("/api/inputs", json!({"uri": more.to_string_lossy()}));
    assert_eq!(status, 201);
    assert_eq!(server.tables(), ["plants", "units"]);
}

#[test]
fn a_query_run_anywhere_lands_in_history() {
    let space = Workspace::new("history");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);

    server.query("SELECT 41 + 1");
    let history = server.get("/api/history");
    let sql: Vec<&str> =
        history["history"].as_array().unwrap().iter().map(|e| e["sql"].as_str().unwrap()).collect();
    assert!(sql.contains(&"SELECT 41 + 1"), "history holds {:?}", sql);

    // it survives the run, which is the point of it
    server.stop();
    let after = space.exec(&space.store(), "SELECT sql FROM history");
    assert!(after.contains("SELECT 41 + 1"), "{}", after);
}

#[test]
fn the_page_is_served_with_the_session_it_belongs_to() {
    let space = Workspace::new("ui");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);

    let page = server.get_text("/");
    assert!(page.contains("<div id=\"root\""), "not the app: {}", &page[..page.len().min(400)]);

    // the id is injected before any of the app's code runs, so its stored state
    // (query history, open tab) can be kept per session rather than per origin
    let id = server.get("/api/session")["id"].as_str().unwrap().to_string();
    assert!(page.contains(&format!("window.SQLNOW_SCOPE = \"{}\"", id)), "no scope in the page");

    // and the assets it asks for are really there, which a broken embed would
    // not be: the whole UI is compiled into the binary
    let asset = page
        .split("src=\"")
        .find(|part| part.starts_with("/assets/"))
        .and_then(|part| part.split('"').next())
        .expect("the page references a script");
    assert_eq!(server.status(asset), 200, "{} is missing from the binary", asset);
}

#[test]
fn a_server_can_list_the_other_sessions() {
    let space = Workspace::new("session-list");
    let one = space.csv("one.csv").to_string_lossy().to_string();
    let two = space.write("two.csv", "name,mw\nUnit 1,50\n").to_string_lossy().to_string();

    // an older session to find, then the one doing the looking
    space.start(&[&one, "-q", "a=SELECT 1"]).stop();
    let server = space.start(&[&two]);

    let listed = server.get("/api/sessions");
    let sessions = listed["sessions"].as_array().expect("an array");
    assert_eq!(sessions.len(), 2, "{}", listed);

    // the one being served says so, and the other does not
    let here = server.get("/api/session")["id"].as_str().unwrap().to_string();
    let current: Vec<&serde_json::Value> =
        sessions.iter().filter(|s| s["current"] == true).collect();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0]["id"], here);

    // enough to offer a way in: what it holds, and where it was last served
    let other = sessions.iter().find(|s| s["current"] == false).unwrap();
    assert_eq!(other["queries"], 1);
    assert!(other["inputs"][0].as_str().unwrap().ends_with("one.csv"), "{}", other);
    assert!(other["url"].is_null(), "a closed session should claim no address: {}", other);
    assert!(sessions[0]["age_seconds"].as_i64().unwrap() >= 0);
}
