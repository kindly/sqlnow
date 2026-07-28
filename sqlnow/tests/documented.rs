//! The commands AGENTS.md tells agents to run, run.
//!
//! That file is compiled into the binary and served by `--agents-help`, so a
//! schema change can quietly turn its recipes into lies — which happened twice
//! while format 2 was being built. These tests are deliberately the documented
//! shapes verbatim rather than the tidiest way to express the same thing.

mod harness;
use harness::Workspace;

#[test]
fn a_session_can_be_seeded_before_the_first_launch() {
    let space = Workspace::new("seeded");
    let csv = space.csv("plants.csv");

    // exec creates the file with its schema, so an agent can prepare a session
    // with nothing running and no duckdb installed
    space.exec(
        &space.path().join("session.sqlnow"),
        "INSERT INTO queries(session, pos, name, sql)
           SELECT id, 1, 'top emitters', 'SELECT * FROM plants ORDER BY co2 DESC' FROM sessions",
    );
    space.exec(
        &space.path().join("session.sqlnow"),
        "INSERT INTO meta(session, key, value) SELECT id, 'open', 'top emitters' FROM sessions",
    );

    let server = space.start(&["session.sqlnow", &csv.to_string_lossy()]);
    assert_eq!(server.query_names(), ["top emitters"]);
    assert_eq!(server.get("/api/queries")["open"], "top emitters");
    // the landing query is offered as a deep link, which is what gets relayed
    server.wait_for_output("/queries/top%20emitters");
}

#[test]
fn dollar_quoting_survives_awkward_sql() {
    let space = Workspace::new("dollar-quoted");
    let csv = space.csv("plants.csv");
    let session = space.path().join("session.sqlnow");

    space.exec(
        &session,
        "INSERT INTO queries(session, pos, name, sql)
           SELECT id, 1, 'names', $q$SELECT name FROM plants WHERE name = 'it''s fine'$q$
           FROM sessions",
    );

    let server = space.start(&["session.sqlnow", &csv.to_string_lossy()]);
    let stored = server.get("/api/queries")["queries"][0]["sql"].as_str().unwrap().to_string();
    assert_eq!(stored, "SELECT name FROM plants WHERE name = 'it''s fine'");
}

#[test]
fn what_a_user_did_can_be_read_back_afterwards() {
    let space = Workspace::new("read-back");
    let csv = space.csv("plants.csv");

    let server = space.start(&[&csv.to_string_lossy(), "-q", "peek=SELECT * FROM plants"]);
    server.query("SELECT count(*) FROM plants");
    server.stop();

    // the two recipes from "reading back what the user did"
    let history = space.exec(&space.store(), "SELECT \"at\", sql FROM history ORDER BY \"at\" DESC");
    assert!(history.contains("SELECT count(*) FROM plants"), "{}", history);
    let queries = space.exec(&space.store(), "SELECT name, sql FROM queries ORDER BY pos");
    assert!(queries.contains("peek"), "{}", queries);
}

#[test]
fn sql_answers_questions_without_a_server() {
    let space = Workspace::new("sql-subcommand");
    let csv = space.csv("plants.csv");

    // the documented way to get a scratch database, then query files in place
    space.exec(&space.path().join("scratch.sqlnow"), "SELECT 1");
    let out = space.run(&[
        "sql",
        "scratch.sqlnow",
        &format!("SELECT count(*) FROM read_csv('{}')", csv.display()),
        "-f",
        "csv",
    ]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains('2'), "{}", text);
}

#[test]
fn sql_reads_a_database_a_server_is_holding() {
    let space = Workspace::new("sql-alongside");
    let csv = space.csv("plants.csv");
    let server = space.start(&["plants.duckdb", "-t", &csv.to_string_lossy()]);

    // the server holds the database read-only; this has to work anyway
    let out = space.run(&["sql", "plants.duckdb", "SELECT count(*) FROM plants", "-f", "csv"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains('2'));

    // and so does exec against the store, which is never held
    let _ = server;
    let listed = space.exec(&space.store(), "SELECT count(*) FROM sessions");
    assert!(listed.lines().last().unwrap().parse::<i64>().unwrap() >= 1);
}

#[test]
fn the_documented_styling_recipe_styles_and_stays_out_of_the_download() {
    let space = Workspace::new("styling");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);

    // section 6's own example, in the shapes it documents
    let sql = "SELECT name, co2,
                      CASE WHEN co2 > 300 THEN 'warn' END AS _sqlnow_format_co2,
                      'width:420; wrap' AS _sqlnow_column_name,
                      56 AS _sqlnow_row_height
               FROM plants ORDER BY name";
    let table = server.query(sql)["table_data"].clone();
    let headers: Vec<&str> =
        table["headers"].as_array().unwrap().iter().map(|h| h.as_str().unwrap()).collect();
    // the grid is handed all of it — the styling is only in the result
    assert!(headers.contains(&"_sqlnow_format_co2"));
    assert!(headers.contains(&"_sqlnow_column_name"));
    assert!(headers.contains(&"_sqlnow_row_height"));
    assert_eq!(table["rows"][1][2], "warn");

    // and the recipe printed under it, verbatim
    let downloaded =
        server.export("SELECT 1 AS co2, 'warn' AS _sqlnow_format_co2", "csv");
    assert_eq!(downloaded, "co2\n1\n");
}

#[test]
fn the_agent_guide_ships_inside_the_binary() {
    let space = Workspace::new("agents-help");
    let printed = space.run_text(&["--agents-help"]);
    // if this drifts from the file, the binary is serving stale instructions
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../AGENTS.md"),
    )
    .expect("AGENTS.md");
    assert_eq!(printed.trim(), source.trim());
}
