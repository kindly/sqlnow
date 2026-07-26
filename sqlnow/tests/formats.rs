//! How results come out: the streaming downloads the UI offers, and the
//! formats the CLI prints for an agent to parse.

mod harness;
use harness::Workspace;

#[test]
fn a_result_can_be_downloaded_in_each_format() {
    let space = Workspace::new("exports");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);
    let sql = "SELECT name, co2 FROM plants ORDER BY name";

    let csv = server.export(sql, "csv");
    assert_eq!(csv, "name,co2\nPlant A,120\nPlant B,340\n");

    // the tab format is a csv with a different delimiter, header included: a
    // headerless export is unusable in a spreadsheet, which is what it is for
    let tab = server.export(sql, "tab");
    assert_eq!(tab, "name\tco2\nPlant A\t120\nPlant B\t340\n");

    // json lines carry their keys on every row, so there is no header
    let jsonl = server.export(sql, "jsonl");
    let rows: Vec<serde_json::Value> =
        jsonl.lines().map(|line| serde_json::from_str(line).expect("a json object")).collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], "Plant A");
    assert_eq!(rows[0]["co2"], "120");
}

#[test]
fn sql_prints_what_it_was_asked_for() {
    let space = Workspace::new("sql-formats");
    let csv = space.csv("plants.csv");
    let query = format!("SELECT name, co2 FROM read_csv('{}') ORDER BY name", csv.display());
    // sql refuses to invent a database, so make one the documented way
    space.exec(&space.path().join("scratch.sqlnow"), "SELECT 1");

    // the default is a duckdb-style table for a human
    let boxed = space.run_text(&["sql", "scratch.sqlnow", &query]);
    assert!(boxed.contains("Plant A") && boxed.contains('│'), "{}", boxed);

    let csv_out = space.run_text(&["sql", "scratch.sqlnow", &query, "-f", "csv"]);
    assert_eq!(csv_out.trim(), "name,co2\nPlant A,120\nPlant B,340");

    let json = space.run_text(&["sql", "scratch.sqlnow", &query, "-f", "json"]);
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("json is an array");
    assert_eq!(parsed.as_array().expect("an array").len(), 2);
    assert_eq!(parsed[0]["name"], "Plant A");

    let jsonl = space.run_text(&["sql", "scratch.sqlnow", &query, "-f", "jsonl"]);
    assert_eq!(jsonl.trim().lines().count(), 2);

    // and a limit cuts the rows, not just the display. Read from stdout alone:
    // the note about stopping early belongs on stderr, and is checked in
    // the_cli_says_when_it_stopped_early
    let limited = space.run(&["sql", "scratch.sqlnow", &query, "-f", "csv", "--limit", "1"]);
    assert_eq!(String::from_utf8_lossy(&limited.stdout).trim(), "name,co2\nPlant A,120");
}

#[test]
fn container_columns_survive_every_route_out() {
    let space = Workspace::new("containers");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);
    let sql = "SELECT i, [i, i * 10] AS lst, {'a': i} AS st FROM range(1, 4) t(i) ORDER BY i";

    // the viewer: a list used to arrive as the whole column pasted into every
    // row, and a struct as an empty body from a panicked handler
    let rows = server.query(sql)["table_data"]["rows"].clone();
    let lists: Vec<&str> =
        rows.as_array().unwrap().iter().map(|row| row[1].as_str().unwrap()).collect();
    assert_eq!(lists, ["[1, 10]", "[2, 20]", "[3, 30]"]);
    assert_eq!(rows[0][2], "{a: 1}");

    // and the downloads, which share the same stringification
    assert_eq!(
        server.export(sql, "csv"),
        "i,lst,st\n1,\"[1, 10]\",{a: 1}\n2,\"[2, 20]\",{a: 2}\n3,\"[3, 30]\",{a: 3}\n"
    );
    let jsonl = server.export(sql, "jsonl");
    let first: serde_json::Value = serde_json::from_str(jsonl.lines().next().unwrap()).unwrap();
    assert_eq!(first["lst"], "[1, 10]");

    // the CLI path too, which stringifies the same way
    space.exec(&space.path().join("scratch.sqlnow"), "SELECT 1");
    let text = space.run_text(&["sql", "scratch.sqlnow", sql, "-f", "csv"]);
    assert!(text.contains("\"[2, 20]\""), "{}", text);
}

#[test]
fn sql_can_start_with_a_comment() {
    let space = Workspace::new("leading-comment");
    space.exec(&space.path().join("scratch.sqlnow"), "SELECT 1");

    // clap would read this as a flag without allow_hyphen_values, and an agent
    // pasting a commented query has no reason to expect a `--` separator
    for command in ["sql", "exec"] {
        let out = space.run(&[command, "scratch.sqlnow", "-- what this does\nSELECT 42 AS answer"]);
        assert!(out.status.success(), "{}: {}", command, String::from_utf8_lossy(&out.stderr));
        assert!(String::from_utf8_lossy(&out.stdout).contains("42"));
    }

    // and a flag after the sql is still a flag
    let out = space.run_text(&["sql", "scratch.sqlnow", "SELECT 42 AS answer", "-f", "csv"]);
    assert_eq!(out.trim(), "answer\n42");
}

#[test]
fn an_export_that_cannot_run_says_so() {
    let space = Workspace::new("export-errors");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);

    // this panicked the worker: the client got a closed connection with no
    // status at all, and the only trace was a backtrace in the server log
    let (status, body) = server.export_status("SELECT * FROM nope", "csv");
    assert_eq!(status, 400, "body was {:?}", body);
    assert!(body.contains("nope"), "the error should name what was missing: {}", body);

    // the server is unharmed and the next export works
    assert_eq!(server.export("SELECT name FROM plants ORDER BY name", "csv"), "name\nPlant A\nPlant B\n");
}

#[test]
fn jsonl_keys_come_out_in_column_order() {
    let space = Workspace::new("jsonl-order");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);

    // a hash map gave a different order per row, which makes the output
    // needlessly unstable for anything reading it line by line
    let jsonl = server.export("SELECT 1 AS zebra, 2 AS apple, 3 AS mango FROM range(2)", "jsonl");
    for line in jsonl.lines() {
        assert_eq!(line, r#"{"zebra":"1","apple":"2","mango":"3"}"#);
    }
}

#[test]
fn a_result_says_whether_it_is_the_whole_answer() {
    let space = Workspace::new("truncation");
    let server = space.start(&[&space.csv("plants.csv").to_string_lossy()]);
    let sql = "SELECT i FROM range(1, 100) t(i)";

    // the viewer's route: 500 rows of 500 and 500 rows of a million used to
    // look identical, which is the difference between an answer and a guess
    let cut = server.query_with_limit(sql, 2);
    assert_eq!(cut["limit"], 2);
    assert_eq!(cut["table_data"]["truncated"], true);
    assert_eq!(cut["table_data"]["rows"].as_array().unwrap().len(), 2);

    let whole = server.query_with_limit("SELECT i FROM range(1, 3) t(i)", 500);
    assert_eq!(whole["table_data"]["truncated"], false);

    // an export with a limit is buffered so it can say the same thing in
    // headers, leaving the body clean for a parser
    let (body, rows, truncated) = server.export_limited(sql, "csv", 3);
    assert_eq!(body, "i\n1\n2\n3\n");
    assert_eq!((rows.as_str(), truncated.as_str()), ("3", "true"));

    let (body, rows, truncated) = server.export_limited("SELECT i FROM range(1, 3) t(i)", "csv", 50);
    assert_eq!(body, "i\n1\n2\n");
    assert_eq!((rows.as_str(), truncated.as_str()), ("2", "false"));

    // jsonl is limited the same way, and a nonsense limit is refused rather
    // than quietly ignored
    let (body, _, truncated) = server.export_limited(sql, "jsonl", 2);
    assert_eq!(body.lines().count(), 2);
    assert_eq!(truncated, "true");
    let (status, message) =
        server.export_form_status(&[("sql", sql), ("csv", "1"), ("limit", "lots")]);
    assert_eq!(status, 400);
    assert!(message.contains("whole number"), "{}", message);

    // and without a limit it still streams everything, headers and all
    assert_eq!(server.export(sql, "csv").lines().count(), 100);
}

#[test]
fn the_cli_says_when_it_stopped_early() {
    let space = Workspace::new("cli-truncation");
    space.exec(&space.path().join("scratch.sqlnow"), "SELECT 1");
    let sql = "SELECT i FROM range(1, 100) t(i)";

    // the box table has a footer to put it in
    let boxed = space.run_text(&["sql", "scratch.sqlnow", sql, "--limit", "2"]);
    assert!(boxed.contains("(2 rows, truncated"), "{}", boxed);

    // csv is meant to be parsed, so the note goes to stderr and stdout stays
    // exactly the data
    let out = space.run(&["sql", "scratch.sqlnow", sql, "--limit", "2", "-f", "csv"]);
    assert_eq!(String::from_utf8_lossy(&out.stdout), "i\n1\n2\n");
    assert!(String::from_utf8_lossy(&out.stderr).contains("stopped at 2 rows"));

    // nothing is said when the limit was not reached
    let out = space.run(&["sql", "scratch.sqlnow", "SELECT 1 AS i", "--limit", "9", "-f", "csv"]);
    assert_eq!(String::from_utf8_lossy(&out.stderr), "");
}
