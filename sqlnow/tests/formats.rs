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

    // and a limit cuts the rows, not just the display
    let limited = space.run_text(&["sql", "scratch.sqlnow", &query, "-f", "csv", "--limit", "1"]);
    assert_eq!(limited.trim(), "name,co2\nPlant A,120");
}
