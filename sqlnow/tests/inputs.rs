//! What gets attached, and which of its tables are visible.

mod harness;
use harness::Workspace;

#[test]
fn only_and_except_choose_tables_from_a_database() {
    let space = Workspace::new("filters");
    let db = space.sqlite("legacy.sqlite", &["units", "owners", "audit_log", "entity_one"]);
    let uri = db.to_string_lossy().to_string();

    // --only is an anchored regex, so a plain name matches exactly
    let picked = space.start(&["-v", &uri, "--as", "legacy", "--only", "units", "--only", "entity_.*"]);
    // an attached database's tables are listed under its name
    assert_eq!(picked.tables(), ["legacy.entity_one", "legacy.units"]);
    picked.stop();

    // --except is applied after, and both are bound to the input before them
    let space = Workspace::new("filters-except");
    let db = space.sqlite("legacy.sqlite", &["units", "owners", "audit_log"]);
    let uri = db.to_string_lossy().to_string();
    let trimmed = space.start(&["-v", &uri, "--as", "legacy", "--except", "audit_log"]);
    assert_eq!(trimmed.tables(), ["legacy.owners", "legacy.units"]);

    // the filter is recorded with the input, so a resumed session keeps it
    trimmed.stop();
    let again = space.start(&["-v", &uri, "--as", "legacy", "--except", "audit_log"]);
    assert_eq!(again.tables(), ["legacy.owners", "legacy.units"]);
}

#[test]
fn json_files_can_be_loaded_as_tables() {
    let space = Workspace::new("json-inputs");
    let object = space.write("plants.json", r#"[{"name": "Plant A", "co2": 120}]"#);
    let lines = space.write("units.jsonl", "{\"name\": \"Unit 1\", \"mw\": 50}\n{\"name\": \"Unit 2\", \"mw\": 70}\n");

    // -t materialises them rather than defining a view, so the reader below is
    // querying stored data, not the files
    let server = space.start(&[
        "loaded.duckdb",
        "-t",
        &object.to_string_lossy(),
        "-t",
        &lines.to_string_lossy(),
    ]);
    assert_eq!(server.tables(), ["plants", "units"]);
    // json numbers land as doubles, which is duckdb's inference, not ours
    assert_eq!(server.query("SELECT co2::INT FROM plants")["table_data"]["rows"][0][0], "120");
    assert_eq!(server.query("SELECT count(*) FROM units")["table_data"]["rows"][0][0], "2");
}

#[test]
fn a_json_file_with_multibyte_text_loads() {
    let space = Workspace::new("json-multibyte");
    // the shape of a json file is guessed from its first 10 kB, and that cut
    // used to be decoded strictly: a multibyte character straddling it failed
    // the whole load with "invalid data"
    let padding = "é".repeat(6000);
    let json = format!(
        "[{{\"name\": \"{}\", \"co2\": 120}}, {{\"name\": \"Plant B\", \"co2\": 340}}]",
        padding
    );
    let path = space.write("wide.json", &json);

    let server = space.start(&["loaded.duckdb", "-t", &path.to_string_lossy()]);
    assert_eq!(server.tables(), ["wide"]);
    assert_eq!(server.query("SELECT count(*) FROM wide")["table_data"]["rows"][0][0], "2");
}

#[test]
fn a_url_without_a_name_is_refused_clearly() {
    let space = Workspace::new("unnamed-url");

    // A database url used to reach duckdb with an empty name, which came back
    // as "Parser Error: zero-length delimited identifier" — true, and no help
    // at all. There is no postgres in the test environment, so this checks the
    // naming rather than the connecting: a url with no database in its path
    // cannot be named after one, and has to say so.
    let out = space.run(&["-v", "postgresql://localhost", "--port", "0"]);
    let complaint = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(complaint.contains("needs a name"), "{}", complaint);
    assert!(!complaint.contains("zero-length"), "the parser error leaked out: {}", complaint);
}
