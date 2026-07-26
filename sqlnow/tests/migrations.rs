//! Old session files still open.
//!
//! The library has unit tests for each conversion; these check the binary
//! actually runs them on the way in, on a file it did not write itself, and
//! serves the session that comes out.

mod harness;
use harness::Workspace;

#[test]
fn a_format_one_session_file_is_upgraded_on_first_use() {
    let space = Workspace::new("format-1");
    let csv = space.csv("plants.csv");
    let session = space.path().join("old.sqlnow");

    // format 1 had no version marker and no session column: one file, one
    // session, with the id kept in meta. Built by hand here because no build
    // that can write it exists any more.
    space.exec(
        &session,
        "DROP TABLE meta; DROP TABLE queries; DROP TABLE history; DROP TABLE inputs;
         DROP TABLE sessions; DROP TABLE format;
         CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
         CREATE TABLE queries(pos INTEGER NOT NULL, name TEXT PRIMARY KEY, sql TEXT NOT NULL);
         CREATE TABLE history(\"at\" TIMESTAMP NOT NULL DEFAULT now(), sql TEXT NOT NULL);
         CREATE TABLE inputs(kind TEXT NOT NULL, name TEXT NOT NULL, uri TEXT NOT NULL,
                             tables TEXT[], except_tables TEXT[]);
         INSERT INTO meta VALUES ('id', 'aaaabbbbccccdddd'), ('open', 'old query');
         INSERT INTO queries VALUES (1, 'old query', 'SELECT * FROM plants');
         INSERT INTO history(sql) VALUES ('SELECT 1');",
    );

    let server = space.start(&["old.sqlnow", &csv.to_string_lossy()]);
    server.wait_for_output("Upgraded session file");

    // everything the old file held is still there, under the id it had
    assert_eq!(server.query_names(), ["old query"]);
    assert_eq!(server.get("/api/queries")["open"], "old query");
    assert_eq!(server.get("/api/session")["id"], "aaaabbbbccccdddd");
    server.stop();

    // and it is a current file now, not converted again on every open
    assert_eq!(space.exec_value(&session, "SELECT max(version) FROM format"), "3");
    let again = space.start(&["old.sqlnow", &csv.to_string_lossy()]);
    assert!(!again.printed().contains("Upgraded"), "{}", again.printed());
}

#[test]
fn a_hand_written_sidecar_still_opens() {
    let space = Workspace::new("legacy-lines");
    space.csv("plants.csv");
    // the original sidecar format: a text file, with paths relative to it
    space.write("notes.sqlnow", "# old format\nid aaaabbbbcccc0001\nview plants.csv\n");

    let server = space.start(&["notes.sqlnow"]);
    server.wait_for_output("Upgraded legacy session file");
    assert_eq!(server.tables(), ["plants"], "the recorded input was not replayed");
    assert_eq!(server.get("/api/session")["id"], "aaaabbbbcccc0001");
}
