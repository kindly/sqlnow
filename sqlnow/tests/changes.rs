//! Two things that only exist between processes: the change stream that tells
//! a browser to refresh, and a write made by something other than the server.

mod harness;
use harness::Workspace;
use serde_json::json;
use std::time::Duration;

#[test]
fn a_change_is_announced_to_its_own_session_and_no_other() {
    let space = Workspace::new("events");
    let one = space.csv("one.csv").to_string_lossy().to_string();
    let two = space.write("two.csv", "name,mw\nUnit 1,50\n").to_string_lossy().to_string();

    // two sessions in the one store, which is what makes this worth testing:
    // the change stamp has to be per session, not per file
    let first = space.start(&[&one]);
    let second = space.start(&[&two]);

    let watching_first = first.watch_changes(Duration::from_secs(4));
    let watching_second = second.watch_changes(Duration::from_secs(4));
    // let both streams settle before the write, so neither counts a stamp it
    // read on connecting
    std::thread::sleep(Duration::from_millis(600));

    let (status, _) =
        first.post_json("/api/queries", json!({"name": "added", "sql": "SELECT 1"}));
    assert_eq!(status, 201);

    let told = watching_first.join().expect("watcher thread");
    let quiet = watching_second.join().expect("watcher thread");
    assert!(told >= 1, "the session that changed was not told");
    assert_eq!(quiet, 0, "the other session was told about someone else's change");
}

#[test]
fn a_write_from_outside_shows_up_without_a_restart() {
    let space = Workspace::new("external-write");
    let csv = space.csv("plants.csv");
    // a real database, held read-only, so the write below has to come from
    // another process — which is exactly how an agent works alongside a session
    let server = space.start(&["plants.duckdb", "-t", &csv.to_string_lossy()]);
    assert_eq!(server.tables(), ["plants"]);

    let out = space.run(&["sql", "plants.duckdb", "CREATE TABLE units(name TEXT, mw INT)"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    // the running server notices because the file's mtime moved, so it reopens
    // rather than serving the catalog it read at startup
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if server.tables() == ["plants", "units"] {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("the new table never appeared; tables are {:?}", server.tables());
}
