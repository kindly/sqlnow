//! Where a session lives, how it comes back, and what happens when two runs
//! want the same one. Every bug this file covers was found by hand first.

mod harness;
use harness::Workspace;

#[test]
fn the_same_inputs_come_back_to_the_same_session() {
    let space = Workspace::new("same-inputs");
    let csv = space.csv("plants.csv");
    let arg = csv.to_string_lossy().to_string();

    let first = space.start(&[&arg, "-q", "kept=SELECT * FROM plants"]);
    assert_eq!(first.query_names(), ["kept"]);
    assert!(first.printed().contains("note: session"), "{}", first.printed());
    first.stop();

    // no -q this time: the query has to come from the session, not the command
    let again = space.start(&[&arg]);
    assert_eq!(again.query_names(), ["kept"]);
    assert!(again.printed().contains("resumed session"), "{}", again.printed());
}

#[test]
fn different_inputs_get_different_sessions_and_order_does_not_matter() {
    let space = Workspace::new("keys");
    let one = space.csv("one.csv").to_string_lossy().to_string();
    let two = space.write("two.csv", "name,mw\nUnit 1,50\n").to_string_lossy().to_string();

    space.start(&[&one, "-q", "a=SELECT 1"]).stop();
    space.start(&[&two, "-q", "b=SELECT 2"]).stop();
    space.start(&[&one, &two, "-q", "c=SELECT 3"]).stop();

    let ids = space.exec(&space.store(), "SELECT count(*) FROM sessions");
    assert_eq!(ids.lines().last().unwrap(), "3", "one session per set of inputs");

    // the same two inputs the other way round is the same session, not a fourth
    let swapped = space.start(&[&two, &one]);
    assert_eq!(swapped.query_names(), ["c"]);
    swapped.stop();
    let ids = space.exec(&space.store(), "SELECT count(*) FROM sessions");
    assert_eq!(ids.lines().last().unwrap(), "3", "argument order changed the key");
}

#[test]
fn a_database_session_resumes_with_its_data() {
    let space = Workspace::new("database");
    let csv = space.csv("plants.csv");

    let first = space.start(&[
        "plants.duckdb",
        "-v",
        &csv.to_string_lossy(),
        "-q",
        "kept=SELECT * FROM plants",
    ]);
    assert_eq!(first.tables(), ["plants"]);
    first.stop();

    // the sidecar sits next to the database, and the store points at the
    // database — not at the sidecar, which on its own has no tables
    assert!(space.path().join("plants.duckdb.sqlnow").exists());
    let recorded = space.exec_value(&space.store(), "SELECT path FROM sessions");
    assert!(recorded.ends_with("plants.duckdb"), "recorded {}", recorded);

    // resuming has to bring back the queries *and* the tables
    let resumed = space.start(&["--resume", "1"]);
    assert_eq!(resumed.query_names(), ["kept"]);
    assert_eq!(resumed.tables(), ["plants"], "resumed without its data");
}

#[test]
fn a_named_session_file_can_be_carried_around() {
    let space = Workspace::new("named");
    let csv = space.csv("plants.csv");
    space.run(&["exec", "analysis.sqlnow", "SELECT 1"]);

    let first = space.start(&[
        "analysis.sqlnow",
        &csv.to_string_lossy(),
        "-q",
        "mine=SELECT count(*) FROM plants",
    ]);
    assert_eq!(first.query_names(), ["mine"]);
    first.stop();

    // it is listed by its own path, and reopening it replays the input
    let listed = space.run_text(&["--resume"]);
    assert!(listed.contains("analysis.sqlnow"), "{}", listed);
    let again = space.start(&["analysis.sqlnow"]);
    assert_eq!(again.tables(), ["plants"]);
    assert_eq!(again.query_names(), ["mine"]);
}

#[test]
fn an_input_that_has_gone_is_an_error_not_a_missing_table() {
    let space = Workspace::new("missing-input");
    let csv = space.csv("plants.csv");
    let arg = csv.to_string_lossy().to_string();
    space.start(&[&arg, "-q", "kept=SELECT 1"]).stop();

    // something else recorded an input that is not there
    let id = space.exec_value(&space.store(), "SELECT id FROM sessions");
    space.exec(
        &space.store(),
        &format!(
            "INSERT INTO inputs(session, kind, name, uri, tables, except_tables)
             VALUES ('{}', 'view', 'ghost', '{}/gone.csv', [], [])",
            id,
            space.path().display()
        ),
    );

    let output = space.run(&[&arg, "--port", "0"]);
    assert!(!output.status.success(), "a session with a missing input started anyway");
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(text.contains("gone.csv"), "{}", text);
    assert!(text.contains("ghost"), "the error should name the input: {}", text);
    assert!(text.contains("DELETE FROM inputs"), "and offer the way out: {}", text);
}

#[test]
fn a_live_session_cannot_be_opened_twice() {
    let space = Workspace::new("live");
    let csv = space.csv("plants.csv");
    let arg = csv.to_string_lossy().to_string();

    let running = space.start(&[&arg, "-q", "a=SELECT 1"]);
    let id = space.exec_value(&space.store(), "SELECT id FROM sessions");

    // it is listed as running, with the address it is on
    assert_eq!(space.live_ids(), [id[..8].to_string()]);
    assert!(space.run_text(&["--resume"]).contains(running.url()));

    // and a second run is refused, pointing at the first
    let refused = space.run(&[&arg, "--port", "0"]);
    assert!(!refused.status.success());
    let text = String::from_utf8_lossy(&refused.stderr);
    assert!(text.contains("already open at"), "{}", text);
    assert!(text.contains(running.url()), "{}", text);

    // closing it cleanly withdraws the address
    running.stop();
    assert!(space.live_ids().is_empty(), "still live after a clean stop");
    space.start(&[&arg]).stop();
}

#[test]
fn a_killed_server_blocks_nothing() {
    let space = Workspace::new("killed");
    let csv = space.csv("plants.csv");
    let arg = csv.to_string_lossy().to_string();

    let doomed = space.start(&[&arg, "-q", "a=SELECT 1"]);
    let url = doomed.url().to_string();
    doomed.kill();

    // the address it published is still in the store
    let stale = space.exec_value(&space.store(), "SELECT url FROM sessions WHERE url IS NOT NULL");
    assert!(stale.contains(url.trim_start_matches("http://")), "stale url: {}", stale);

    // but nothing answers there, so the listing clears it and a new run starts
    assert!(space.live_ids().is_empty(), "a killed server was reported as running");
    let after = space.start(&[&arg]);
    assert_eq!(after.query_names(), ["a"]);
}

#[test]
fn no_register_keeps_a_run_out_of_the_list() {
    let space = Workspace::new("no-register");
    let csv = space.csv("plants.csv");

    let quiet = space.start(&[
        "private.duckdb",
        "-v",
        &csv.to_string_lossy(),
        "-q",
        "hidden=SELECT 1",
        "--no-register",
    ]);
    assert_eq!(quiet.query_names(), ["hidden"], "the session itself still works");

    let listed = space.run_text(&["--resume"]);
    assert!(!listed.contains("private.duckdb"), "{}", listed);
    quiet.stop();

    // its session is still there in its own file, just not advertised
    let kept = space.exec_value(
        &space.path().join("private.duckdb.sqlnow"),
        "SELECT name FROM queries",
    );
    assert_eq!(kept, "hidden");
}

#[test]
fn resume_reports_what_it_cannot_do() {
    let space = Workspace::new("resume-errors");
    let empty = space.run_text(&["--resume"]);
    assert!(empty.contains("No stored sessions"), "{}", empty);

    let csv = space.csv("plants.csv");
    space.start(&[&csv.to_string_lossy(), "-q", "a=SELECT 1"]).stop();

    for (argument, expected) in
        [("99", "no session is at that position"), ("0", "counts from 1"), ("zzzz", "no session")]
    {
        let text = space.run_text(&["--resume", argument, "--port", "0"]);
        assert!(text.contains(expected), "--resume {} said: {}", argument, text);
    }
}

#[test]
fn closing_a_session_puts_it_back_at_the_top_of_the_list() {
    let space = Workspace::new("last-used");
    let one = space.csv("one.csv").to_string_lossy().to_string();
    let two = space.write("two.csv", "name,mw\nUnit 1,50\n").to_string_lossy().to_string();

    // open the older one first and leave it running, so the only thing that can
    // reorder the list is which of them was closed last
    let first = space.start(&[&one, "-q", "a=SELECT 1"]);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    space.start(&[&two, "-q", "b=SELECT 2"]).stop();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    first.stop();

    // a session you just closed is the one you are most likely to want back
    let listed = space.run_text(&["--resume"]);
    let order: Vec<&str> = listed
        .lines()
        .filter(|line| line.contains("one.csv") || line.contains("two.csv"))
        .collect();
    assert_eq!(order.len(), 2, "expected both sessions listed:\n{}", listed);
    assert!(order[0].contains("one.csv"), "listed in the wrong order:\n{}", listed);

    // and --resume 1 opens that one
    let resumed = space.start(&["--resume", "1"]);
    assert_eq!(resumed.query_names(), ["a"]);
}
