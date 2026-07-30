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

#[test]
fn a_server_stops_when_the_shell_that_started_it_is_killed() {
    let space = Workspace::new("parent-death");
    let csv = space.csv("plants.csv");

    // stand in for the desktop shell: a process the server is told to watch
    let mut shell = std::process::Command::new("sleep")
        .arg("300")
        .spawn()
        .expect("spawning a stand-in parent");
    let shell_pid = shell.id().to_string();

    let server = space.start_with_env(
        &[("SQLNOW_PARENT_PID", &shell_pid)],
        &[&csv.to_string_lossy()],
    );
    assert_eq!(server.tables(), ["plants"]);

    // killed outright, the way a crashing shell goes: nothing gets to run a
    // cleanup handler, so the server has to notice by itself. Otherwise it
    // keeps listening, keeps the session open, and the app refuses to reopen it.
    shell.kill().expect("killing the stand-in parent");
    let _ = shell.wait();

    let stopped = server.wait_for_exit(std::time::Duration::from_secs(20));
    assert!(stopped.is_some(), "the server outlived the process it was watching");
}

#[test]
fn a_stop_that_arrives_immediately_still_closes_the_session() {
    let space = Workspace::new("prompt-stop");
    let csv = space.csv("plants.csv");

    // stopped the instant it says it is listening, which is what a shell does
    // when its window is closed straight away — and what the harness does every
    // time. Actix registers its own handlers only when the server future is
    // first polled, so a stop landing before that killed the process outright
    // and none of the closing bookkeeping ran.
    space.start(&[&csv.to_string_lossy()]).stop();

    let url = space.exec_value(&space.store(), "SELECT coalesce(url, 'CLEARED') FROM sessions");
    assert_eq!(url, "CLEARED", "the address was left behind, so the session still looks live");

    // and the run is recorded as used, which is the other half of closing
    let age = space.exec_value(&space.store(), "SELECT epoch(now()::TIMESTAMP - last_used)::BIGINT FROM sessions");
    assert!(age.parse::<i64>().unwrap() < 30, "last_used was not touched: {}", age);
}

#[test]
fn a_session_too_busy_to_answer_is_still_treated_as_open() {
    let space = Workspace::new("busy-ping");
    let csv = space.csv("plants.csv");
    space.start(&[&csv.to_string_lossy(), "-q", "a=SELECT 1"]).stop();

    // Something listening that never replies, standing in for a server whose
    // single worker is inside a long query. A ping cannot tell that apart from
    // a hang, so it must not conclude the session is closed: withdrawing the
    // address of a session someone is using would let a second server open it.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a silent listener");
    let address = listener.local_addr().unwrap();
    let id = space.exec_value(&space.store(), "SELECT id FROM sessions");
    space.exec(
        &space.store(),
        &format!("UPDATE sessions SET url = 'http://{}' WHERE id = '{}'", address, id),
    );

    let listed = space.run_text(&["--resume"]);
    assert!(listed.contains("is open at"), "a busy session was reported as closed:\n{}", listed);

    // and the address is left alone rather than cleared
    let kept = space.exec_value(&space.store(), "SELECT coalesce(url, 'CLEARED') FROM sessions");
    assert_eq!(kept, format!("http://{}", address));

    // a launch onto it is still refused, which is the point of all this
    let refused = space.run(&[&csv.to_string_lossy(), "--port", "0"]);
    assert!(!refused.status.success(), "a second server opened a session that looked busy");
    drop(listener);
}

#[test]
fn a_watching_page_does_not_hold_the_server_open() {
    let space = Workspace::new("shutdown");
    let csv = space.csv("plants.csv");
    let server = space.start(&[&csv.to_string_lossy()]);

    // the UI keeps this open for as long as it is on screen, and a graceful
    // shutdown waits for in-flight requests — which is how closing a window
    // used to leave its server running for half a minute, still answering
    // pings, so the session could not be reopened
    let watching = server.watch_changes(std::time::Duration::from_secs(10));
    std::thread::sleep(std::time::Duration::from_millis(500));

    let stopped = server.wait_for_stop(std::time::Duration::from_secs(15));
    let took = stopped.expect("the server was still running 15s after being asked to stop");
    assert!(
        took < std::time::Duration::from_secs(8),
        "shutdown waited {:?} for a watching page",
        took
    );
    eprintln!("shutdown with a page watching took {:?}", took);
    let _ = watching.join();
}

/// A store that cannot be written must cost only the listing, never the run.
/// This is an agent inside a sandbox that allows writes nowhere but the
/// working directory: the launch used to print two alarming notes (anchored)
/// or fail outright (unanchored), either of which taught the agent to escape
/// the sandbox.
#[cfg(unix)]
#[test]
fn an_unwritable_store_does_not_stop_an_anchored_run() {
    use std::os::unix::fs::PermissionsExt;
    let space = Workspace::new("unwritable-anchored");
    let csv = space.csv("plants.csv");
    let config = space.path().join("config/sqlnow");
    std::fs::create_dir_all(&config).expect("config dir");
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o555))
        .expect("making the store unwritable");

    let server = space.start(&["--db", "plants.duckdb", "-v", &csv.to_string_lossy()]);
    // one calm note, saying where the session actually is
    let printed = server.wait_for_output("not writable from here");
    assert!(printed.contains("The session itself is unaffected"), "{}", printed);
    assert_eq!(server.tables(), ["plants"]);
    server.stop();

    // it lost nothing: everything is in the sidecar next to the database
    assert!(space.path().join("plants.duckdb.sqlnow").exists());

    // so the scratch directory can be removed again
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o755))
        .expect("restoring permissions");
}

/// The same, for a run kept in the store itself: rather than refusing to
/// start, it runs with a session that lasts only for the run, and says so.
#[cfg(unix)]
#[test]
fn an_unwritable_store_does_not_stop_an_unanchored_run() {
    use std::os::unix::fs::PermissionsExt;
    let space = Workspace::new("unwritable-unanchored");
    let csv = space.csv("plants.csv");
    let config = space.path().join("config/sqlnow");
    std::fs::create_dir_all(&config).expect("config dir");
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o555))
        .expect("making the store unwritable");

    let server = space.start(&[&csv.to_string_lossy(), "-q", "kept=SELECT * FROM plants"]);
    let printed = server.wait_for_output("session not persisted");
    assert!(printed.contains("not writable from here"), "{}", printed);
    // and the run itself works: data attached, query saved for the run
    assert_eq!(server.tables(), ["plants"]);
    assert_eq!(server.query_names(), ["kept"]);
    server.stop();

    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o755))
        .expect("restoring permissions");
}
