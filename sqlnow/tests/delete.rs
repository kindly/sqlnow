//! `sqlnow delete`: the one thing that removes work, so it is checked from
//! both sides — that it takes everything belonging to that session, and that
//! it takes nothing else.

mod harness;
use harness::Workspace;

#[test]
fn deleting_a_session_takes_its_queries_history_and_inputs() {
    let space = Workspace::new("delete-stored");
    let one = space.csv("one.csv").to_string_lossy().to_string();
    let two = space.write("two.csv", "name,mw\nUnit 1,50\n").to_string_lossy().to_string();

    let doomed = space.start(&[&one, "-q", "a=SELECT 1"]);
    doomed.query("SELECT count(*) FROM plants");
    doomed.stop();
    let kept = space.start(&[&two, "-q", "b=SELECT 2"]);
    kept.query("SELECT 2");
    kept.stop();

    let doomed_id = space.exec_value(
        &space.store(),
        "SELECT id FROM sessions WHERE id IN (SELECT session FROM queries WHERE name = 'a')",
    );

    // position 2: the older of the two, which is the one with query 'a'
    let out = space.run_text(&["delete", "2", "--yes"]);
    assert!(out.contains("Deleted session"), "{}", out);
    assert!(out.contains("1 query"), "it should say what it took: {}", out);
    assert!(out.contains("1 history entry"), "{}", out);
    assert!(out.contains("one.csv"), "it should name the session: {}", out);

    // every table is keyed by session, and none of them still mentions it
    for table in ["sessions", "queries", "history", "inputs", "meta"] {
        let column = if table == "sessions" { "id" } else { "session" };
        let left = space.exec_value(
            &space.store(),
            &format!("SELECT count(*) FROM {} WHERE {} = '{}'", table, column, doomed_id),
        );
        assert_eq!(left, "0", "{} still holds rows for the deleted session", table);
    }

    // and the other session is exactly as it was
    let listed = space.run_text(&["--resume"]);
    assert!(!listed.contains("one.csv"), "the deleted session is still listed: {}", listed);
    assert!(listed.contains("two.csv"), "{}", listed);
    let survivor = space.start(&[&two]);
    assert_eq!(survivor.query_names(), ["b"]);
}

#[test]
fn a_session_in_a_file_of_its_own_goes_from_both_places() {
    let space = Workspace::new("delete-file");
    let csv = space.csv("plants.csv");
    space.run(&["exec", "analysis.sqlnow", "SELECT 1"]);
    space
        .start(&["analysis.sqlnow", &csv.to_string_lossy(), "-q", "mine=SELECT 1"])
        .stop();

    let file = space.path().join("analysis.sqlnow");
    let id = space.exec_value(&file, "SELECT id FROM sessions");

    let out = space.run_text(&["delete", "1", "--yes"]);
    assert!(out.contains("Deleted session"), "{}", out);
    assert!(out.contains("analysis.sqlnow"), "{}", out);
    assert!(out.contains("is left in place"), "it should say the file stays: {}", out);

    // the file is the user's, so it stays — but the session inside it is gone,
    // and so is the store's pointer to it. Checked by id rather than by
    // counting, because `exec` on a session file with no sessions left seeds a
    // fresh one, which is what lets an agent prepare a session before a run.
    assert!(file.exists(), "it deleted a file it was only asked to empty");
    for (table, column) in [("sessions", "id"), ("queries", "session"), ("inputs", "session")] {
        let left = space.exec_value(
            &file,
            &format!("SELECT count(*) FROM {} WHERE {} = '{}'", table, column, id),
        );
        assert_eq!(left, "0", "{} in the file still holds the deleted session", table);
    }
    let pointer = space.exec_value(
        &space.store(),
        &format!("SELECT count(*) FROM sessions WHERE id = '{}'", id),
    );
    assert_eq!(pointer, "0", "the store still points at the deleted session");
    assert!(!space.run_text(&["--resume"]).contains("analysis.sqlnow"));
}

#[test]
fn a_session_someone_is_using_is_not_deleted() {
    let space = Workspace::new("delete-live");
    let csv = space.csv("plants.csv");
    let running = space.start(&[&csv.to_string_lossy(), "-q", "a=SELECT 1"]);

    let out = space.run(&["delete", "1", "--yes"]);
    assert!(!out.status.success(), "it deleted a session that was open");
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("close it before deleting it"), "{}", text);
    assert!(text.contains(running.url()), "it should say where: {}", text);

    // still all there, and still being served
    assert_eq!(running.query_names(), ["a"]);
    assert_eq!(space.exec_value(&space.store(), "SELECT count(*) FROM sessions"), "1");
}

#[test]
fn deleting_needs_confirming() {
    let space = Workspace::new("delete-confirm");
    let csv = space.csv("plants.csv");
    space.start(&[&csv.to_string_lossy(), "-q", "a=SELECT 1"]).stop();

    // a test has no terminal, which is the same position a script or an agent
    // is in: there is nobody to ask, so it has to be told
    let out = space.run(&["delete", "1"]);
    assert!(!out.status.success(), "it deleted without being confirmed");
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("--yes"), "{}", text);
    assert!(text.contains("no terminal"), "{}", text);
    assert_eq!(space.exec_value(&space.store(), "SELECT count(*) FROM sessions"), "1");
}

#[test]
fn several_can_go_at_once_and_a_bad_one_stops_all_of_them() {
    let space = Workspace::new("delete-many");
    let one = space.csv("one.csv").to_string_lossy().to_string();
    let two = space.write("two.csv", "name,mw\nUnit 1,50\n").to_string_lossy().to_string();
    let three = space.write("three.csv", "name,t\nT,1\n").to_string_lossy().to_string();
    space.start(&[&one, "-q", "a=SELECT 1"]).stop();
    space.start(&[&two, "-q", "b=SELECT 2"]).stop();
    space.start(&[&three, "-q", "c=SELECT 3"]).stop();

    // one of the values is nonsense: nothing should go, because positions are
    // read from the listing and a partial run would shift the rest
    let out = space.run(&["delete", "1", "99", "--yes"]);
    assert!(!out.status.success());
    assert_eq!(space.exec_value(&space.store(), "SELECT count(*) FROM sessions"), "3");

    // two at once, one by position and one by id, and the id may be short
    let oldest = space.exec_value(&space.store(), "SELECT id FROM sessions ORDER BY last_used LIMIT 1");
    let out = space.run_text(&["delete", "1", &oldest[..6], "--yes"]);
    assert_eq!(out.matches("Deleted session").count(), 2, "{}", out);
    assert_eq!(space.exec_value(&space.store(), "SELECT count(*) FROM sessions"), "1");

    // the same session twice over is not an error, and deletes once
    let last = space.exec_value(&space.store(), "SELECT id FROM sessions");
    let out = space.run_text(&["delete", "1", &last, "--yes"]);
    assert_eq!(out.matches("Deleted session").count(), 1, "{}", out);
    assert!(space.run_text(&["--resume"]).contains("No stored sessions"));
}
