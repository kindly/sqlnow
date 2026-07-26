//! The desktop shell gets far enough to serve.
//!
//! This exists because attaching a sqlite database used to kill it outright: our
//! statically linked sqlite was exported into the dynamic symbol table and the
//! duckdb extension's own calls bound to it instead of its own copy. Nothing in
//! the library could catch that — it is a property of how this binary is linked
//! — so the only test that would have is one that runs the binary.
//!
//! No display is needed, which is the useful part: the crash happened while the
//! catalog was first read, before the window was created, so waiting for the
//! address is enough.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

const DESKTOP: &str = env!("CARGO_BIN_EXE_sqlnow-desktop");

#[test]
fn it_serves_a_session_with_a_sqlite_database_attached() {
    let dir = std::env::temp_dir().join(format!("sqlnow-desktop-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch directory");

    std::fs::write(dir.join("plants.csv"), "name,co2\nPlant A,120\n").expect("fixture");
    let sqlite = dir.join("legacy.sqlite");
    {
        let db = rusqlite::Connection::open(&sqlite).expect("sqlite fixture");
        db.execute_batch("CREATE TABLE units(name TEXT, mw INT); INSERT INTO units VALUES ('U1', 50);")
            .expect("sqlite fixture");
    }

    let mut child = Command::new(DESKTOP)
        .current_dir(&dir)
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .arg("plants.csv")
        .args(["-v", "legacy.sqlite", "--as", "legacy"])
        .args(["--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning sqlnow-desktop");

    let stdout = child.stdout.take().expect("piped stdout");
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(url) = line.strip_prefix("Server running on ") {
                let _ = sender.send(url.trim().to_string());
                return;
            }
        }
    });

    let ready = receiver.recv_timeout(Duration::from_secs(60));
    let outcome = child.try_wait().expect("checking on the child");
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);

    // a signal here means it died rather than exited, which is the symptom the
    // symbol clash produced
    assert!(
        outcome.is_none(),
        "the desktop shell exited during startup: {:?}",
        outcome
    );
    let url = ready.expect("the desktop shell never reported an address");
    assert!(url.starts_with("http://"), "reported {:?}", url);
}

/// The published binary keeps its inspector.
///
/// tauri and wry both gate the inspector on `debug_assertions` *or* the
/// `devtools` feature, and on GTK that cfg is what decides whether webkit's
/// developer extras are switched on at all. Without the feature a debug build
/// still has devtools and a release build silently has none — which is exactly
/// how the shipped 0.4.1 came to have no way to inspect anything.
///
/// So the invariant lives in Cargo.toml, and that is what this reads: a test
/// running under `cargo test` is a debug build, where the working inspector
/// proves nothing about the release one.
#[test]
fn the_devtools_feature_is_kept_for_release_builds() {
    let manifest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )
    .expect("our own Cargo.toml");
    let tauri = manifest
        .lines()
        .find(|line| line.starts_with("tauri = "))
        .expect("a tauri dependency line");
    assert!(
        tauri.contains("\"devtools\""),
        "the tauri dependency dropped the devtools feature, so a release build \
         would ship without an inspector: {}",
        tauri
    );
}
