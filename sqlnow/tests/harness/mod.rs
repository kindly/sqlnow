#![allow(dead_code)] // each test binary uses a different part of this
//! Running sqlnow the way a user does, and looking at what it did.
//!
//! Every test here drives the built binary as a child process rather than
//! calling into the library, because most of what is worth checking is only
//! visible from outside: the session store it writes, the lines it prints, the
//! HTTP it serves, and how two of them behave at once. Cargo builds the binary
//! before these tests run and passes its path in, so there is nothing to
//! compile here and nothing to keep in step.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

/// Cargo hands us the binary it just built for this test.
const SQLNOW: &str = env!("CARGO_BIN_EXE_sqlnow");

/// How long to wait for a server to say it is listening.
const STARTUP: Duration = Duration::from_secs(30);

/// A scratch directory that stands in for a user's home.
///
/// Only `XDG_CONFIG_HOME` is redirected, which is where the session store
/// lives. The real `HOME` is left alone deliberately: duckdb keeps its
/// downloaded extensions under it, and a home without them turns a 160ms
/// startup into a ten second one while it fetches six extensions again.
pub struct Workspace {
    dir: PathBuf,
}

impl Workspace {
    pub fn new(label: &str) -> Workspace {
        // the pid keeps concurrent cargo test runs apart, the label keeps the
        // tests within one run apart
        let dir = std::env::temp_dir()
            .join(format!("sqlnow-test-{}-{}", std::process::id(), label));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch directory");
        Workspace { dir }
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Where unanchored sessions are kept for this workspace.
    pub fn store(&self) -> PathBuf {
        self.dir.join("config/sqlnow/sessions.sqlnow")
    }

    /// A fixture written from the test rather than checked in, because the
    /// repository's example data is not tracked.
    pub fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.dir.join(name);
        std::fs::write(&path, contents).expect("writing a fixture");
        path
    }

    pub fn csv(&self, name: &str) -> PathBuf {
        self.write(name, "name,co2\nPlant A,120\nPlant B,340\n")
    }

    /// A sqlite database with several tables, for the table filters.
    pub fn sqlite(&self, name: &str, tables: &[&str]) -> PathBuf {
        let path = self.dir.join(name);
        let db = rusqlite::Connection::open(&path).expect("sqlite fixture");
        for table in tables {
            db.execute_batch(&format!(
                "CREATE TABLE {0}(name TEXT); INSERT INTO {0} VALUES ('a');",
                table
            ))
            .expect("sqlite fixture");
        }
        path
    }

    fn command(&self) -> Command {
        let mut command = Command::new(SQLNOW);
        command.env("XDG_CONFIG_HOME", self.dir.join("config"));
        command.current_dir(&self.dir);
        command
    }

    /// Run sqlnow to completion: `--resume`, `exec`, `sql`, or a launch that is
    /// expected to fail.
    pub fn run(&self, args: &[&str]) -> Output {
        self.command().args(args).output().expect("running sqlnow")
    }

    /// Everything the run printed, both streams, for asserting on messages.
    pub fn run_text(&self, args: &[&str]) -> String {
        let out = self.run(args);
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    }

    /// SQL against a session database, using sqlnow's own runner — the same way
    /// the documentation tells an agent to inspect one.
    pub fn exec(&self, database: &Path, sql: &str) -> String {
        let out = self.run(&["exec", &database.to_string_lossy(), sql]);
        assert!(
            out.status.success(),
            "exec failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// One value out of a session database.
    pub fn exec_value(&self, database: &Path, sql: &str) -> String {
        self.exec(database, sql).lines().last().unwrap_or_default().to_string()
    }

    /// Start a server and wait until it is listening. Panics with whatever it
    /// printed if it exits first, which is what a broken launch looks like.
    pub fn start(&self, args: &[&str]) -> Server {
        self.start_with_env(&[], args)
    }

    /// The same, with extra environment — for the settings only a parent
    /// process would pass.
    pub fn start_with_env(&self, env: &[(&str, &str)], args: &[&str]) -> Server {
        let mut command = self.command();
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command
            .args(args)
            .args(["--port", "0"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawning sqlnow");

        // Both streams are drained by threads for two reasons: a full pipe
        // would block the server, and output keeps coming after it is ready —
        // the deep link is printed *after* the address, so a reader that
        // stopped at readiness would never see it.
        let printed = Arc::new(Mutex::new(String::new()));
        let (sender, receiver) = std::sync::mpsc::channel();

        let stdout = child.stdout.take().expect("piped stdout");
        let collected = printed.clone();
        std::thread::spawn(move || {
            let mut announced = false;
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(url) = line.strip_prefix("Server running on ") {
                    if !announced {
                        announced = true;
                        let _ = sender.send(Some(url.trim().to_string()));
                    }
                }
                let mut buffer = collected.lock().expect("output lock");
                buffer.push_str(&line);
                buffer.push('\n');
            }
            if !announced {
                let _ = sender.send(None);
            }
        });

        let stderr = child.stderr.take().expect("piped stderr");
        let collected = printed.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let mut buffer = collected.lock().expect("output lock");
                buffer.push_str(&line);
                buffer.push('\n');
            }
        });

        match receiver.recv_timeout(STARTUP) {
            Ok(Some(url)) => Server { child, url, printed },
            Ok(None) => {
                let _ = child.wait();
                panic!(
                    "sqlnow exited before listening:\n{}",
                    printed.lock().expect("output lock")
                );
            }
            Err(_) => {
                let _ = child.kill();
                panic!("sqlnow did not start within {:?}", STARTUP);
            }
        }
    }
}

impl Workspace {
    /// The sessions the listing reports as running, by id.
    ///
    /// Read from the lines that name an address rather than by looking for the
    /// word "live" anywhere in the output — which also matches a path, as one
    /// of these tests discovered about its own scratch directory.
    pub fn live_ids(&self) -> Vec<String> {
        self.run_text(&["--resume"])
            .lines()
            .filter_map(|line| {
                let rest = line.strip_prefix("Session ")?;
                let (id, tail) = rest.split_once(' ')?;
                tail.starts_with("is open at").then(|| id.to_string())
            })
            .collect()
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A running server, killed when the test lets go of it.
pub struct Server {
    child: Child,
    url: String,
    printed: Arc<Mutex<String>>,
}

impl Server {
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Everything it has printed so far — the session notes and deep links the
    /// documentation promises.
    pub fn printed(&self) -> String {
        self.printed.lock().expect("output lock").clone()
    }

    /// Wait for a line to appear, for output that follows readiness.
    pub fn wait_for_output(&self, needle: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let seen = self.printed();
            if seen.contains(needle) {
                return seen;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("never printed {:?}; saw:\n{}", needle, self.printed())
    }

    /// The body as it came, for the pages that are not json.
    pub fn get_text(&self, path: &str) -> String {
        ureq::get(&format!("{}{}", self.url, path))
            .call()
            .expect("GET failed")
            .into_string()
            .expect("reading the body")
    }

    pub fn status(&self, path: &str) -> u16 {
        match ureq::get(&format!("{}{}", self.url, path)).call() {
            Ok(response) => response.status(),
            Err(ureq::Error::Status(code, _)) => code,
            Err(e) => panic!("GET failed: {}", e),
        }
    }

    /// An export with a row limit: the body plus the headers describing it.
    pub fn export_limited(&self, sql: &str, format: &str, limit: usize) -> (String, String, String) {
        let response = ureq::post(&format!("{}/outputs", self.url))
            .send_form(&[("sql", sql), (format, "1"), ("limit", &limit.to_string())])
            .expect("limited export failed");
        let rows = response.header("X-Sqlnow-Rows").unwrap_or_default().to_string();
        let truncated = response.header("X-Sqlnow-Truncated").unwrap_or_default().to_string();
        (response.into_string().expect("reading the body"), rows, truncated)
    }

    /// A streaming export that is expected to fail, with the status it gave.
    pub fn export_status(&self, sql: &str, format: &str) -> (u16, String) {
        let response = ureq::post(&format!("{}/outputs", self.url))
            .send_form(&[("sql", sql), (format, "1")]);
        match response {
            Ok(response) => {
                let status = response.status();
                (status, response.into_string().unwrap_or_default())
            }
            Err(ureq::Error::Status(code, response)) => {
                (code, response.into_string().unwrap_or_default())
            }
            Err(e) => panic!("export request failed: {}", e),
        }
    }

    /// The headers of a request that is expected to be refused.
    pub fn export_form_status(&self, fields: &[(&str, &str)]) -> (u16, String) {
        match ureq::post(&format!("{}/outputs", self.url)).send_form(fields) {
            Ok(response) => {
                let status = response.status();
                (status, response.into_string().unwrap_or_default())
            }
            Err(ureq::Error::Status(code, response)) => {
                (code, response.into_string().unwrap_or_default())
            }
            Err(e) => panic!("export request failed: {}", e),
        }
    }

    /// A streaming export, as the download buttons do it.
    pub fn export(&self, sql: &str, format: &str) -> String {
        ureq::post(&format!("{}/outputs", self.url))
            .send_form(&[("sql", sql), (format, "1")])
            .expect("export failed")
            .into_string()
            .expect("reading the body")
    }

    /// Watch this session's change stream for a while, counting what it reports.
    ///
    /// Returned as a handle so a test can watch two servers at once and see
    /// which of them was told about a change.
    pub fn watch_changes(&self, window: Duration) -> std::thread::JoinHandle<usize> {
        let url = format!("{}/api/events", self.url);
        std::thread::spawn(move || {
            let agent = ureq::AgentBuilder::new()
                .timeout_read(Duration::from_millis(250))
                .build();
            let response = match agent.get(&url).call() {
                Ok(response) => response,
                Err(_) => return 0,
            };
            let mut reader = response.into_reader();
            let deadline = Instant::now() + window;
            let mut seen = String::new();
            let mut buffer = [0u8; 512];
            while Instant::now() < deadline {
                match std::io::Read::read(&mut reader, &mut buffer) {
                    Ok(0) => break,
                    Ok(n) => seen.push_str(&String::from_utf8_lossy(&buffer[..n])),
                    // nothing arrived within the read timeout, which is normal
                    Err(_) => continue,
                }
            }
            seen.matches("data: changed").count()
        })
    }

    pub fn get(&self, path: &str) -> Value {
        let body = ureq::get(&format!("{}{}", self.url, path))
            .call()
            .expect("GET failed")
            .into_string()
            .expect("reading the body");
        serde_json::from_str(&body).unwrap_or(Value::String(body))
    }

    /// The status and body together, for the cases where a refusal is the point.
    pub fn post_json(&self, path: &str, body: Value) -> (u16, Value) {
        let response = ureq::post(&format!("{}{}", self.url, path)).send_json(body);
        status_and_body(response)
    }

    pub fn put_json(&self, path: &str, body: Value) -> (u16, Value) {
        let response = ureq::put(&format!("{}{}", self.url, path)).send_json(body);
        status_and_body(response)
    }

    pub fn delete(&self, path: &str) -> u16 {
        match ureq::delete(&format!("{}{}", self.url, path)).call() {
            Ok(response) => response.status(),
            Err(ureq::Error::Status(code, _)) => code,
            Err(e) => panic!("DELETE failed: {}", e),
        }
    }

    /// Run SQL with a row limit, the way the editor's limit box does.
    pub fn query_with_limit(&self, sql: &str, limit: usize) -> Value {
        let body = ureq::post(&format!("{}/query.json", self.url))
            .send_form(&[("sql", sql), ("display_limit", &limit.to_string())])
            .expect("query failed")
            .into_string()
            .expect("reading the body");
        serde_json::from_str(&body).expect("query.json returns json")
    }

    /// Run SQL the way the query editor does.
    pub fn query(&self, sql: &str) -> Value {
        let body = ureq::post(&format!("{}/query.json", self.url))
            .send_form(&[("sql", sql), ("display_limit", "500")])
            .expect("query failed")
            .into_string()
            .expect("reading the body");
        serde_json::from_str(&body).expect("query.json returns json")
    }

    /// The names in the sidebar, sorted — the single most useful assertion.
    pub fn tables(&self) -> Vec<String> {
        let body = ureq::post(&format!("{}/tables.json", self.url))
            .call()
            .expect("tables.json failed")
            .into_string()
            .expect("reading the body");
        let parsed: Value = serde_json::from_str(&body).expect("tables.json returns json");
        let mut names: Vec<String> = parsed["tables"]
            .as_array()
            .expect("tables is an array")
            .iter()
            .map(|table| table["name"].as_str().unwrap_or_default().to_string())
            .collect();
        names.sort();
        names
    }

    pub fn query_names(&self) -> Vec<String> {
        self.get("/api/queries")["queries"]
            .as_array()
            .expect("queries is an array")
            .iter()
            .map(|query| query["name"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    /// Wait for it to exit on its own, returning how long that took.
    pub fn wait_for_exit(mut self, within: Duration) -> Option<Duration> {
        let started = Instant::now();
        let deadline = started + within;
        while Instant::now() < deadline {
            match self.child.try_wait() {
                Ok(Some(_)) => {
                    let took = started.elapsed();
                    std::mem::forget(self);
                    return Some(took);
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        None
    }

    /// Stop it the way Ctrl-C does, and wait, so the session is closed properly
    /// before anything looks at the store.
    pub fn stop(mut self) {
        terminate(&mut self.child);
        let _ = self.child.wait();
        // hand back nothing: dropping the killed child is all that is left
        std::mem::forget(self);
    }

    /// Kill it outright, leaving the store as it was mid-run.
    pub fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::mem::forget(self);
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        terminate(&mut self.child);
        let _ = self.child.wait();
    }
}

fn terminate(child: &mut Child) {
    // SIGTERM so actix shuts down gracefully and the session is marked closed;
    // SIGKILL would skip that and is what `kill` is for
    #[cfg(unix)]
    unsafe {
        libc_kill(child.id() as i32, 15);
    }
    #[cfg(not(unix))]
    let _ = child.kill();

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(_) => return,
        }
    }
    let _ = child.kill();
}

#[cfg(unix)]
extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

fn status_and_body(response: Result<ureq::Response, ureq::Error>) -> (u16, Value) {
    let (status, body) = match response {
        Ok(response) => (response.status(), response.into_string().unwrap_or_default()),
        Err(ureq::Error::Status(code, response)) => {
            (code, response.into_string().unwrap_or_default())
        }
        Err(e) => panic!("request failed: {}", e),
    };
    (status, serde_json::from_str(&body).unwrap_or(Value::String(body)))
}
