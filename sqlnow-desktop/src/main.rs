//! Desktop shell for sqlnow.
//!
//! This is the same program as the `sqlnow` CLI — same arguments, same DuckDB
//! session, same HTTP server from `libsqlnow` — with a native window pointed
//! at the server instead of a printed URL. The server still listens on
//! loopback and its address is still printed, so a browser tab or an agent can
//! attach to the very same session while the window is open.
#![cfg_attr(all(not(debug_assertions), windows), windows_subsystem = "windows")]

use std::net::SocketAddr;
use std::sync::mpsc;

use eyre::Result;
use tauri::{WebviewUrl, WebviewWindowBuilder};

/// What the server thread reports back once it is listening.
type Bound = (SocketAddr, Option<String>, sqlnow::Closer);

fn main() -> Result<()> {
    let (cli, matches) = sqlnow::parse_args()?;

    // --agents-help and the sql/exec subcommands stay terminal-only: they
    // print and exit without ever opening a window.
    if sqlnow::run_immediate(&cli)? {
        return Ok(());
    }

    // The webview needs the main thread (a hard requirement on macOS), so
    // actix gets a thread of its own. It is the same in-process server the CLI
    // runs — no sidecar binary, no second copy of the UI.
    let (tx, rx) = mpsc::channel::<Result<Bound>>();
    std::thread::spawn(move || {
        let system = actix_web::rt::System::new();
        let started = system.block_on(async move {
            let prepared = sqlnow::prepare(&cli, &matches).await?;
            let host = prepared.host.clone();
            // Port 0 unless asked otherwise: an app launched twice must not
            // fail on a busy 8080, and the real port is read back after bind.
            let (server, addr) = sqlnow::serve(prepared.app_data, &host, prepared.port.unwrap_or(0))?;
            Ok::<_, eyre::Report>((server, addr, prepared.open_query, prepared.closer))
        });

        match started {
            Ok((server, addr, open_query, closer)) => {
                // announce only after a successful bind, so the window is
                // never pointed at an address that does not answer
                if tx.send(Ok((addr, open_query, closer))).is_err() {
                    return;
                }
                if let Err(e) = system.block_on(server) {
                    eprintln!("sqlnow server stopped: {}", e);
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e));
            }
        }
    });

    let (addr, open_query, closer) = rx
        .recv()
        .map_err(|_| eyre::eyre!("the sqlnow server thread stopped before it bound a port"))??;

    let base_url = format!("http://{}", addr);
    // the window is about to open on it, so publish where it can be reached
    closer.mark_live(&base_url);
    let target = sqlnow::query_url(&base_url, open_query.as_deref())
        .unwrap_or_else(|| base_url.clone());

    // still printed: the session is reachable from a browser or an agent while
    // the window is open, which is the whole point of keeping the server
    println!("Server running on {}", base_url);
    if let Some(name) = &open_query {
        println!("Open query \"{}\": {}", name, target);
    }

    let url = target
        .parse()
        .map_err(|e| eyre::eyre!("Could not build a window URL from {}: {}", target, e))?;

    tauri::Builder::default()
        .setup(move |app| {
            WebviewWindowBuilder::new(app, "main", WebviewUrl::External(url))
                .title("sqlnow")
                .inner_size(1280.0, 850.0)
                .build()?;
            Ok(())
        })
        .run(tauri::generate_context!())
        .map_err(|e| eyre::eyre!("{}", e))?;

    // the window has closed, so the session was last used just now
    closer.mark_used();

    Ok(())
}
