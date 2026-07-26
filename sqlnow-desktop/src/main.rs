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
            let window = WebviewWindowBuilder::new(app, "main", WebviewUrl::External(url))
                .title("sqlnow")
                .inner_size(1280.0, 850.0)
                // WebView2 zooms itself with these; the other platforms need
                // the keys wiring up, which is what wire_shortcuts does
                .zoom_hotkeys_enabled(true)
                .build()?;
            // Match the desktop's own text scaling before anything is drawn.
            // WebKitGTK renders at devicePixelRatio 1 whatever the screen is,
            // while a browser reads the same setting and scales — which is why
            // the canvas grid looked coarse here and fine in a browser tab.
            let base = desktop_scale();
            if base != 1.0 {
                if let Err(e) = window.set_zoom(base) {
                    eprintln!("note: could not apply the desktop text scale ({})", e);
                }
            }
            if let Err(e) = wire_shortcuts(&window, base) {
                eprintln!("note: keyboard shortcuts unavailable ({})", e);
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .map_err(|e| eyre::eyre!("{}", e))?;

    // the window has closed, so the session was last used just now
    closer.mark_used();

    Ok(())
}

/// Zoom and inspect, on the keys a browser uses.
///
/// wry implements `zoom_hotkeys_enabled` for WebView2 only, so on GTK nothing
/// happens unless the keys are handled here: the window is a real
/// `gtk::ApplicationWindow`, and key presses reach it before the webview.
///
/// Zoom is tracked here rather than read back, because the webview has no
/// getter for it. It starts at 1.0, which is where the window opens.
#[cfg(target_os = "linux")]
fn wire_shortcuts(window: &tauri::WebviewWindow, base: f64) -> Result<()> {
    use gtk::gdk::keys::constants as key;
    use gtk::gdk::ModifierType;
    use gtk::prelude::*;

    /// Multiplying keeps each step the same size as it feels: 1.2× per press,
    /// the same ratio chromium uses, clamped where text stops being readable.
    const STEP: f64 = 1.2;
    const RANGE: (f64, f64) = (0.25, 5.0);

    let gtk_window = window.gtk_window().map_err(|e| eyre::eyre!("{}", e))?;
    // steps compose with the desktop scale, and Ctrl+0 goes back to it rather
    // than to a 1.0 nobody asked for
    let zoom = std::rc::Rc::new(std::cell::Cell::new(base));
    let webview = window.clone();

    gtk_window.connect_key_press_event(move |_, event| {
        let state = event.state();
        let ctrl = state.contains(ModifierType::CONTROL_MASK);
        let shift = state.contains(ModifierType::SHIFT_MASK);
        let pressed = event.keyval();

        // Ctrl+= as well as Ctrl+plus: on most layouts the + is the shifted =,
        // and nobody wants to be told to hold shift
        let level = match pressed {
            key::plus | key::equal | key::KP_Add if ctrl => {
                Some((zoom.get() * STEP).clamp(RANGE.0, RANGE.1))
            }
            key::minus | key::KP_Subtract if ctrl => {
                Some((zoom.get() / STEP).clamp(RANGE.0, RANGE.1))
            }
            key::_0 | key::KP_0 if ctrl => Some(base),
            _ => None,
        };
        if let Some(level) = level {
            zoom.set(level);
            if let Err(e) = webview.set_zoom(level) {
                eprintln!("note: could not zoom ({})", e);
            }
            return gtk::glib::Propagation::Stop;
        }

        // F12 and Ctrl+Shift+I, the two every browser answers to. WebKit's own
        // "Inspect Element" in the right-click menu works too, now that the
        // devtools feature keeps developer extras on in a release build.
        if pressed == key::F12 || (ctrl && shift && (pressed == key::I || pressed == key::i)) {
            webview.open_devtools();
            return gtk::glib::Propagation::Stop;
        }

        gtk::glib::Propagation::Proceed
    });
    Ok(())
}

/// Nothing to wire: WebView2 has its own zoom hotkeys and its own F12, and
/// macOS is not built yet.
#[cfg(not(target_os = "linux"))]
fn wire_shortcuts(_window: &tauri::WebviewWindow, _base: f64) -> Result<()> {
    Ok(())
}

/// How much the desktop wants text scaled up, as a zoom factor.
///
/// GTK keeps this as an Xft DPI in 1024ths — 132710 is 129.6 DPI, which is
/// 1.35× the 96 DPI baseline — and every toolkit on the desktop honours it.
/// WebKitGTK does not apply it to the page: `devicePixelRatio` stays 1 however
/// dense the screen is, so a canvas renders one bitmap pixel per CSS pixel and
/// 12px text lands on 12 physical pixels. Chrome reads the same setting and
/// scales, which is the whole of the difference between the two.
#[cfg(target_os = "linux")]
fn desktop_scale() -> f64 {
    use gtk::traits::GtkSettingsExt;

    match gtk::Settings::default() {
        Some(settings) => scale_from_xft_dpi(settings.gtk_xft_dpi()),
        None => 1.0,
    }
}

/// GTK's Xft DPI, in 1024ths, as a zoom factor against the 96 DPI baseline.
///
/// A scale below 1 is ignored: it would shrink text nobody asked to shrink.
/// The ceiling is where a mistaken setting would stop being recoverable with
/// Ctrl+0, which resets to this value rather than to 1.
#[cfg(target_os = "linux")]
fn scale_from_xft_dpi(dpi_1024ths: i32) -> f64 {
    // negative means unset, and every toolkit then assumes 96
    if dpi_1024ths <= 0 {
        return 1.0;
    }
    ((f64::from(dpi_1024ths) / 1024.0) / 96.0).clamp(1.0, 3.0)
}

#[cfg(not(target_os = "linux"))]
fn desktop_scale() -> f64 {
    // WebView2 and WKWebView both scale with the system already
    1.0
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::scale_from_xft_dpi;

    #[test]
    fn the_desktop_text_scale_is_read_from_gtk() {
        // 96 DPI is the baseline every toolkit assumes: no scaling
        assert_eq!(scale_from_xft_dpi(96 * 1024), 1.0);
        // this machine's 132710 is 129.6 DPI, which is 1.35x
        assert!((scale_from_xft_dpi(132710) - 1.35).abs() < 0.001);
        assert_eq!(scale_from_xft_dpi(192 * 1024), 2.0);

        // unset, and a value that would shrink text, both mean "leave it alone"
        assert_eq!(scale_from_xft_dpi(-1), 1.0);
        assert_eq!(scale_from_xft_dpi(0), 1.0);
        assert_eq!(scale_from_xft_dpi(48 * 1024), 1.0);

        // and something absurd stays recoverable
        assert_eq!(scale_from_xft_dpi(9600 * 1024), 3.0);
    }
}
