// Desktop shell for sqlnow.
//
// The same shape as the tauri shell it replaces: the `sqlnow` binary does the
// work — it serves the UI over loopback exactly as it does for a browser tab —
// and this opens a window on it. Nothing here touches DuckDB, and the page is
// ordinary HTTP content, so there is no IPC surface and no second copy of the
// UI to keep in step.
//
// Electron rather than a system webview because the renderer is then the same
// everywhere. Three webviews meant three text rasterisers, and the one on
// linux drew the canvas grid noticeably worse than a browser did — a class of
// difference that can only be found on the machine it happens on.

const { app, BrowserWindow, shell } = require('electron');
const { spawn } = require('node:child_process');
const path = require('node:path');
const readline = require('node:readline');

/// How long to wait for the server to report its address before giving up.
const STARTUP_MS = 60_000;

/// The address line arrives first and a deep link (when a query was named) just
/// after it, so the window waits this long for the second one.
const DEEP_LINK_MS = 150;

/// The sqlnow binary: beside the app when packaged, on PATH when run from a
/// checkout, or wherever SQLNOW_BIN says.
function serverBinary() {
  if (process.env.SQLNOW_BIN) return process.env.SQLNOW_BIN;
  if (!app.isPackaged) return 'sqlnow';
  const name = process.platform === 'win32' ? 'sqlnow.exe' : 'sqlnow';
  return path.join(process.resourcesPath, name);
}

/// Switches that belong to electron or chromium rather than to sqlnow.
///
/// The two share one command line, so anything a user passes for the window —
/// `--no-sandbox` being the common one — would otherwise reach the server and
/// be rejected as an unknown argument. Everything after a bare `--` is the
/// server's regardless, which is the escape hatch when this list is wrong.
const SHELL_SWITCHES = [
  'no-sandbox',
  'disable-gpu',
  'disable-gpu-sandbox',
  'disable-software-rasterizer',
  'in-process-gpu',
  'enable-logging',
  'enable-features',
  'disable-features',
  'ozone-platform',
  'ozone-platform-hint',
  'gtk-version',
  'force-device-scale-factor',
  'user-data-dir',
  'remote-debugging-port',
  'lang',
  'trace-startup',
  'inspect',
  'inspect-brk',
];

/// Everything the user typed that the server should see.
///
/// Electron keeps argv[0] for itself and, unpackaged, argv[1] is the app path.
function serverArgs() {
  const argv = process.argv.slice(app.isPackaged ? 1 : 2).filter((arg) => arg !== '.');

  const separator = argv.indexOf('--');
  if (separator !== -1) {
    return argv.slice(separator + 1);
  }

  return argv.filter((arg) => {
    if (!arg.startsWith('--')) return true;
    const name = arg.slice(2).split('=')[0];
    return !SHELL_SWITCHES.includes(name);
  });
}

/// Start the server and resolve with the address it bound.
///
/// Port 0 unless asked otherwise, so opening the app twice cannot fail on a
/// busy 8080 — the real port is read back from what it prints.
function startServer() {
  const child = spawn(serverBinary(), [...serverArgs(), '--port', process.env.PORT ?? '0'], {
    stdio: ['ignore', 'pipe', 'inherit'],
    // so the server stops on its own if this process is killed outright rather
    // than quitting: otherwise it keeps listening and keeps the session open,
    // and the app refuses to reopen it
    env: { ...process.env, SQLNOW_PARENT_PID: String(process.pid) },
  });

  return new Promise((resolve, reject) => {
    let deepLink = null;
    let settled = false;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      resolve(result);
    };

    const lines = readline.createInterface({ input: child.stdout });
    lines.on('line', (line) => {
      // still printed: the session is reachable from a browser or an agent
      // while the window is open, which is the point of keeping the server
      process.stdout.write(`${line}\n`);
      if (line.startsWith('Open query ')) {
        deepLink = line.slice(line.indexOf('http')).trim();
      }
      const address = line.startsWith('Server running on ') ? line.slice(18).trim() : null;
      if (address) {
        setTimeout(() => finish({ child, url: deepLink ?? address }), DEEP_LINK_MS);
      }
    });

    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error(`sqlnow did not report an address within ${STARTUP_MS / 1000}s`));
    }, STARTUP_MS);
    timer.unref?.();

    child.on('exit', (code) => {
      // a refusal to start (a live session, a missing input) is the server's to
      // explain: it has already said so on stderr
      if (!settled) reject(new Error(`sqlnow exited with ${code} before it bound a port`));
      else app.quit();
    });
    child.on('error', (e) => reject(new Error(`could not run ${serverBinary()}: ${e.message}`)));
  });
}

/// Zoom on the keys a browser uses.
///
/// Electron's default menu binds `CommandOrControl+Plus`, and on most layouts
/// `+` is the shifted `=` — so Ctrl+Shift+= zoomed and plain Ctrl+= did
/// nothing. Chrome accepts both, and its reset accelerator did not reach us
/// either, so the keys are handled here instead of left to the menu.
///
/// Levels rather than factors: chromium's own step is one level, which is a
/// factor of 1.2, and level 0 is 100%.
function wireZoom(window) {
  const contents = window.webContents;
  const bounded = (level) => Math.max(-5, Math.min(5, level));

  contents.on('before-input-event', (event, input) => {
    if (input.type !== 'keyDown' || !(input.control || input.meta)) return;

    // `=` and `+` are the same physical key, shift apart; the numpad sends its
    // own names on some platforms
    if (['=', '+', 'Add'].includes(input.key)) {
      contents.setZoomLevel(bounded(contents.getZoomLevel() + 1));
      event.preventDefault();
    } else if (['-', '_', 'Subtract'].includes(input.key)) {
      contents.setZoomLevel(bounded(contents.getZoomLevel() - 1));
      event.preventDefault();
    } else if (input.key === '0') {
      contents.setZoomLevel(0);
      event.preventDefault();
    }
  });
}

async function main() {
  await app.whenReady();

  let started;
  try {
    started = await startServer();
  } catch (e) {
    console.error(String(e.message ?? e));
    app.exit(1);
    return;
  }

  const window = new BrowserWindow({
    width: 1280,
    height: 850,
    title: 'sqlnow',
    icon: path.join(__dirname, 'icons', 'icon.png'),
    backgroundColor: '#ffffff',
    webPreferences: {
      // the page comes from our own server over HTTP: it needs no node, no
      // preload and no bridge, so it gets none
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
    },
  });
  window.setMenuBarVisibility(false);
  window.loadURL(started.url);

  wireZoom(window);

  // links to anywhere else belong in the user's browser, not in this window
  window.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  // A seam for the smoke test: report what loaded and exit, so CI can check
  // that a real window really does display a real session.
  if (process.env.SQLNOW_SMOKE) {
    window.webContents.once('did-finish-load', async () => {
      const title = await window.webContents.executeJavaScript('document.title');
      const tables = await window.webContents.executeJavaScript(
        "fetch('/tables.json', {method: 'POST'}).then(r => r.json()).then(d => d.tables.map(t => t.name).join(','))"
      );
      console.log(`SMOKE url=${window.webContents.getURL()} title=${title} tables=${tables}`);
      started.child.kill('SIGTERM');
      app.exit(0);
    });
  }

  // A seam for the zoom test: chromium delivers these to the renderer exactly
  // as a real key press, so before-input-event sees what a user would send.
  if (process.env.SQLNOW_ZOOM_CHECK) {
    window.webContents.once('did-finish-load', async () => {
      const press = (key, modifiers) =>
        new Promise((done) => {
          window.webContents.sendInputEvent({ type: 'keyDown', keyCode: key, modifiers });
          window.webContents.sendInputEvent({ type: 'keyUp', keyCode: key, modifiers });
          setTimeout(done, 120);
        });
      const level = () => window.webContents.getZoomLevel().toFixed(2);

      const log = [];
      for (const [label, key, modifiers] of [
        ['ctrl+=', '=', ['control']],
        ['ctrl+= again', '=', ['control']],
        ['ctrl+shift+=', '=', ['control', 'shift']],
        ['ctrl+-', '-', ['control']],
        ['ctrl+0', '0', ['control']],
        ['plain =', '=', []],
      ]) {
        await press(key, modifiers);
        log.push(`${label} -> ${level()}`);
      }
      console.log(`ZOOM ${log.join(' | ')}`);
      started.child.kill('SIGTERM');
      app.exit(0);
    });
  }

  // closing the window ends the session: SIGTERM is what lets sqlnow record it
  // as last used and shut the server down cleanly
  const stopServer = () => started.child.kill('SIGTERM');
  app.on('window-all-closed', () => {
    stopServer();
    app.quit();
  });
  app.on('before-quit', stopServer);
  // a signal aimed at this process should take the server with it too
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
    process.on(signal, () => {
      stopServer();
      app.exit(0);
    });
  }
}

main();
