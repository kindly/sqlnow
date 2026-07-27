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
//
// A window and the server behind it are one unit: opening another session
// starts another server, and closing a window stops the one it owns. The menu
// is the way in to both, and gives a Close on desktops that draw no window
// decorations at all.

const { app, BrowserWindow, Menu, dialog, shell } = require('electron');
const { spawn } = require('node:child_process');
const path = require('node:path');
const readline = require('node:readline');

/// How long to wait for a server to report its address before giving up.
/// Overridable so a test can watch the giving-up path without waiting a minute.
const STARTUP_MS = Number(process.env.SQLNOW_STARTUP_MS ?? 60_000);

/// The address line arrives first and a deep link (when a query was named) just
/// after it, so the window waits this long for the second one.
const DEEP_LINK_MS = 150;

/// How many sessions the menu offers. The rest stay reachable by id from the
/// command line, the same way `sqlnow --resume` lists a page and no more.
const SESSIONS_LISTED = 20;

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

/// Each window and the server it owns. A window opened on a session something
/// else is already serving owns nothing, and closing it leaves that server be.
const windows = new Map();

/// The sqlnow binary: beside the app when packaged, on PATH when run from a
/// checkout, or wherever SQLNOW_BIN says.
function serverBinary() {
  if (process.env.SQLNOW_BIN) return process.env.SQLNOW_BIN;
  if (!app.isPackaged) return 'sqlnow';
  const name = process.platform === 'win32' ? 'sqlnow.exe' : 'sqlnow';
  return path.join(process.resourcesPath, name);
}

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

/// Start a server and resolve with the address it bound.
///
/// Port 0 unless asked otherwise, so a second session — or the app opened
/// twice — cannot fail on a busy 8080; the real port is read back from what it
/// prints. A refusal to start is the server's to explain, so its message is
/// kept and shown rather than replaced.
function startServer(args) {
  const child = spawn(serverBinary(), [...args, '--port', process.env.PORT ?? '0'], {
    stdio: ['ignore', 'pipe', 'pipe'],
    // so the server stops on its own if this process is killed outright rather
    // than quitting: otherwise it keeps listening and keeps the session open,
    // and the app refuses to reopen it
    env: { ...process.env, SQLNOW_PARENT_PID: String(process.pid) },
  });

  return new Promise((resolve, reject) => {
    let deepLink = null;
    let settled = false;
    let complaint = '';

    // Cleared the moment the address arrives. Left running, it killed the
    // server it was only supposed to give up on: a minute into every session
    // the shell SIGKILLed its own child, the page lost everything, and the
    // reject that followed was a no-op because the promise had long resolved.
    const startupTimer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error(`sqlnow did not report an address within ${STARTUP_MS / 1000}s`));
    }, STARTUP_MS);
    startupTimer.unref?.();

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
        setTimeout(() => {
          if (settled) return;
          settled = true;
          clearTimeout(startupTimer);
          resolve({ child, url: address, deepLink });
        }, DEEP_LINK_MS);
      }
    });

    // piped rather than inherited so a refusal can be put in a dialog: a shell
    // launched from a menu has no terminal for the user to read
    child.stderr.on('data', (chunk) => {
      const text = String(chunk);
      process.stderr.write(text);
      complaint += text;
    });

    child.on('exit', (code) => {
      clearTimeout(startupTimer);
      if (!settled) {
        settled = true;
        reject(new Error(complaint.trim() || `sqlnow exited with ${code} before it bound a port`));
      }
    });
    child.on('error', (e) => {
      clearTimeout(startupTimer);
      reject(new Error(`could not run ${serverBinary()}: ${e.message}`));
    });
  });
}

/// A window on a session, and the server behind it when we started one.
///
/// `url` is the server's address and `target` the page to open — a deep link
/// when the session names a query to land on. They are kept apart because the
/// menu asks the *server* things: with the deep link as the base, every request
/// went to `…/queries/<name>/api/…` and quietly 404'd, which is what left the
/// session list empty and made attaching fail.
function openWindow({ url, target, child }) {
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
  windows.set(window, { child, url });
  window.loadURL(target ?? url);

  // links to anywhere else belong in the user's browser, not in this window
  window.webContents.setWindowOpenHandler(({ url: href }) => {
    shell.openExternal(href);
    return { action: 'deny' };
  });

  wireZoom(window);

  window.on('closed', () => {
    const owned = windows.get(window);
    windows.delete(window);
    console.log(
      `closing the window on ${owned?.url ?? 'an unknown address'}` +
        `${owned?.child ? ` and its server (pid ${owned.child.pid})` : ' (its server is not ours)'}` +
        `; ${windows.size} still open`
    );
    // SIGTERM is what lets sqlnow record the session as last used and shut the
    // server down cleanly; a window on someone else's server owns no child
    owned?.child?.kill('SIGTERM');
  });
  // what the menu offers depends on which window is in front
  window.on('focus', () => buildMenu());

  return window;
}

/// Start a session and show it. `args` is a command line for the server.
async function openSession(args) {
  let started;
  try {
    started = await startServer(args);
  } catch (e) {
    const message = String(e.message ?? e);
    // "already open at http://…" is the server refusing to serve one session
    // twice — and that refusal carries the address we actually want
    const running = message.match(/already open at (http:\/\/\S+)/);
    if (running) {
      showSession(running[1]);
      return;
    }
    dialog.showErrorBox('sqlnow', message);
    return;
  }
  openWindow({ url: started.url, target: started.deepLink, child: started.child });
  buildMenu();
}

/// Put another session in a window that already exists.
///
/// The new server is started before the old one is stopped, so a session that
/// cannot be opened — one already being served elsewhere, an input that has
/// gone — leaves the window on what it was showing rather than emptying it.
async function switchSession(window, args) {
  const previous = windows.get(window);

  let started;
  try {
    started = await startServer(args);
  } catch (e) {
    const message = String(e.message ?? e);
    const running = message.match(/already open at (http:\/\/\S+)/);
    if (running) {
      showSession(running[1]);
    } else {
      dialog.showErrorBox('sqlnow', message);
    }
    return;
  }

  windows.set(window, { child: started.child, url: started.url });
  window.loadURL(started.deepLink ?? started.url);
  // only now, so the window is never pointed at a server that has been asked
  // to stop. A window showing someone else's session owns no child.
  previous?.child?.kill('SIGTERM');
  buildMenu();
}

/// Show a session something else is already serving, starting nothing.
function showSession(url) {
  for (const [window, owned] of windows) {
    if (owned.url.startsWith(url) || url.startsWith(owned.url)) {
      window.focus();
      return window;
    }
  }
  const window = openWindow({ url, child: null });
  buildMenu();
  return window;
}

/// The window in front and the server behind it, which is what the menu acts on.
function current() {
  const window = BrowserWindow.getFocusedWindow() ?? [...windows.keys()][0];
  return window ? { window, ...windows.get(window) } : null;
}

/// Attach files or databases to a running session.
///
/// Straight to the endpoint an agent would use, and the page picks the change
/// up on its own: adding an input bumps the session's change stamp, which the
/// UI is already listening for. Returns what could not be attached.
async function attachPaths(url, paths, kind = 'view') {
  const failures = [];
  for (const uri of paths) {
    try {
      const response = await fetch(`${url}/api/inputs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ uri, kind }),
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        failures.push(`${path.basename(uri)}: ${body.error ?? response.statusText}`);
      }
    } catch (e) {
      failures.push(`${path.basename(uri)}: ${e.message}`);
    }
  }
  return failures;
}

/// What each menu item picks, and what it makes of it.
///
/// A view reads the file where it lies, so the data stays on disk and a later
/// launch replays it; a table copies it into the database, which costs the
/// import once and reads faster after. A database is attached whole, and its
/// tables appear under its name.
const ATTACH_KINDS = {
  view: {
    title: 'Create view over file',
    button: 'Create view',
    kind: 'view',
    filters: [
      { name: 'Data', extensions: ['csv', 'tsv', 'txt', 'parquet', 'json', 'jsonl', 'ndjson', 'xlsx'] },
      { name: 'All files', extensions: ['*'] },
    ],
  },
  table: {
    title: 'Load file as table',
    button: 'Load',
    kind: 'table',
    filters: [
      { name: 'Data', extensions: ['csv', 'tsv', 'txt', 'parquet', 'json', 'jsonl', 'ndjson', 'xlsx'] },
      { name: 'All files', extensions: ['*'] },
    ],
  },
  database: {
    title: 'Attach database',
    button: 'Attach',
    kind: 'view',
    filters: [
      { name: 'Databases', extensions: ['duckdb', 'ddb', 'db', 'sqlite', 'sqlite3'] },
      { name: 'All files', extensions: ['*'] },
    ],
  },
};

async function attachData(which) {
  const here = current();
  if (!here) return;
  const how = ATTACH_KINDS[which];

  const picked = await dialog.showOpenDialog(here.window, {
    title: how.title,
    buttonLabel: how.button,
    properties: ['openFile', 'multiSelections'],
    filters: how.filters,
  });
  if (picked.canceled || picked.filePaths.length === 0) return;

  const failures = await attachPaths(here.url, picked.filePaths, how.kind);
  if (failures.length > 0) {
    dialog.showErrorBox(`Could not ${how.button.toLowerCase()}`, failures.join('\n'));
  }
}

/// Every session in the store, as the server in front sees them.
async function storedSessions(url) {
  try {
    const response = await fetch(`${url}/api/sessions`);
    if (!response.ok) return [];
    const body = await response.json();
    return body.sessions ?? [];
  } catch {
    return [];
  }
}

/// How a session reads in a menu: the file it lives in if it has one, else the
/// inputs it was created for, and how long ago it was used.
function sessionLabel(session) {
  const home = app.getPath('home');
  const short = (where) => (where.startsWith(home) ? `~${where.slice(home.length)}` : where);
  const what = session.path
    ? short(session.path)
    : session.inputs.length > 0
      ? session.inputs.map(short).join(', ')
      : '(no inputs recorded)';

  const seconds = session.age_seconds;
  const age =
    seconds < 60
      ? 'just now'
      : seconds < 3600
        ? `${Math.floor(seconds / 60)}m ago`
        : seconds < 86400
          ? `${Math.floor(seconds / 3600)}h ago`
          : `${Math.floor(seconds / 86400)}d ago`;

  return `${what}  —  ${age}`;
}

/// Rebuild the application menu around the window in front.
///
/// Rebuilt rather than kept in step, because the session list is a question
/// for a server and which server that is depends on focus.
async function buildMenu() {
  const here = current();
  const sessions = here ? await storedSessions(here.url) : [];

  const offered = sessions.slice(0, SESSIONS_LISTED);
  const entry = (session, open) => ({
    label: sessionLabel(session),
    type: 'checkbox',
    checked: session.current === true,
    enabled: session.current !== true,
    // Always through the launcher, never straight at the recorded address: it
    // is what a server published when it opened the session, and a server that
    // died without a clean shutdown left it behind — following one of those
    // opens a window on a dead port, which is a blank page. If the session
    // really is being served the launcher refuses and says where, and that
    // address has just been proven.
    click: () => open(['--resume', session.id]),
  });

  const switchItems = offered.map((session) =>
    entry(session, (args) => here && switchSession(here.window, args))
  );
  const windowItems = offered.map((session) => entry(session, openSession));

  const template = [
    {
      label: '&File',
      submenu: [
        {
          id: 'view',
          label: 'Create View over File…',
          accelerator: 'CommandOrControl+O',
          click: () => attachData('view'),
        },
        { id: 'table', label: 'Load File as Table…', click: () => attachData('table') },
        { id: 'database', label: 'Attach Database…', click: () => attachData('database') },
        { type: 'separator' },
        { id: 'new', label: 'New Session', click: () => openSession([]) },
        { type: 'separator' },
        // Electron hands the click the window its menu belongs to. The `close`
        // role instead acts on whatever it considers focused, which on a
        // tiling desktop is not reliably the window you clicked in.
        {
          id: 'close',
          label: 'Close Session',
          accelerator: 'CommandOrControl+W',
          click: (_item, from) => (from ?? current()?.window)?.close(),
        },
        { id: 'quit', label: 'Quit', accelerator: 'CommandOrControl+Q', click: () => app.quit() },
      ],
    },
    {
      label: '&Session',
      submenu:
        offered.length > 0
          ? [
              { label: 'Open in this window', enabled: false },
              ...switchItems,
              { type: 'separator' },
              {
                label: 'Open in a New Window',
                submenu: windowItems,
              },
            ]
          : [{ label: 'No sessions', enabled: false }],
    },
    {
      label: '&View',
      submenu: [
        { role: 'reload', accelerator: 'CommandOrControl+R' },
        { type: 'separator' },
        // The keys are wireZoom's, which also takes ctrl+= — an accelerator
        // would claim only the shifted plus. registerAccelerator: false shows
        // the shortcut without binding it, so there is one handler, not two.
        {
          label: 'Zoom In',
          accelerator: 'CommandOrControl+Plus',
          registerAccelerator: false,
          click: () => zoomBy(1),
        },
        {
          label: 'Zoom Out',
          accelerator: 'CommandOrControl+-',
          registerAccelerator: false,
          click: () => zoomBy(-1),
        },
        {
          label: 'Actual Size',
          accelerator: 'CommandOrControl+0',
          registerAccelerator: false,
          click: () => zoomTo(0),
        },
        { type: 'separator' },
        { role: 'toggleDevTools', accelerator: 'F12' },
      ],
    },
  ];

  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

/// Zoom the window in front. Levels rather than factors: chromium's own step
/// is one level, which is a factor of 1.2, and level 0 is 100%.
function zoomBy(steps) {
  const here = current();
  if (!here) return;
  const contents = here.window.webContents;
  contents.setZoomLevel(Math.max(-5, Math.min(5, contents.getZoomLevel() + steps)));
}

function zoomTo(level) {
  const here = current();
  if (here) here.window.webContents.setZoomLevel(level);
}

/// Zoom on the keys a browser uses.
///
/// Electron's default menu binds `CommandOrControl+Plus`, and on most layouts
/// `+` is the shifted `=` — so ctrl+shift+= zoomed and plain ctrl+= did
/// nothing. Chrome accepts both, and its reset accelerator did not reach us
/// either, so the keys are handled here rather than left to a menu.
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
    started = await startServer(serverArgs());
  } catch (e) {
    console.error(String(e.message ?? e));
    app.exit(1);
    return;
  }

  const window = openWindow({ url: started.url, target: started.deepLink, child: started.child });
  await buildMenu();

  if (process.env.SQLNOW_SMOKE || process.env.SQLNOW_ZOOM_CHECK || process.env.SQLNOW_MENU_CHECK
      || process.env.SQLNOW_WINDOW_CHECK || process.env.SQLNOW_LIVE_CHECK
      || process.env.SQLNOW_STALE_CHECK || process.env.SQLNOW_SWITCH_CHECK) {
    window.webContents.once('did-finish-load', () => runChecks(window, started));
  }

  app.on('window-all-closed', () => app.quit());
  // a signal aimed at this process should take every server with it
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
    process.on(signal, () => {
      for (const owned of windows.values()) owned.child?.kill('SIGTERM');
      app.exit(0);
    });
  }
}

/// Seams for the tests: drive what a person would, report, and exit.
///
/// Chromium delivers sendInputEvent keys to the renderer exactly as a real
/// press would, and a menu item can be clicked directly, so all of this goes
/// through the same path as using it — bar the native file dialog, which
/// nothing can automate, so the attach is handed the paths it would return.
async function runChecks(window, started) {
  const contents = window.webContents;
  const stop = (code) => {
    for (const owned of windows.values()) owned.child?.kill('SIGTERM');
    app.exit(code);
  };

  try {
    if (process.env.SQLNOW_SMOKE) {
      const title = await contents.executeJavaScript('document.title');
      const tables = await contents.executeJavaScript(
        "fetch('/tables.json', {method: 'POST'}).then(r => r.json()).then(d => d.tables.map(t => t.name).join(','))"
      );
      console.log(`SMOKE url=${contents.getURL()} title=${title} tables=${tables}`);
    }

    if (process.env.SQLNOW_ZOOM_CHECK) {
      const press = (key, modifiers) =>
        new Promise((done) => {
          contents.sendInputEvent({ type: 'keyDown', keyCode: key, modifiers });
          contents.sendInputEvent({ type: 'keyUp', keyCode: key, modifiers });
          setTimeout(done, 120);
        });
      const level = () => contents.getZoomLevel().toFixed(2);
      // chromium keeps zoom per host in the profile, so a previous run leaves
      // this window already zoomed: start from a known level
      const log = [`start ${level()}`];
      contents.setZoomLevel(0);
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
    }

    if (process.env.SQLNOW_LIVE_CHECK) {
      // The startup timeout is set low by the caller, so this waits past it:
      // an uncleared timer killed the server it was watching for, and every
      // session died a minute in with the window still sitting there.
      const server = windows.get(window).child;
      const wait = Number(process.env.SQLNOW_LIVE_CHECK);
      await new Promise((done) => setTimeout(done, wait));
      const alive = (() => {
        try { process.kill(server.pid, 0); return true; } catch { return false; }
      })();
      const answered = await contents.executeJavaScript(
        "fetch('/api/session').then(r => r.ok).catch(() => false)"
      );
      console.log(`LIVE after=${wait}ms server_alive=${alive} page_can_reach_it=${answered}`);
    }

    if (process.env.SQLNOW_SWITCH_CHECK) {
      const before = windows.get(window);
      const menu = Menu.getApplicationMenu().items.find((m) => m.label.includes('Session')).submenu;
      const shape = menu.items.map((i) => (i.type === 'separator' ? '|' : i.label)).join(',');
      // the first offered session in the top list, which replaces this window
      const item = menu.items.find((i) => i.type === 'checkbox' && i.enabled);
      item.click();
      await new Promise((done) => setTimeout(done, 6000));

      const after = windows.get(window);
      const oldGone = (() => {
        try { process.kill(before.child.pid, 0); return false; } catch { return true; }
      })();
      const reachable = await window.webContents.executeJavaScript(
        "fetch('/api/session').then(r => r.ok).catch(() => false)"
      );
      const nested = menu.items.find((i) => i.label.includes('New Window'));
      console.log(
        `SWITCH windows=${windows.size} address_changed=${before.url !== after.url}` +
          ` old_server_stopped=${oldGone} reachable=${reachable}` +
          ` nested_list=${nested ? nested.submenu.items.length : 0} shape=${shape}`
      );
    }

    if (process.env.SQLNOW_STALE_CHECK) {
      // a session whose recorded address belongs to nothing, which is what a
      // server killed outright leaves behind
      const sessions = await storedSessions(windows.get(window).url);
      const other = sessions.find((entry) => entry.current !== true);
      const item = Menu.getApplicationMenu()
        .items.find((menu) => menu.label.includes('Session'))
        .submenu.items.find((entry) => !entry.checked && entry.enabled);
      item.click();
      await new Promise((done) => setTimeout(done, 6000));

      const opened = [...windows.keys()].filter((win) => win !== window);
      const reachable = opened.length
        ? await opened[0].webContents.executeJavaScript(
            "fetch('/api/session').then(r => r.ok).catch(() => false)"
          )
        : false;
      console.log(
        `STALE claimed=${other?.url ?? 'none'} windows=${opened.length + 1}` +
          ` opened_reachable=${reachable}`
      );
    }

    if (process.env.SQLNOW_WINDOW_CHECK) {
      const pidOf = (win) => windows.get(win)?.child?.pid;
      const alive = (pid) => {
        try { process.kill(pid, 0); return true; } catch { return false; }
      };
      const settle = () => new Promise((done) => setTimeout(done, 2500));

      // a second session, the way the Session menu opens one
      await openSession([process.env.SQLNOW_WINDOW_CHECK]);
      await settle();
      const [first, second] = [...windows.keys()];
      const firstServer = pidOf(first);
      const secondServer = pidOf(second);

      // close it the way File > Close does: the menu item, on the window in
      // front, rather than by calling close() on a window we picked ourselves
      first.focus();
      await new Promise((done) => setTimeout(done, 300));
      const closeItem = Menu.getApplicationMenu()
        .items.find((item) => item.label.includes('File'))
        .submenu.items.find((item) => item.id === 'close');
      closeItem.click(undefined, first);
      // how long the server behind it takes to go: graceful shutdown waits for
      // in-flight requests, and the page holds an SSE stream open
      const closedAt = Date.now();
      let took = null;
      for (let i = 0; i < 80; i++) {
        if (!alive(firstServer)) { took = Date.now() - closedAt; break; }
        await new Promise((done) => setTimeout(done, 500));
      }

      console.log(`SHUTDOWN closed_server_gone_after=${took === null ? 'over 40s' : took + 'ms'}`);
      console.log(
        `WINDOWS left=${windows.size}` +
          ` closed_server_alive=${alive(firstServer)}` +
          ` other_server_alive=${alive(secondServer)}` +
          ` other_window_open=${!second.isDestroyed()}`
      );
    }

    if (process.env.SQLNOW_MENU_CHECK) {
      const menu = Menu.getApplicationMenu();
      const menus = menu.items.map((item) => item.label).join(',');
      const file = menu.items.find((item) => item.label.includes('File')).submenu.items;
      const listed = menu.items.find((item) => item.label.includes('Session')).submenu.items;
      // through what the menu itself would use, not the address we happen to
      // have here: the two differed once, and everything quietly 404'd
      const here = current();
      const sessions = await storedSessions(here.url);

      // what each dialog would have handed back, one file per kind
      const [asView, asTable] = process.env.SQLNOW_MENU_CHECK.split(',');
      const failures = [
        ...(await attachPaths(here.url, [asView], 'view')),
        ...(await attachPaths(here.url, [asTable], 'table')),
      ];
      const tables = await contents.executeJavaScript(
        "fetch('/tables.json', {method: 'POST'}).then(r => r.json()).then(d => d.tables.map(t => t.name).sort().join(','))"
      );
      // a view reads the file where it lies; a table was copied in, so it
      // survives the file going away
      const kinds = await contents.executeJavaScript(
        "fetch('/api/inputs').then(r => r.json()).then(d => d.inputs.map(i => i.kind + ':' + i.name).sort().join(','))"
      );
      console.log(
        `MENU menus=${menus} file=${file.filter((i) => i.type !== 'separator').length}` +
          ` sessions=${sessions.length} listed=${listed.length}` +
          ` current=${listed.filter((item) => item.checked).length}` +
          ` failures=${failures.length} tables=${tables} inputs=${kinds}`
      );
    }

  } catch (e) {
    console.error(`CHECK FAILED ${e.message}`);
    stop(1);
    return;
  }
  stop(0);
}

main();
