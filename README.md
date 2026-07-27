# sqlnow

A local SQL viewer built for LLM-agent workflows: an agent (or you) launches
`sqlnow` with data sources and **pre-defined queries**, and the user gets a
fast browser UI to view, tweak, and export the results. DuckDB does the
querying, so parquet, CSV, SQLite, DuckDB, Postgres, and xlsx/json inputs all
work — locally or over s3/http.

```
sqlnow data.parquet -q "sample=SELECT * FROM data LIMIT 100" --open sample
```

That starts a server (default `http://127.0.0.1:8080`), prints a deep link to
the `sample` query, and opens the browser on it.

If you are an agent integrating with sqlnow, read [AGENTS.md](AGENTS.md) —
it has copy-paste recipes for launching sessions, seeding queries, and
reading results and history back. The same guide is embedded in the binary:
`sqlnow --agents-help`.

## Install

Prebuilt single-file binaries are on the
[releases page](https://github.com/kindly/sqlnow/releases) for Linux
(x86_64/arm64, also as `.deb` and `.rpm`), macOS (Apple Silicon), and
Windows — download, unpack, and put `sqlnow` on your `PATH`. Each archive
has a `.sha256` alongside it.

On Arch Linux: `paru -S sqlnow-bin` (or any AUR helper).

### The desktop app

`sqlnow-desktop` is the same server and the same UI in a native window instead
of a browser tab, with the same arguments and the same session store — either
version can open a session the other left. Zoom (`Ctrl +` / `Ctrl -` /
`Ctrl 0`) and the web inspector (`F12`) are the browser's own.

The menu is how you reach the rest. **File** adds data to the running session,
one item per kind: **Create View over File…** leaves the data where it is and
reads it on each query, **Load File as Table…** copies it into the database
once, and **Attach Database…** takes a duckdb or sqlite file whole, its tables
appearing under its name. **New Session** starts an empty one. The **Session**
menu lists everything `sqlnow --resume` would, opening each in its own window —
or focusing the window that already has it, since a session is served once.
Each window owns the server behind it, so closing one stops that session and
leaves the others alone. There is a Close and a Quit in there too, which
matters on desktops that draw no window decorations.

It is an Electron shell around the same `sqlnow` binary: the window is pointed
at the local server exactly as a browser tab would be, and the server is stopped
when the window closes. Electron rather than a system webview so that the
renderer is the same on every platform — three webviews meant three text
rasterisers, and the results grid is drawn on a canvas, which shows the
difference plainly.

On Arch: `paru -S sqlnow-desktop-bin`. Elsewhere, take the AppImage (Linux),
`.dmg` (macOS) or installer (Windows) from the releases page; each carries its
own copy of `sqlnow`, so nothing else is needed. The macOS and Windows builds
are unsigned, so they warn on first run.

Build it yourself with `cargo build --release -p sqlnow` then
`cd sqlnow-electron && npm install && npm run dist` (or `npm start` to run it
without packaging).

### Build from source

```
cd ui && npm install && npm run build && cd ..   # embeds the UI
cargo build --release                            # target/release/sqlnow
```

## Inputs

Name an input or query with `--as` (applies to the value immediately before
it, taken literally — safe for URIs, paths, and SQL containing any
characters); limit a database input's tables with `--only t1` and/or
`--except t2` (repeatable; values are fully-anchored regexes, so plain names
match exactly and `--only 'entity_.*'` works; `--except` applies
after `--only`):

```
sqlnow -v postgresql://host/db --as pg -q 'SELECT count(*) FROM pg.public.t' --as counts
```

For simple names there is also the shorthand `name=uri#table1,table2`
(guarded: existing paths, URIs, and keyword-leading SQL are never split).

| Kind | Example |
|---|---|
| Parquet / CSV (view) | `sqlnow data.parquet sales.csv` |
| xlsx / json / jsonl (loaded as tables) | `sqlnow book.xlsx events.jsonl` |
| SQLite (attached) | `sqlnow app.sqlite` or `-v db=sqlite://app.db` |
| DuckDB (attached) | `sqlnow main.duckdb other.duckdb` |
| Postgres (attached) | `-v pg=postgresql://user:pass@host:5432/dbname` |
| s3 / http | `sqlnow s3://bucket/data.parquet` |

- `-v/--view` creates views (data stays where it is); `-t/--table` copies data
  into tables.
- A **DuckDB file as the first positional argument becomes the main
  database** (same as `--db`): tables you create persist inside it. `.db`
  files are disambiguated from SQLite by their file header.
- `-x/--text` reads all CSV/sqlite columns as text; `--drop` recreates
  existing tables/views.

## Pre-defined queries

- `-q "name=SELECT ..."` — repeatable; bare SQL gets an auto-generated name.
- `--query-file report.sql` or `--query-file "name=path.sql"`.
- `--open <name>` starts the UI on that query (and opens the browser);
  bare `--open` just opens the browser.

## Attaching data to a running session

Inputs are not fixed at startup. `POST /api/inputs` attaches another file or
database to the server that is already running — the tables appear in the
sidebar within a second, and are recorded so later launches replay them —
and `DELETE /api/inputs/<name>` detaches one again:

```
curl -s localhost:8080/api/inputs                                   # what will replay
curl -s -X POST localhost:8080/api/inputs -H 'content-type: application/json' \
  -d '{"uri":"more.parquet"}'                                       # attach
curl -s -X DELETE localhost:8080/api/inputs/more                    # detach
```

Detaching drops the view or table, so with a main database it is removed from
that file; detaching a database input only detaches it.

**The viewer reads; it does not write.** The server holds its main database
read-only, and attaches every other database read-only too, so SQL typed in the
query editor cannot change your data — a `CREATE TABLE` or a `DELETE` against
an attached postgres comes back refused. Data is added through
`POST /api/inputs`, which is the only path allowed to write. Other processes
are unaffected: they can read the main database while a session runs, and a
write from outside (`sqlnow sql`, the duckdb CLI) is noticed and picked up on
the next request.

## Sessions

Queries and run history live in a **session database** (`.sqlnow`), which is
itself a small DuckDB database (tables: `format`, `sessions`, `meta`,
`queries`, `history`, `inputs`). One database can hold many sessions, each
owning its rows through a `session` column, and the server reads and writes it
directly — there is no other state. Where a session lives:

1. With a main database `data.duckdb`, the session is `data.duckdb.sqlnow`
   next to it (only database attaches are recorded there; file tables persist
   in the main db itself).
2. `sqlnow session.sqlnow` (a session file as the first `.sqlnow` argument)
   continues that session: its inputs replay and new queries and history are
   written back to it. Create one anywhere with `sqlnow exec name.sqlnow
   "SELECT 1"`.
3. With none of the above, the session goes in the **store** —
   `<config dir>/sqlnow/sessions.sqlnow`, so `~/.config/sqlnow/sessions.sqlnow`
   on Linux — keyed by the set of inputs the run was given, and its id is
   printed on startup. Run the same command again and that session resumes,
   queries and history included. Nothing is ever deleted behind your back: one
   store holding a hundred sessions is a couple of megabytes, and pruning is
   `sqlnow delete`, asked for explicitly. If a resumed session records an
   input that has since moved or gone, the run stops with an error naming it
   instead of starting up without that table.

Sessions from cases 1 and 2 are recorded in the store as well — just a pointer
to their file, since their queries and history stay there — so one list covers
everything you have worked on. `--no-register` leaves a run out of it; the
session works as usual, but `--resume` will not find it.

`sqlnow --resume` lists sessions, most recently used first, showing what each
one is — its own file if it has one, otherwise the inputs it was created for
(and `-` for queries when they live in a file the store cannot count):

```
#  id        used     queries  state   session
1  81b95136  just now       3  live    ~/data/plants.parquet, ~/data/units.csv
2  7c287423  1d ago         -          ~/work/plants.duckdb
3  64942c08  3d ago         1          postgresql://host/db

Session 81b95136 is open at http://127.0.0.1:8080
```

A session marked `live` has a server on it right now. Opening a second server
on the same session is refused — two would write over each other's queries and
history — and the error tells you where the first one is. The addresses are
pinged rather than trusted, so a server that was killed outright leaves nothing
blocking you: the listing finds it dead, clears the address and moves on.

A session whose file has since moved is listed as `(missing)` rather than
dropped, and resuming it says so instead of quietly starting an empty one.

`sqlnow --resume 2` opens one by position, `sqlnow --resume 64942c` by id, and
either replays that session's inputs — so you get the tables, queries and
history back without retyping the launch command. The 20 most recent are
listed; older ones are still there and can be resumed by id.

Inspect or edit a session with the built-in runner (no duckdb install
needed): `sqlnow exec session.sqlnow "SELECT * FROM queries"`.

### Deleting a session

`sqlnow delete 3` (a position from the listing) or `sqlnow delete 64942c` (an
id) removes that session and everything recorded under it — its saved queries,
its query history, its recorded inputs and its metadata. Several at once is
fine: `sqlnow delete 2 4 81b95136`.

Nothing else is touched. The data files the session read are never opened, and
a session that lives in a file of its own is removed from that file and from
the listing, leaving the file itself in place. A session with a server on it is
refused until you close it, and every argument is resolved before anything is
deleted — so a typo in the third one means none of them go.

It asks first, and there is no undo. Without a terminal to ask on (a script, an
agent) it refuses unless you pass `--yes`.

## Querying the database from the command line

`sqlnow sql` runs SQL against a DuckDB database file directly — like the
duckdb CLI, but with the session sidecar's attaches replayed so
cross-database queries keep working, and no separate install:

```
sqlnow sql data.duckdb "SELECT count(*) FROM sales"        # duckdb-style table
sqlnow sql data.duckdb -f csv "SELECT * FROM sales" > out.csv
sqlnow sql data.duckdb -f jsonl "SELECT * FROM sales" | jq .total
sqlnow sql data.duckdb "CREATE TABLE top AS SELECT * FROM sales LIMIT 10"
```

Formats: `box` (default), `csv`, `json`, `jsonl`; `--limit N` caps returned
rows. Writes persist. This also works while a sqlnow server has the same
database open — the server holds no connection between requests.

History is **never truncated** — every query ever run in a persisted session
stays retrievable (identical SQL just refreshes its timestamp). The UI shows
the most recent 200; the full list is available via the API or `exec`.

Legacy line-format `.sqlnow` files from older versions are still read and are
upgraded to the database format in place. Queries stored in browser
localStorage by older versions are not migrated.

## Server

`--port` and `--host` control binding (defaults `8080` on `127.0.0.1`), with
`PORT`/`HOST` env vars as fallbacks; `WORKERS` env sets worker count
(default 1). The UI talks to a JSON API (`/api/queries`, `/api/history`)
documented in [AGENTS.md](AGENTS.md); exports stream from `POST /outputs`
as CSV/TSV/JSONL, or come back with `X-Sqlnow-Rows` and
`X-Sqlnow-Truncated` headers if you pass a `limit`. Results from
`POST /query.json` carry the limit that was applied and a `truncated` flag,
so a caller can tell a complete answer from a first page — AGENTS.md has a
table of which route to use for what.

## Security

sqlnow has **no authentication**, and anyone who can reach the port can run
arbitrary SQL — which, through DuckDB, can read and write files on the host
(`read_csv('/etc/…')`, `COPY … TO`, `ATTACH`). The default bind is
loopback-only. Only ever bind (`--host`) to interfaces where every client is
trusted — e.g. a private tailnet — and never expose it to the public
internet.

`--only`/`--except` are focus filters, not access control: they decide what
is listed in the UI, but the whole database is attached and a hidden table
can still be queried by name.
