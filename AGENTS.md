# sqlnow for agents

sqlnow turns data files and databases into a browser UI for a human, with
queries you define up front. This document is the integration reference:
how to launch sessions, seed queries, point the user at the right place, and
read back what they did.

This guide ships inside the binary: `sqlnow --agents-help` prints it, so it
is always available (and always matches the binary's version) even without
this repository.

## 1. Launching (the common case)

```bash
# one file, one query, browser opens on it
sqlnow data.parquet -q "top rows=SELECT * FROM data ORDER BY total DESC LIMIT 100" --open "top rows"

# several queries; -q is repeatable, names before '=', bare SQL gets auto-named
sqlnow sales.csv customers.sqlite \
  -q "by region=SELECT region, sum(total) FROM sales GROUP BY 1" \
  -q "recent=SELECT * FROM sales ORDER BY at DESC LIMIT 50" \
  --open "by region"

# long SQL from files
sqlnow data.parquet --query-file report.sql --query-file "join check=checks/join.sql"

# the session (queries + history) is kept automatically, keyed by the inputs
sqlnow data.parquet -q "peek=SELECT * FROM data LIMIT 100"
# ... later: the same inputs continue exactly where they left off
sqlnow data.parquet
# ... or find it among the recent ones
sqlnow --resume
```

Inputs are positional or via `-v` (view) / `-t` (table): parquet, csv, xlsx,
json/jsonl, sqlite files, duckdb files, `postgresql://...` URIs, s3/http
URLs. A DuckDB file as the **first** positional argument becomes the main
database (tables created there persist in it).

**Naming things — `--as` is the always-safe form.** It names the input or
query immediately before it, and makes that value completely literal (no
splitting of any kind), so any URI, path, SQL, or name works:

```bash
sqlnow -v 'postgresql://localhost/db?sslmode=disable' --as gem \
       -v app.sqlite --as "legacy db" --only 'entity_.*' --except entity_log \
       -q 'SELECT * FROM t WHERE a=1' --as 'top rows'
```

The `name=target` / `name=SQL` shorthand also exists for simple cases
(`-v gem=postgresql://...`, `-q "top=SELECT 1"`). It is guarded — it never
splits existing file paths, URIs, or SQL that starts with a SQL keyword —
but when a value could be ambiguous, prefer `--as`.

**Relay the URL**: the server prints these lines on startup — pass the deep
link to the user.

```
Server running on http://127.0.0.1:8080
Open query "top rows": http://127.0.0.1:8080/queries/top%20rows
```

`--open <name>` also launches a browser **on the user's desktop** — even when
you run in a sandbox or background shell. Only pass it on the final launch of
a server that will keep running for the user. Never use it for test or
verification launches, and never when your processes die with your shell
(prepare the session and report the launch command instead — include `--open`
in the command you hand over, not in the ones you run). Otherwise omit it and
relay the printed URL. `--port`/`--host` (or the `PORT`/`HOST` env vars)
control binding.

Practical notes that save time:

- **One launch command is usually all you need.** For a fresh session, do not
  create a database first or seed queries with `exec` — a single command
  records the inputs, saves every query, and picks the landing query:

  ```bash
  sqlnow example.parquet data.sqlite \
    -q "jurisdictions=SELECT incorporatedInJurisdiction_name, count(*) AS c FROM example GROUP BY 1 ORDER BY c DESC" \
    -q "sample=SELECT * FROM example LIMIT 100" \
    --open jurisdictions
  ```

  Each file is queryable by its stem (`example`, `data`); sqlite/duckdb
  attaches expose their tables under that name (`data.some_table`).
  `exec`-based seeding is only for adding to an existing session file.
- **Answering one-off questions needs no server.** `sqlnow sql` works against
  any DuckDB file and can read files in place:
  `sqlnow exec scratch.sqlnow "SELECT 1"` once to create a scratch db, then
  `sqlnow sql scratch.sqlnow "SELECT count(*) FROM read_parquet('data.parquet')"`.

- **Keeping the server alive**: if you run inside an agent harness, launch the
  server with the harness's own background mechanism (e.g. a background tool
  call). `nohup ... &`/`disown` inside a sandboxed shell dies with that shell.
  If your processes cannot outlive your task at all (typical for subagents),
  don't promise a running server: prepare the session file and report the
  exact launch command — everything (attaches, queries, history) replays from
  the session file.
- **Startup is done when `Server running on ...` prints.** Attaches and the
  session file are fully written by then; killing the process earlier can
  leave the session file without its recorded inputs.
- **Attaches replay automatically.** Once a launch has recorded inputs in the
  session file, later launches (`sqlnow data.duckdb`, `sqlnow session.sqlnow`)
  and `sqlnow sql` replay them — do not re-pass `-v`/`-t` each time.
- **Postgres over a Unix socket**: if TCP is refused, pass the socket
  directory libpq-style: `postgresql://localhost/dbname?host=/run/postgresql`.

## 2. HTTP API (live channel, always safe)

While the server runs, this is the reliable way to add or change queries —
the UI picks changes up on its next navigation or reload.

```bash
# list queries (also returns which one is the session's "open" query)
curl -s localhost:8080/api/queries
# -> {"open":"top rows","queries":[{"name":"top rows","sql":"SELECT ..."}]}

# add a query (omit "name" for an auto-generated one) — 201, 409 if the name exists
curl -s -X POST localhost:8080/api/queries \
  -H 'content-type: application/json' \
  -d '{"name":"errors by day","sql":"SELECT day, count(*) FROM errors GROUP BY 1"}'

# read / update / rename / delete
curl -s localhost:8080/api/queries/errors%20by%20day
curl -s -X PUT localhost:8080/api/queries/errors%20by%20day \
  -H 'content-type: application/json' -d '{"sql":"SELECT 1"}'
curl -s -X PUT localhost:8080/api/queries/errors%20by%20day \
  -H 'content-type: application/json' -d '{"name":"errors"}'     # rename
curl -s -X DELETE localhost:8080/api/queries/errors              # 204; sql kept in history

# run history, newest first (limit=0 or omitted returns everything)
curl -s "localhost:8080/api/history?limit=50"
# -> {"history":[{"at":"2026-07-23 21:45:38","sql":"SELECT 42"}]}

# change stream (server-sent events): emits `data: changed` within ~1s of any
# session change, whatever the writer (this API, the UI, or sqlnow exec)
curl -sN localhost:8080/api/events
```

**Attaching data to a running session.** You do not have to relaunch to add a
file or database — the new tables appear in the user's sidebar within about a
second, and are recorded so later launches replay them:

```bash
# what the session will replay next time
curl -s localhost:8080/api/inputs

# attach a file: "as" names it (default: the file stem), "kind" is
# "view" (query it where it lies, the default) or "table" (read it in once)
curl -s -X POST localhost:8080/api/inputs \
  -H 'content-type: application/json' \
  -d '{"uri":"/data/plants.parquet"}'                       # -> 201

# a database, named, with the same table filters as --only/--except
curl -s -X POST localhost:8080/api/inputs \
  -H 'content-type: application/json' \
  -d '{"uri":"postgresql://localhost/db","as":"pg","only":["entity_.*"]}'

# detach by name — 204, or 404 if nothing has that name
curl -s -X DELETE localhost:8080/api/inputs/plants
```

Attaching a name that is taken is a 400 rather than a silent no-op: detach it
first to replace it. A path that does not exist is reported before anything is
attached. **`DELETE` drops the view or table**, so with a main database it
removes it from that file for good; detaching a *database* input only detaches
it and leaves the file alone.

**Live updates**: the UI subscribes to `/api/events`, so queries you add or
update on a running server appear in the user's browser within about a
second — including the query they are currently looking at. Updating an open
query is safe: the previous SQL is preserved in history before being
overwritten (a PUT without `base_sql` always archives what it replaces), so
feel free to push improved versions of a query while the user watches.

Query names are identities: unique, case-sensitive, no `/`, max 100 chars.
Percent-encode them in URLs. The UI deep link for a query is
`/queries/<percent-encoded name>`.

To run SQL programmatically and get results (same endpoint the UI uses —
form-encoded, not JSON):

```bash
curl -s -d 'sql=SELECT count(*) FROM data' -d 'display_limit=500' localhost:8080/query.json
# -> {"error":null,"table_data":{"headers":[...],"rows":[[...]]}}   (runs land in history)

# streaming exports (form-encoded; the field name picks the format: csv | tab | jsonl)
curl -s -d 'sql=SELECT * FROM data' -d 'csv=1' localhost:8080/outputs > out.csv
```

## 3. Querying the database: `sqlnow sql`

For database work outside the browser — checking what a session produced,
building derived tables, extracting results — `sqlnow sql` runs SQL against
a DuckDB database file using sqlnow's embedded engine. No duckdb install, no
version mismatch. Recorded inputs are replayed automatically so names
resolve the same way they do in the UI: point it at a main database (its
sidecar's attaches replay) or directly at a session `.sqlnow` file (its own
recorded attaches and file views replay; file views are temporary, so a
query never writes them into the session file):

```bash
sqlnow sql data.duckdb "SELECT count(*) FROM sales"          # duckdb-style box table
sqlnow sql data.duckdb -f csv "SELECT * FROM sales"          # machine-readable
sqlnow sql data.duckdb -f json "SELECT * FROM sales LIMIT 5" # array of objects
sqlnow sql data.duckdb -f jsonl "SELECT * FROM sales" | jq .
sqlnow sql data.duckdb --limit 100 "SELECT * FROM big_table"
sqlnow sql data.duckdb "CREATE TABLE summary AS SELECT region, sum(total) FROM sales GROUP BY 1"
```

Writes persist in the file. Values in json/jsonl output are strings (the
same stringification the UI grid uses). This works even while a sqlnow
server has the database open — the server holds no connection between
requests — though a concurrent operation can occasionally hit a lock (brief
automatic retry, then a clear error). On a running server the HTTP
equivalents are `POST /query.json` (JSON results) and `POST /outputs`
(streaming csv/tsv/jsonl).

## 4. Session files and `sqlnow exec`

A session file (`.sqlnow`) is a DuckDB database with this schema (format 2):

```sql
format(version INTEGER)                     -- 2; a higher number is refused
sessions(id TEXT PRIMARY KEY, key TEXT, path TEXT,
         last_used TIMESTAMP, changed_at TIMESTAMP)
meta(session TEXT, key TEXT, value TEXT, PRIMARY KEY (session, key))  -- keys: 'open'
queries(session TEXT, pos INTEGER, name TEXT, sql TEXT, PRIMARY KEY (session, name))
history(session TEXT, "at" TIMESTAMP DEFAULT now(), sql TEXT)
inputs(session TEXT, kind TEXT, name TEXT, uri TEXT, tables TEXT[], except_tables TEXT[])
```

One database can hold **many** sessions, each owning its rows through the
`session` column: a file you name holds one, and the store under the config
directory holds every unanchored run. So scope writes by session id — for a
single-session file, `(SELECT id FROM sessions)` is that one id. Format 1
files (no `format` table, no `session` column) are migrated in place the
first time they are opened.

`sqlnow exec` runs SQL against a session database using sqlnow's own embedded
DuckDB — nothing else to install, no version mismatch. It creates the file
(with the schema and one session) if missing, so you can seed before the first
launch, and it works on the multi-session store too. It refuses to touch an
existing database that is not a session file (query those with `sqlnow sql`
instead):

```bash
sqlnow exec session.sqlnow "INSERT INTO queries(session, pos, name, sql)
  SELECT id, 1, 'top emitters', 'SELECT name, co2 FROM plants ORDER BY co2 DESC LIMIT 50'
  FROM sessions"
sqlnow exec session.sqlnow "INSERT INTO meta(session, key, value)
  SELECT id, 'open', 'top emitters' FROM sessions"
sqlnow session.sqlnow plants.parquet
```

Results print as CSV with a header row. Multi-statement input is allowed
(no rows returned). When the SQL you are inserting itself contains quotes,
duckdb's dollar-quoting avoids all escaping:

```bash
sqlnow exec session.sqlnow "INSERT INTO queries(session, pos, name, sql)
  SELECT id, 1, 'names', \$q\$SELECT name FROM t WHERE note = 'it''s fine'\$q\$ FROM sessions"
```

This also works while a server is running — the server
holds no file handle between requests — but concurrent access can
occasionally hit a lock; the HTTP API never does, so prefer it for live
changes.

## 5. Reading back what the user did

```bash
# while running
curl -s "localhost:8080/api/history"

# after the session ended
sqlnow exec session.sqlnow "SELECT \"at\", sql FROM history ORDER BY \"at\" DESC"
sqlnow exec session.sqlnow "SELECT name, sql FROM queries ORDER BY pos"
```

History is uncapped and deduplicated: rerunning identical SQL refreshes its
timestamp instead of adding a row, so it is a complete record of everything
tried. Failed queries are recorded too.

## 6. Constraints

- One server per session file; while running, the server is the writer of
  record. Use the HTTP API for live changes; direct `exec` writes work but
  can race a concurrent server operation.
- Runs without an anchor (`--db`/first-duckdb, or a replayed `.sqlnow`) still
  get a session: it lives in the store, `<config dir>/sqlnow/sessions.sqlnow`,
  keyed by the inputs, and its id is printed on startup. The same inputs resume
  the same session, so queries and history come back, and nothing is ever
  deleted. An input the session records that has since gone missing is an
  error, not a silently skipped table. For a session in a file of its own,
  create it with `sqlnow exec <path>.sqlnow "SELECT 1"` and pass that file on
  the command line.
- Sessions that live in a file (a database sidecar, or a `.sqlnow` you name)
  are recorded in the store too — a pointer row holding the path, with the
  queries and history staying in that file. So one list covers every session,
  wherever it lives. `--no-register` leaves a run out of the list.
- `sqlnow --resume` lists sessions (most recent first, showing each one's own
  file or the inputs it was created for) and exits; `sqlnow --resume <n|id>`
  opens one by position or id and replays its inputs. Use it to find the
  session a user was last in without asking them for the path. A session whose
  file has moved is listed as `(missing)` and refuses to resume.
- Query names are the identity — renaming changes the URL. The `open` meta
  key follows renames and is cleared if its query is deleted.
- Old line-format `.sqlnow` files are auto-upgraded to the database format
  the first time they are used.
- The server has no authentication and its SQL can read/write host files.
  Leave it on the default loopback bind unless the user asks otherwise, and
  never bind it to a publicly reachable interface.
- `--only`/`--except` filter the table *listing* only (matched against the
  bare table name, not schema-qualified); the whole database is attached
  and hidden tables remain queryable by name. They are for focus, not
  access control.
