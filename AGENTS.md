# sqlnow for agents

sqlnow turns data files and databases into a browser UI for a human, with
queries you define up front. This document is the integration reference:
how to launch sessions, seed queries, style the results, point the user at the
right place, and read back what they did.

This guide ships inside the binary: `sqlnow --agents-help` prints it, so it
is always available (and always matches the binary's version) even without
this repository.

It only covers what you cannot work out from the SQL: nothing here teaches
DuckDB. Skip to what you need —

1. **Launching** — the one command that does it, naming, relaying the URL.
2. **HTTP API** — changing a running session; attaching data to it.
3. **`sqlnow sql`** — answering a question with no server and no UI.
4. **Session files** — preparing a session for someone else to open.
5. **Reading back** — what the user ran after you handed it over.
6. **Styling** — colour a cell, a bar or sparkline in it, column widths. This
   is how a diff reads as a diff, a check reads as pass/fail, and a number
   reads as big or small. You cannot guess these column names; read the
   section before showing a comparison or a judgement.
7. **Constraints** — one server per session, row caps, what not to run.

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
- **A result can carry its own colours and widths** (§6). Worth a look before
  writing a query whose point is a comparison or a verdict rather than a list.

## 2. HTTP API (live channel, always safe)

While the server runs, this is the reliable way to add or change queries —
the UI picks changes up on its next navigation or reload.

```bash
# which session this server is serving (also a liveness check)
curl -s localhost:8080/api/session
# -> {"id":"81b95136...","open":"top rows","path":"/home/u/data.duckdb.sqlnow"}

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

**`/query.json` cannot write.** The server holds its main database, and every
database it attaches, read-only — so DDL or DML sent there, or typed by the user
in the editor, comes back as
`Cannot execute statement of type "CREATE" … read-only mode`. `POST /api/inputs`
is the one path that escalates to write access; for anything else use
`sqlnow sql`, which works alongside a running server.

**Live updates**: the UI subscribes to `/api/events`, so queries you add or
update on a running server appear in the user's browser within about a
second — including the query they are currently looking at. Updating an open
query is safe: the previous SQL is preserved in history before being
overwritten (a PUT without `base_sql` always archives what it replaces), so
feel free to push improved versions of a query while the user watches.

Query names are identities: unique, case-sensitive, no `/`, max 100 chars.
Percent-encode them in URLs. The UI deep link for a query is
`/queries/<percent-encoded name>`.

### Getting results: which route to use

Three routes run SQL. All three are fast enough that speed is not the
question — choose on what each one can do:

| | writes | lands in history | row limit | errors |
|---|---|---|---|---|
| `POST /outputs` | no | no | `limit=N` (optional) | 400 with the message |
| `POST /query.json` | no | **yes**, and nudges the UI | `display_limit`, **500 by default** | 200 with `error` in the body |
| `sqlnow sql` | **yes** | no | `--limit N` (optional) | non-zero exit, message on stderr |

The rules that follow from that:

1. **Server running?** Use HTTP: you share the user's session and its
   attaches, and never contend for the file lock. **No server?** Use
   `sqlnow sql`.
2. **Writing anything** — `CREATE TABLE`, `INSERT`, `COPY` — only
   `sqlnow sql` can. The server's connection is read-only on purpose; the
   one exception is attaching data, which is `POST /api/inputs`.
3. **Reading for yourself** — `POST /outputs` with `csv=1`. It is the
   cheapest output there is and it stays out of the user's history.
4. **Reading for the user** — `POST /query.json`, *because* it records the
   query and pokes the UI. That is how they retrace what you did.

**Prefer csv.** On the same 200-row × 3-column result: csv and tab 3.7 kB,
`query.json` 5.4 kB, the box table 7.5 kB, json and jsonl 8.9 kB. The box
table is for humans only.

```bash
# form-encoded, not JSON — the same endpoint the UI uses
curl -s -d 'sql=SELECT count(*) FROM data' -d 'display_limit=500' localhost:8080/query.json
# -> {"error":null,"limit":500,"table_data":{"headers":[...],"rows":[[...]],"truncated":false}}

# exports: the field name picks the format (csv | tab | jsonl)
curl -s -d 'sql=SELECT * FROM data' -d 'csv=1' localhost:8080/outputs > out.csv

# with a limit, the reply says exactly what you got
curl -si -d 'sql=SELECT * FROM data' -d 'csv=1' -d 'limit=1000' localhost:8080/outputs
# -> X-Sqlnow-Rows: 1000
#    X-Sqlnow-Truncated: true
```

**Always check for truncation.** A result of exactly N rows is either the
whole answer or the first page of a million, and the difference is invisible
unless you look:

- `query.json` — `table_data.truncated`, with the limit that produced it in
  `limit`. It defaults to **500 rows** even when you do not ask for a limit.
- `/outputs` with `limit=N` — `X-Sqlnow-Truncated` and `X-Sqlnow-Rows`
  headers. Without `limit` it streams the whole result and sends no such
  headers.
- `sqlnow sql --limit N` — `(N rows, truncated — there are more)` in the box
  footer, or a note on **stderr** for csv/json/jsonl so stdout stays parseable.

You are told whether there was more, never how much more.

The limit goes into the query rather than being applied afterwards, so 500 rows
of a 20M-row table costs about 20ms — asking for a limit is always cheaper than
not. A query that carries its own `LIMIT` is left alone.

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
same stringification the UI grid uses) — including containers, which render
the way duckdb prints them: a `LIST` as `[1, 10]`, a `STRUCT` as `{a: 1}`, a
`MAP` as `{k: 1}`. If you need the parts separately, unnest or cast in SQL
(`array_to_string(lst, ',')`, `to_json(st)`) rather than parsing that text.
SQL may begin with a `--` comment; no `--` separator is needed.

This works even while a server has the database open, because the server holds
no connection between requests — though a concurrent operation can occasionally
hit a lock (brief automatic retry, then a clear error). The HTTP API never
does, so prefer it for live changes.

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

`sqlnow exec` is `sqlnow sql` for session databases. It creates the file (with
the schema and one session) if missing, so you can seed before the first launch,
and it works on the multi-session store too. It refuses to touch an existing
database that is not a session file (use `sqlnow sql` for those):

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

Unlike `sqlnow sql`, this never contends with a running server: session files are
never held open. The HTTP API is still preferable for live changes, because the
UI hears about those within a second.

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

## 6. Styling what the user sees

Colour, width and row height come from the result itself: name a companion
column after the column it describes and the grid uses it and hides it. There
is nothing to configure and nothing stored — it is in the SQL, so it travels
with the query and you can add it with a `PUT`.

| column | applies to | read from | example |
|---|---|---|---|
| `_sqlnow_format_<col>` | each cell of `<col>` | every row | `warn`, `heat:0.7` |
| `_sqlnow_cell_<col>` | each cell of `<col>` | every row | `{"kind":"bar","value":0.7}` |
| `_sqlnow_column_<col>` | the column `<col>` | the first row | `width:420; wrap` |
| `_sqlnow_row_height` | each row | every row | `56` |

```sql
SELECT p.name,
       p.notes,
       p.co2,
       -- what changed since last week, and how big this number is
       CASE WHEN p.co2 IS DISTINCT FROM old.co2 THEN 'changed' END AS _sqlnow_format_name,
       (p.co2 - min(p.co2) OVER ()) /
         nullif(max(p.co2) OVER () - min(p.co2) OVER (), 0) AS _sqlnow_format_co2,
       'width:420; wrap' AS _sqlnow_column_notes,
       56 AS _sqlnow_row_height
FROM plants p LEFT JOIN plants_last_week old USING (name)
```

A style is one word, or `;`-separated declarations:

| value | means |
|---|---|
| `ok` `added` `warn` `changed` `error` `removed` `muted` | a named background |
| `0.73` | position on a light-to-dark ramp — a heatmap |
| `#2d5016`, `rgba(255,0,0,.15)`, `oklch(.7 .15 20)` | that colour as the background |
| `heat:0.73` / `div:-0.4` | the ramp explicitly; `div` is signed, 0 neutral |
| `bg:` `fg:` `bold` `italic` `align:left\|right\|center` | set one thing at a time |
| `width:<px>` `wrap` | column only |

Text colour is chosen for contrast against whatever background you set, so a
raw colour stays readable in both themes. Prefer the names and the ramps: they
are picked per theme, a raw colour is not.

### Cells that are not text

A string styles a cell; **JSON says what the cell is**. `_sqlnow_cell_<col>` takes
an object whose `kind` picks one of these, and `to_json` over a struct is the
easiest way to build it:

| kind | payload | shows |
|---|---|---|
| `bar` | `value`, and `min`/`max` (default 0–1), `label` | a filled bar in the cell |
| `sparkline` | `values` (a list), `graph` = `line`\|`bar`\|`area`, `color` | the shape of a series |
| `tags` | `tags` (a list) | coloured pills, one per tag |
| `link` | `href`, `text` | a clickable link |
| `bool` | `value` | a checkbox |
| `bubble` | `tags` (a list) | plain pills, no colour |

```sql
SELECT p.name,
       '' AS capacity,          -- the cell's own value is unused when a kind is set
       '' AS units,
       to_json({'kind': 'bar', 'value': mw / max(mw) OVER (),
                'label': round(mw / 1000, 1)::text || ' GW'})   AS _sqlnow_cell_capacity,
       to_json({'kind': 'sparkline', 'values': unit_mw,
                'graph': 'bar'})                                AS _sqlnow_cell_units
FROM plants p
```

The styling keys work here too — `bg`, `fg`, `bold`, `italic`, `align` — so one
column can be both a kind and a colour: `{"kind":"bar","value":0.4,"bg":"warn"}`.
Cells are read-only; nothing in the grid edits.

An unknown or missing `kind`, or malformed JSON, falls back to the column's own
text — which is how a newer kind degrades on an older binary.

The prefix `_sqlnow_` is reserved. Any column using it is an instruction to the
viewer rather than data, so it is hidden from the grid **and from every export**
— csv, tab, jsonl and `sqlnow sql` alike:

```bash
# what the user sees is styled; what they download is only the data
curl -s -d "sql=SELECT 1 AS co2, 'warn' AS _sqlnow_format_co2" -d csv=1 localhost:8080/outputs
# -> co2
#    1
```

Three things to get right. `<col>` must match the displayed column's name, and
DuckDB lowercases unquoted aliases (`AS _sqlnow_format_CO2` names the column
`_sqlnow_format_co2`, which finds `co2`) — quote the alias if the column has
capitals. `wrap` needs `_sqlnow_row_height` too, or the extra lines have
nowhere to go. And an unknown name or an unusable value is ignored rather than
refused, so a style you got slightly wrong shows as plain text — check the
cell, not for an error.

## 7. Constraints

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
- **A session already being served is marked `live`, with its address, and
  cannot be opened a second time** — launching onto it fails and names the
  running server instead. That is the one to talk to: use its HTTP API rather
  than starting your own. Addresses are pinged (`GET /api/session`, which
  answers with the session id), so a killed server blocks nothing.
- `sqlnow --resume` is also how to find a server a user already has open when
  you were not told the port.
- `sqlnow delete <n|id>...` deletes sessions and everything recorded under
  them: queries, history, inputs and metadata, from the store and from a
  session's own file. Data files are never touched, a live session is refused,
  and every argument is resolved before anything goes. **It needs `--yes` when
  there is no terminal, which is always the case for you — so it cannot happen
  by accident, and you must not run it unless the user asked for that session
  to be deleted.** There is no undo, and the history is usually the work.
- Query names are the identity — renaming changes the URL. The `open` meta
  key follows renames and is cleared if its query is deleted.
- Results are capped: `/query.json` at 500 rows unless told otherwise. Never
  conclude anything from a row count without checking `truncated` (or the
  `X-Sqlnow-Truncated` header, or the CLI's note on stderr).
- Old line-format `.sqlnow` files are auto-upgraded to the database format
  the first time they are used.
- The server has no authentication and its SQL can read/write host files.
  Leave it on the default loopback bind unless the user asks otherwise, and
  never bind it to a publicly reachable interface.
- `--only`/`--except` filter the table *listing* only (matched against the
  bare table name, not schema-qualified); the whole database is attached
  and hidden tables remain queryable by name. They are for focus, not
  access control.
