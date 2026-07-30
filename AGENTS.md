# sqlnow for agents

sqlnow turns data files and databases into a browser UI, with queries you
define up front. This is the integration reference for launching sessions,
changing them, styling results and reading back what the user did. It ships
inside the binary as `sqlnow --agents-help` and does not teach DuckDB.

1. **Launching** — inputs, queries, URLs and process lifetime.
2. **HTTP API** — the live channel for queries, inputs and results.
3. **`sqlnow sql`** — database work without the UI.
4. **Session files** — preparing a session for someone else.
5. **Reading back** — queries and history.
6. **Styling** — colours, widths, bars, sparklines and other cells.
7. **Constraints** — persistence, deletion, binding and table filters.

## 1. Launching

One command can attach all inputs, save several queries and select the landing
query:

```bash
sqlnow sales.csv customers.sqlite \
  -q "by region=SELECT region, sum(total) FROM sales GROUP BY 1" \
  -q "recent=SELECT * FROM sales ORDER BY at DESC LIMIT 50" \
  --open "by region"
```

`-q` is repeatable; bare SQL gets an automatic name. For long SQL use
`--query-file report.sql` or `--query-file "join check=checks/join.sql"`.

Inputs may be positional or passed with `-v` (view) or `-t` (table): parquet,
csv, xlsx, json/jsonl, sqlite or DuckDB files, PostgreSQL URIs and s3/http
URLs. Each file is queryable by its stem; sqlite/DuckDB tables appear below
that name (`data.some_table`). A DuckDB file in the first position is the main
database, so tables created there persist.

**Use `--as` whenever a name or value could be ambiguous.** It names the input
or query immediately before it and treats the value literally:

```bash
sqlnow -v 'postgresql://localhost/db?sslmode=disable' --as gem \
       -v app.sqlite --as "legacy db" --only 'entity_.*' --except entity_log \
       -q 'SELECT * FROM t WHERE a=1' --as 'top rows'
```

The `name=target` and `name=SQL` shorthands remain convenient for simple
values (`-v gem=postgresql://...`, `-q "top=SELECT 1"`).

Startup prints the server URL and a deep link. Pass the deep link to the user:

```
Server running on http://127.0.0.1:8080
Open query "top rows": http://127.0.0.1:8080/queries/top%20rows
```

`--open <name>` launches that query on the **user's desktop**, even from a
sandbox or background shell. Use it only for the final server that will stay
alive, never for verification. If your process cannot outlive the task,
prepare a session file and give the user a launch command containing `--open`.
`--port`/`--host` and the `PORT`/`HOST` environment variables control binding.

In an agent harness, use its background-process mechanism. `nohup ... &` or
`disown` inside a sandboxed shell may die with that shell. Startup is complete
only when `Server running on ...` prints; stopping earlier can leave inputs
unrecorded.

Sessions persist automatically. Re-running with the same inputs resumes the
same queries and history; `sqlnow --resume` finds recent sessions. Recorded
inputs replay on later launches and in `sqlnow sql`, so do not pass `-v`/`-t`
again.

For a fresh session, prefer one launch command over creating a database and
seeding it with `exec`. Use `exec` when preparing an existing session file.
Before presenting a comparison or judgement, read §6: results can carry their
own colours, widths and richer cells.

## 2. HTTP API

While a server runs, use HTTP for reads and live changes. It shares the user's
session and inputs, avoids file-lock contention, and updates the UI within
about a second.

### Session and queries

| method and path | purpose |
|---|---|
| `GET /api/session` | liveness and the served session id, path and open query |
| `GET /api/queries` | list saved queries and the open query |
| `POST /api/queries` | create a query; name may be omitted |
| `GET /api/queries/<name>` | read one query |
| `PUT /api/queries/<name>` | replace SQL or rename |
| `DELETE /api/queries/<name>` | delete it; SQL remains in history |
| `GET /api/history?limit=50` | newest run history; omit limit or use 0 for all |
| `GET /api/events` | SSE stream; emits `data: changed` on session changes |

```bash
curl -s localhost:8080/api/session
curl -s -X POST localhost:8080/api/queries \
  -H 'content-type: application/json' \
  -d '{"name":"errors by day","sql":"SELECT day, count(*) FROM errors GROUP BY 1"}'
curl -s -X PUT localhost:8080/api/queries/errors%20by%20day \
  -H 'content-type: application/json' -d '{"sql":"SELECT 1"}'
```

Creating an existing name returns 409. A PUT can instead rename with
`{"name":"errors"}`. Replacing an open query is safe: without `base_sql`, PUT
archives the previous SQL before replacing it.

Query names are unique, case-sensitive, at most 100 characters, and cannot
contain `/`. Percent-encode names in URLs. Their deep links are
`/queries/<percent-encoded name>`; renaming changes the URL. The open query
follows a rename and is cleared when deleted.

### Inputs

Attach data without relaunching; the sidebar and recorded replay inputs update
within about a second:

```bash
curl -s localhost:8080/api/inputs

# "view" queries the file in place (default); "table" reads it in once
# "as" defaults to the file stem
curl -s -X POST localhost:8080/api/inputs \
  -H 'content-type: application/json' \
  -d '{"uri":"/data/plants.parquet"}'

# databases accept the same listing filters as --only/--except
curl -s -X POST localhost:8080/api/inputs \
  -H 'content-type: application/json' \
  -d '{"uri":"postgresql://localhost/db","as":"pg","only":["entity_.*"]}'

curl -s -X DELETE localhost:8080/api/inputs/plants
```

Missing paths are rejected, and an existing name returns 400; detach it before
replacing it. **Deleting a file input drops its view or table. With a main
database, that can permanently remove the table from the file.** Deleting a
database input only detaches the database.

### Running SQL

| route | writes | history | limit | errors |
|---|---|---|---|---|
| `POST /outputs` | no | no | optional `limit=N` | HTTP 400 |
| `POST /query.json` | no | yes; nudges UI | `display_limit`, default 500 | HTTP 200 with `error` |
| `sqlnow sql` | yes | no | optional `--limit N` | non-zero exit |

The server's databases are read-only. Use `sqlnow sql` when there is no server,
or for `CREATE`, `INSERT`, `COPY` and other writes; `POST /api/inputs` is the
only HTTP operation that temporarily escalates write access.

For your own reads, prefer CSV from `/outputs`; it stays out of history. Use
`/query.json` when the user should be able to retrace the query:

```bash
# form-encoded, not JSON
curl -s -d 'sql=SELECT count(*) FROM data' \
  -d 'display_limit=500' localhost:8080/query.json

# csv, tab and jsonl are selected by field name
curl -s -d 'sql=SELECT * FROM data' -d 'csv=1' localhost:8080/outputs
```

**Always check for truncation.** A result of exactly N rows may be a page of a
much larger result:

- `/query.json`: inspect `table_data.truncated`; `limit` reports the applied
  limit, which defaults to 500.
- `/outputs` with `limit=N`: inspect `X-Sqlnow-Truncated` and `X-Sqlnow-Rows`.
  Without a limit it streams the entire result and sends neither header.
- `sqlnow sql --limit N`: inspect the box footer, or stderr for csv/json/jsonl.

These report whether more rows exist, not how many. Limits are applied inside
the query; a query with its own `LIMIT` is left alone.

## 3. Querying the database: `sqlnow sql`

`sqlnow sql` uses sqlnow's embedded DuckDB engine for reads and writes outside
the UI. Point it at a main database or a `.sqlnow` session file; recorded
inputs replay so names resolve as they do in the UI. File views remain
temporary and are not written into the session file.

```bash
sqlnow sql data.duckdb "SELECT count(*) FROM sales"          # box table
sqlnow sql data.duckdb -f csv "SELECT * FROM sales"          # also json/jsonl
sqlnow sql data.duckdb --limit 100 "SELECT * FROM big_table"
sqlnow sql data.duckdb "CREATE TABLE summary AS SELECT region, sum(total) FROM sales GROUP BY 1"
```

Writes persist. JSON and JSONL values are strings, including DuckDB containers;
unnest or cast containers in SQL if you need their parts separately. SQL may
begin with a `--` comment; no `--` separator is needed.

For a one-off question without an existing database, create a scratch session:

```bash
sqlnow exec scratch.sqlnow "SELECT 1"
sqlnow sql scratch.sqlnow "SELECT count(*) FROM read_parquet('data.parquet')"
```

This can run alongside a server because the server holds no connection between
requests. A concurrent operation can still briefly encounter a file lock.

## 4. Session files and `sqlnow exec`

A `.sqlnow` session file is a DuckDB database with this format-2 schema:

```sql
format(version INTEGER)
sessions(id TEXT PRIMARY KEY, key TEXT, path TEXT,
         last_used TIMESTAMP, changed_at TIMESTAMP)
meta(session TEXT, key TEXT, value TEXT, PRIMARY KEY (session, key))
queries(session TEXT, pos INTEGER, name TEXT, sql TEXT, PRIMARY KEY (session, name))
history(session TEXT, "at" TIMESTAMP DEFAULT now(), sql TEXT)
inputs(session TEXT, kind TEXT, name TEXT, uri TEXT, tables TEXT[], except_tables TEXT[])
```

A database can contain many sessions, each scoped by the `session` column. A
named session file normally contains one; the central store contains many.
Scope writes by session id. In a single-session file, use
`(SELECT id FROM sessions)`.

`sqlnow exec` creates a missing session file, then runs SQL against its session
database. It also works on the multi-session store and refuses an existing
non-session database:

```bash
sqlnow exec session.sqlnow "INSERT INTO queries(session, pos, name, sql)
  SELECT id, 1, 'top emitters', 'SELECT name, co2 FROM plants ORDER BY co2 DESC LIMIT 50'
  FROM sessions"
sqlnow exec session.sqlnow "INSERT INTO meta(session, key, value)
  SELECT id, 'open', 'top emitters' FROM sessions"
sqlnow session.sqlnow plants.parquet
```

Results are CSV with a header. Multi-statement input is allowed and returns no
rows. For embedded SQL with quotes, use DuckDB dollar-quoting:

```bash
sqlnow exec session.sqlnow "INSERT INTO queries(session, pos, name, sql)
  SELECT id, 1, 'names', \$q\$SELECT name FROM t WHERE note = 'it''s fine'\$q\$ FROM sessions"
```

Session files are not held open between operations, so `exec` normally works
alongside a server, and its changes reach the UI like any other. Prefer the HTTP
API for live changes only because it cannot hit a concurrent-operation lock.

## 5. Reading back what the user did

```bash
# while running
curl -s "localhost:8080/api/history"

# after the server stops
sqlnow exec session.sqlnow "SELECT \"at\", sql FROM history ORDER BY \"at\" DESC"
sqlnow exec session.sqlnow "SELECT name, sql FROM queries ORDER BY pos"
```

History is uncapped and deduplicated: rerunning identical SQL refreshes its
timestamp. It records successful and failed queries.

## 6. Styling what the user sees

Companion columns style a result and are hidden by the grid. Styling lives in
the SQL, not configuration, so it travels with the query.

| column | applies to | read from | example |
|---|---|---|---|
| `_sqlnow_format_<col>` | each cell of `<col>` | every row | `warn`, `heat:0.7` |
| `_sqlnow_cell_<col>` | each cell of `<col>` | every row | `{"kind":"bar","value":0.7}` |
| `_sqlnow_column_<col>` | the column `<col>` | first row | `width:420; wrap` |
| `_sqlnow_row_height` | each row | every row | `56` |

```sql
SELECT p.name,
       p.notes,
       p.co2,
       CASE WHEN p.co2 IS DISTINCT FROM old.co2 THEN 'changed' END
         AS _sqlnow_format_name,
       (p.co2 - min(p.co2) OVER ()) /
         nullif(max(p.co2) OVER () - min(p.co2) OVER (), 0)
         AS _sqlnow_format_co2,
       'width:420; wrap' AS _sqlnow_column_notes,
       56 AS _sqlnow_row_height
FROM plants p LEFT JOIN plants_last_week old USING (name)
```

A style is one word or a `;`-separated set of declarations:

| value | meaning |
|---|---|
| `ok` `added` `warn` `changed` `error` `removed` `muted` | named background |
| `0.73` or `heat:0.73` | position on a heatmap |
| `div:-0.4` | signed ramp; zero is neutral |
| `#2d5016`, `rgba(...)`, `oklch(...)` | raw background colour |
| `bg:` `fg:` `bold` `italic` `align:left\|right\|center` | individual properties |
| `width:<px>` `wrap` | column properties |

Text colour is chosen for contrast. Prefer named colours and ramps because
they adapt to the theme.

### Cells that are not text

`_sqlnow_cell_<col>` takes JSON whose `kind` selects a richer cell.
`to_json` over a DuckDB struct is the easiest way to build it:

| kind | payload | display |
|---|---|---|
| `bar` | `value`, optional `min`/`max` (default 0–1), `label` | filled bar |
| `sparkline` | `values`, `graph`=`line`\|`bar`\|`area`, `color` | series shape |
| `tags` | `tags` | coloured pills |
| `link` | `href`, `text` | clickable link |
| `bool` | `value` | checkbox |
| `bubble` | `tags` | plain pills |

```sql
SELECT p.name,
       '' AS capacity,
       '' AS units,
       to_json({'kind': 'bar', 'value': mw / max(mw) OVER (),
                'label': round(mw / 1000, 1)::text || ' GW'})
         AS _sqlnow_cell_capacity,
       to_json({'kind': 'sparkline', 'values': unit_mw, 'graph': 'bar'})
         AS _sqlnow_cell_units
FROM plants p
```

Cell JSON also accepts `bg`, `fg`, `bold`, `italic` and `align`. Cells are
read-only. Unknown kinds or malformed JSON fall back to the cell's text.

The `_sqlnow_` prefix is reserved. Its columns are hidden from the grid and
all exports, including csv, tab, jsonl and `sqlnow sql`.

Three pitfalls matter:

1. `<col>` must exactly match the displayed column. DuckDB lowercases unquoted
   aliases; quote the companion alias when the displayed name has capitals.
2. `wrap` also needs `_sqlnow_row_height` to provide space for extra lines.
3. Invalid styles are ignored, so verify the rendered cell rather than waiting
   for an error.

## 7. Constraints

- One server per session file. While live, it is the writer of record; direct
  `exec` changes can race it.
- Unanchored runs live in `<config dir>/sqlnow/sessions.sqlnow`, keyed by their
  inputs. Named `.sqlnow` files and database sidecars are registered there as
  pointers; their content stays in their own files. `--no-register` omits the
  pointer.
- `sqlnow --resume` lists recent sessions and exits;
  `sqlnow --resume <n|id>` opens one and replays its inputs. It marks served
  sessions `live` with their address, which is how to find an unknown port. A
  live session cannot be opened twice. Missing files are shown as `(missing)`
  and cannot resume; missing recorded inputs are errors.
- `sqlnow delete <n|id>...` permanently deletes the selected sessions,
  including queries, history, inputs and metadata, but not data files. It
  refuses live sessions and requires `--yes` without a terminal. **Never run
  it unless the user explicitly asks to delete those sessions.**
- Old line-format and format-1 session files migrate automatically. A session
  file with a newer unsupported format is refused.
- The server has no authentication and its SQL can read and write host files.
  Keep it on the default loopback address unless the user explicitly asks
  otherwise; never bind it publicly.
- `--only`/`--except` filter the displayed table list by bare table name. The
  whole database remains attached and hidden tables remain queryable; these
  flags are for focus, not access control.
