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

## Build

```
cd ui && npm install && npm run build && cd ..   # embeds the UI
cargo build --release                            # target/release/sqlnow
```

## Inputs

Name an input or query with `--as` (applies to the value immediately before
it, taken literally — safe for URIs, paths, and SQL containing any
characters); filter database tables with `--tables t1,t2`:

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

## Sessions

Queries and run history live in a **session file** (`.sqlnow`), which is
itself a small DuckDB database (tables: `meta`, `queries`, `history`,
`inputs`). The server reads and writes it directly — there is no other state.
Where it lives:

1. With a main database `data.duckdb`, the session is `data.duckdb.sqlnow`
   next to it (only database attaches are recorded there; file tables persist
   in the main db itself).
2. Otherwise `--save name` anchors the session at `name.sqlnow` and records
   every input, so `sqlnow name.sqlnow` replays the whole setup — queries and
   history included.
3. `sqlnow session.sqlnow` (a session file as the first `.sqlnow` argument)
   continues that session: new queries and history are written back to it.
4. With none of the above, the session is in-memory: everything works, but
   nothing survives the process.

Inspect or edit a session with the built-in runner (no duckdb install
needed): `sqlnow exec session.sqlnow "SELECT * FROM queries"`.

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
(default 1). The UI talks to a JSON API (`/api/queries`,
`/api/history`) documented in [AGENTS.md](AGENTS.md); exports stream from
`POST /outputs` as CSV/TSV/JSONL.
