import { useMemo, useCallback, useState, useRef, useEffect } from 'react';
import PropTypes from 'prop-types';
import { useOutletContext, useNavigate } from "react-router-dom";
import { storageKey, fetchQuery, fetchQueries, updateQuery, deleteQueryApi, queryPath } from './utils';
import { editorTheme, gridTheme } from './theme';
import {
  buildFormatPlan, buildColumnConfig, cellFormat, cellSpec, rowHeight, DEFAULT_ROW_HEIGHT,
} from './cell-format';
// only the renderers the cell kinds actually use, so the barrel's editor-heavy
// cells (dropdown, article) stay out of the bundle where the bundler can manage it
import { RangeCell, SparklineCell, TagsCell } from '@glideapps/glide-data-grid-cells';
import { DataEditor, GridCellKind } from '@glideapps/glide-data-grid';
import "@glideapps/glide-data-grid/dist/index.css";
import { vim } from "@replit/codemirror-vim";
import { keymap } from '@codemirror/view';
import { Prec } from '@codemirror/state';

import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';

const ghostButton = "rounded border border-edge bg-transparent px-2 py-0.5 font-mono text-[11px] text-muted hover:border-edge-strong hover:text-ink";

// Stable across renders: glide rebuilds its renderer map whenever this changes.
const CUSTOM_RENDERERS = [RangeCell, SparklineCell, TagsCell];

function surroundWithQuotes(str) {
  return `"${str.replace('"', '""')}"`;
}

function generateSql(schema, sqlType) {
  if (sqlType == "select_star") {
    return "SELECT \n    * \nFROM\n    " + schema.db_name;
  } else if (sqlType == "select_fields") {
    return "SELECT \n" + schema.fields.map(([field]) => '    ' + surroundWithQuotes(field)).join(", \n") + "\nFROM\n    " + schema.db_name;
  } else if (sqlType == "select_fields_type") {
    return "SELECT \n" + schema.fields.map(([field, type]) => '    ' + surroundWithQuotes(field) + " -- " + type).join(", \n") + "\nFROM\n    " + schema.db_name;
  }
}


export default function QueryForm(props) {

  let queryType = props.queryType;
  let queryName = props.queryName;

  let { tables, sessionVersion, theme, vimEnabled } = useOutletContext();
  let navigate = useNavigate();

  let schema = undefined;
  let table = undefined;

  if (queryType == "table" && tables) {
    table = tables.tables.find((table) => table.name == queryName);
    if (table) {
      schema = table.schema;
    }
  }

  // undefined = loading, null = does not exist. The component is keyed by
  // query name, so each query tab fetches its own SQL fresh from the server.
  const [query, setQuery] = useState(undefined);

  // table tabs keep scratch SQL in localStorage; query tabs are server-backed
  let initialSql = '';
  if (queryType == "table") {
    initialSql = window.localStorage.getItem(storageKey(`sql-table-${queryName}`)) || '';
    if (!initialSql && schema) {
      initialSql = generateSql(schema, "select_fields");
    }
  }

  const [sql, setSql] = useState(initialSql);

  // On a direct page load the tables fetch may finish after this component
  // mounts, so the table's default SQL can't be derived in the initializer.
  useEffect(() => {
    if (queryType === "table" && schema && !sql) {
      setSql(generateSql(schema, "select_fields"));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [schema]);

  // the last SQL known to be on the server: the base for our own saves, and
  // the reference for applying live changes made by other writers
  const serverSqlRef = useRef("");

  useEffect(() => {
    if (queryType !== "query") {
      return;
    }
    let cancelled = false;
    fetchQuery(queryName)
      .then((fetched) => {
        if (!cancelled) {
          setQuery(fetched);
          serverSqlRef.current = fetched.sql;
          setSql(fetched.sql);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setQuery(null);
        }
      });
    return () => { cancelled = true; };
  }, [queryType, queryName]);

  // live updates: when the session changes elsewhere, reload this query's
  // SQL. The editor is only overwritten when there are no unsaved local
  // edits; any overwritten server version is preserved in history by the
  // server, so nothing is lost either way.
  useEffect(() => {
    if (queryType !== "query" || !sessionVersion) {
      return;
    }
    let cancelled = false;
    fetchQuery(queryName)
      .then((fetched) => {
        if (cancelled) {
          return;
        }
        setQuery(fetched);
        if (pendingSqlRef.current == null && fetched.sql !== serverSqlRef.current) {
          serverSqlRef.current = fetched.sql;
          setSql(fetched.sql);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setQuery(null);
        }
      });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionVersion]);

  let [columnWidths, setColumnWidths] = useState([])

  const [displayLimit, setDisplayLimit] = useState('500');

  const [results, setResults] = useState(null);

  // Which columns the grid shows and where each directive column points. Must be
  // declared above onColumnResize, which maps a visual index through it.
  const formatPlan = useMemo(
    () => (results ? buildFormatPlan(results.headers) : null),
    [results]
  );
  const columnConfigs = useMemo(
    () => (results && formatPlan ? buildColumnConfig(formatPlan, results.rows, theme) : []),
    [results, formatPlan, theme]
  );

  const [error, setError] = useState(null);
  const [running, setRunning] = useState(false);
  const [stats, setStats] = useState(null);

  const [nameDraft, setNameDraft] = useState(queryName);
  const [renameError, setRenameError] = useState(null);

  // debounced write-back of query SQL to the server
  const pendingSqlRef = useRef(null);
  const saveTimerRef = useRef();

  const flushSql = useCallback(async () => {
    clearTimeout(saveTimerRef.current);
    const value = pendingSqlRef.current;
    if (value === null || value === undefined) {
      return;
    }
    pendingSqlRef.current = null;
    try {
      // base_sql tells the server this is an incremental edit of the version
      // we last saw, so it does not archive every keystroke save to history
      await updateQuery(queryName, { sql: value, base_sql: serverSqlRef.current });
      serverSqlRef.current = value;
    } catch (e) {
      console.error("Failed to save query:", e);
    }
  }, [queryName]);

  useEffect(() => {
    return () => { flushSql(); };
  }, [flushSql]);

  function onSqlChange(value) {
    setSql(value);
    if (queryType === "table") {
      window.localStorage.setItem(storageKey(`sql-table-${queryName}`), value);
    } else {
      pendingSqlRef.current = value;
      clearTimeout(saveTimerRef.current);
      saveTimerRef.current = setTimeout(flushSql, 500);
    }
  }

  function setSqlFromSchema(sqlType) {
    let generated = generateSql(schema, sqlType);
    setSql(generated);
    window.localStorage.setItem(storageKey(`sql-table-${queryName}`), generated);
  }

  const onColumnResize = useCallback((column, newSize, colIndex) => {
    // colIndex is a visual index; columnWidths is keyed by data index, because
    // hiding a column must not shift the width of the ones after it
    const dataIndex = formatPlan ? formatPlan.visibleToData[colIndex] : colIndex;
    setColumnWidths(prevColumns => {
      let new_columns = [...prevColumns]
      new_columns[dataIndex] = newSize
      return new_columns;
    });
  }, [formatPlan]);

  async function runQuery() {
    if (running) {
      return;
    }

    if (queryType === "query") {
      await flushSql();
    }

    let formData = new URLSearchParams();
    formData.append('sql', sql);
    formData.append('display_limit', displayLimit);

    setRunning(true);
    let started = performance.now();

    try {
      let res = await fetch(location.origin + "/query.json",
        {
          method: "POST",
          body: formData,
        }
      )
      let resp = await res.json();
      setResults(resp.table_data);
      // a width the SQL asked for only seeds the column; a later drag overwrites
      // it and stands until the next run, so the grid never fights the mouse
      const plan = buildFormatPlan(resp.table_data.headers);
      const configs = buildColumnConfig(plan, resp.table_data.rows, theme);
      setColumnWidths(
        resp.table_data.headers.map((_, i) => configs[i]?.width || 150)
      );
      setError(resp.error);
      setStats(resp.error ? null : {
        rows: resp.table_data.rows.length,
        seconds: (performance.now() - started) / 1000,
      });
    } catch (e) {
      setResults(null);
      setError(String(e));
      setStats(null);
    } finally {
      setRunning(false);
    }
  }

  const runRef = useRef();
  runRef.current = runQuery;

  const extensions = useMemo(() => [
    Prec.highest(keymap.of([{
      key: 'Mod-Enter',
      run: () => { runRef.current(); return true; },
    }])),
    ...(vimEnabled ? [vim()] : []),
    langs.sql(),
  ], [vimEnabled]);

  let columns = useMemo(() => {
    if (!results || !formatPlan) {
      return [];
    }
    return formatPlan.visibleToData.map((dataIndex) => {
      const header = results.headers[dataIndex];
      return {
        "title": header,
        "id": header,
        "width": columnWidths[dataIndex] || 150
      }
    });

  }, [results, formatPlan, columnWidths]);

  const getCellContent = useCallback((cell) => {
    const [col, row] = cell;
    const rowData = results.rows[row];
    const dataCol = formatPlan.visibleToData[col];
    const value = rowData[dataCol];

    const config = columnConfigs[dataCol];
    const base = {
      kind: GridCellKind.Text,
      data: value,
      displayData: value,
      allowWrapping: config?.wrap === true,
      contentAlign: config?.align,
    };

    // a JSON spec says what the cell *is*, so it replaces the text cell rather
    // than decorating it, and it carries its own styling
    if (formatPlan.anyCell) {
      const json = formatPlan.cellFor[dataCol];
      if (json >= 0) {
        const spec = cellSpec(rowData[json], theme);
        if (spec !== null) {
          return { ...spec, contentAlign: spec.contentAlign ?? base.contentAlign };
        }
      }
    }

    if (!formatPlan.anyFormat) {
      return base;
    }
    const src = formatPlan.formatFor[dataCol];
    if (src < 0) {
      return base;
    }
    const style = cellFormat(rowData[src], theme);
    if (style === null) {
      return base;
    }
    // a cell's own alignment wins over the column's
    return {
      ...base,
      themeOverride: style.themeOverride,
      contentAlign: style.contentAlign ?? base.contentAlign,
    };
  }, [results, formatPlan, columnConfigs, theme]);

  // A constant keeps glide on its uniform-height path; the closure is only built
  // when the result asks for heights, and reads a precomputed array because glide
  // calls this per row per frame.
  const rowHeights = useMemo(() => {
    if (!results || !formatPlan || formatPlan.rowHeightAt < 0) {
      return DEFAULT_ROW_HEIGHT;
    }
    const src = formatPlan.rowHeightAt;
    const heights = new Int16Array(results.rows.length);
    for (let i = 0; i < results.rows.length; i++) {
      heights[i] = rowHeight(results.rows[i][src]) || DEFAULT_ROW_HEIGHT;
    }
    return (row) => heights[row] || DEFAULT_ROW_HEIGHT;
  }, [results, formatPlan]);

  async function commitRename() {
    const newName = nameDraft.trim();
    if (!query || newName === query.name) {
      setNameDraft(queryName);
      setRenameError(null);
      return;
    }
    try {
      await flushSql();
      const updated = await updateQuery(queryName, { name: newName });
      navigate(queryPath(updated.name), { replace: true });
    } catch (e) {
      setRenameError(e.message);
      setNameDraft(queryName);
    }
  }

  async function deleteQuery() {
    pendingSqlRef.current = null;
    clearTimeout(saveTimerRef.current);
    try {
      await deleteQueryApi(queryName);
      const data = await fetchQueries();
      navigate(data.queries.length ? queryPath(data.queries[0].name) : "/");
    } catch (e) {
      console.error("Failed to delete query:", e);
    }
  }

  if (queryType === "query" && query === null) {
    return (
      <main role="main" className="flex min-w-0 flex-1 items-center justify-center bg-bg">
        <p className="font-mono text-xs text-dim">Query “{queryName}” does not exist</p>
      </main>
    );
  }

  let limitNumber = parseInt(displayLimit, 10);
  let limitReached = stats && !isNaN(limitNumber) && stats.rows >= limitNumber;

  return (
    <main role="main" className="flex min-w-0 flex-1 flex-col bg-bg">
      <header className="flex h-12 shrink-0 items-center justify-between gap-3 border-b border-edge bg-surface px-4">
        <div className="flex min-w-0 items-center gap-2">
          {queryType === "table" && schema &&
            <>
              <span className="truncate font-mono text-[13px] font-medium">{schema.name}</span>
              <span className="mx-1 h-4 w-px shrink-0 bg-edge" aria-hidden="true"></span>
              <button className={ghostButton} onClick={() => setSqlFromSchema("select_star")}>select *</button>
              <button className={ghostButton} onClick={() => setSqlFromSchema("select_fields")}>columns</button>
              <button className={ghostButton} onClick={() => setSqlFromSchema("select_fields_type")}>columns + types</button>
            </>
          }
          {(queryType === "query" && query) &&
            <>
              <input
                className="w-64 truncate rounded border border-transparent bg-transparent px-2 py-1 text-[13px] font-medium hover:border-edge focus:border-edge-strong focus:bg-bg focus:outline-none"
                id="name" name="name" value={nameDraft}
                onChange={(e) => { setNameDraft(e.target.value); setRenameError(null); }}
                onBlur={commitRename}
                onKeyDown={(e) => { if (e.key === "Enter") { e.target.blur(); } }}
                aria-label="Query name"
              />
              {renameError &&
                <span className="truncate font-mono text-[11px] text-danger">{renameError}</span>
              }
              <button
                className="rounded px-2 py-1 text-xs text-dim hover:bg-raised hover:text-danger"
                onClick={deleteQuery}
              >
                Delete
              </button>
            </>
          }
        </div>
        <label className="flex shrink-0 items-center gap-2 text-xs text-muted">
          Limit
          <input
            className="w-16 rounded border border-edge bg-transparent px-2 py-1 text-right font-mono text-xs focus:border-edge-strong focus:outline-none"
            id="display_limit" name="display_limit" value={displayLimit}
            onChange={(e) => setDisplayLimit(e.target.value)}
          />
        </label>
      </header>

      <div className="shrink-0 border-b border-edge">
        <CodeMirror
          value={sql}
          height="280px"
          theme={editorTheme(theme)}
          extensions={extensions}
          onChange={onSqlChange}
        />
      </div>

      <form
        autoComplete="off" method="post" action="/outputs"
        className="flex h-11 shrink-0 items-center justify-between border-b border-edge bg-surface px-4"
      >
        <input type="hidden" name="sql" value={sql} />
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={runQuery}
            disabled={running}
            className="rounded bg-accent px-4 py-1.5 font-mono text-xs font-semibold text-accent-ink hover:opacity-90 disabled:opacity-60"
          >
            {running ? "Running…" : "Run"}
          </button>
          <span className="font-mono text-[11px] text-dim" title="Ctrl+Enter (or Cmd+Enter) runs the query">
            ctrl+↵
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="mr-1 font-mono text-[10px] uppercase tracking-[0.14em] text-dim">Export</span>
          <button className={ghostButton} type="submit" name="csv" value="CSV">csv</button>
          <button className={ghostButton} type="submit" name="tab" value="Tab delimited">tsv</button>
          <button className={ghostButton} type="submit" name="jsonl" value="JSONL Stream">jsonl</button>
        </div>
      </form>

      <div className="min-h-0 flex-1">
        {error &&
          <div className="h-full overflow-auto p-4">
            <pre className="whitespace-pre-wrap font-mono text-xs leading-5 text-danger">{error}</pre>
          </div>
        }
        {!error && results &&
          <DataEditor
            getCellContent={getCellContent}
            getCellsForSelection={true}
            keybindings={{ search: true }}
            columns={columns}
            rows={results.rows.length}
            width="100%"
            height="100%"
            rowHeight={rowHeights}
            headerHeight={28}
            // glide's own floor, rather than its 50px default: a `width:` a query
            // asked for should be honoured, and 50 is wide enough to make narrow
            // columns impossible
            minColumnWidth={20}
            onColumnResize={onColumnResize}
            customRenderers={CUSTOM_RENDERERS}
            theme={gridTheme(theme)}
          />
        }
        {!error && !results &&
          <div className="flex h-full items-center justify-center">
            <p className="font-mono text-xs text-dim">No results yet — press Run to execute the query</p>
          </div>
        }
      </div>

      <footer className="flex h-7 shrink-0 items-center justify-between border-t border-edge bg-surface px-4 font-mono text-[11px] text-muted">
        <div className="flex items-center gap-2">
          {running &&
            <><span className="text-accent">●</span><span>running…</span></>
          }
          {!running && error &&
            <><span className="text-danger">✕</span><span className="text-danger">error</span></>
          }
          {!running && !error && stats &&
            <>
              <span className="text-ok">●</span>
              <span>
                {stats.rows.toLocaleString()} {stats.rows === 1 ? "row" : "rows"} · {stats.seconds.toFixed(2)}s
                {limitReached && <span className="text-dim"> · limit reached</span>}
              </span>
            </>
          }
          {!running && !error && !stats &&
            <><span className="text-dim">●</span><span>ready</span></>
          }
        </div>
        <div className="text-dim">
          limit {displayLimit || "500"}{vimEnabled && " · vim"}
        </div>
      </footer>
    </main>
  );
}

QueryForm.propTypes = {
  queryType: PropTypes.string,
  queryName: PropTypes.string,
};
