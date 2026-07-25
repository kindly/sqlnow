import { createQuery, fetchQueries, queryPath } from "../utils";
import { applyTheme, initialTheme, applyVim, initialVim } from "../theme";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { useEffect, useRef, useState } from "react";
import PropTypes from "prop-types";

const itemBase = "block truncate rounded px-2 py-0.5";

function navItemClass({ isActive }) {
  return isActive
    ? `${itemBase} bg-raised text-accent`
    : `${itemBase} text-muted hover:bg-raised hover:text-ink`;
}

function groupLabelClass(extra = "") {
  return `flex items-center justify-between px-2 pt-3 pb-0.5 font-mono text-[10px] font-medium uppercase tracking-[0.14em] text-dim ${extra}`;
}

function GearIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09a1.65 1.65 0 0 0 1.51-1 1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33h.01a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51h.01a1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82v.01a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </svg>
  );
}

function SegmentedControl({ label, options, value, onChange }) {
  return (
    <div>
      <div className="font-mono text-[10px] font-medium uppercase tracking-[0.14em] text-dim">{label}</div>
      <div className="mt-1 flex gap-1">
        {options.map((option) => (
          <button
            key={option.value}
            onClick={() => onChange(option.value)}
            className={
              option.value === value
                ? "flex-1 rounded border border-edge-strong bg-raised px-2 py-1 font-mono text-[11px] text-ink"
                : "flex-1 rounded border border-edge px-2 py-1 font-mono text-[11px] text-muted hover:text-ink"
            }
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

SegmentedControl.propTypes = {
  label: PropTypes.string,
  options: PropTypes.arrayOf(PropTypes.shape({
    value: PropTypes.any,
    label: PropTypes.string,
  })),
  value: PropTypes.any,
  onChange: PropTypes.func,
};

function Settings({ theme, setTheme, vimEnabled, setVimEnabled }) {
  const [open, setOpen] = useState(false);
  const panelRef = useRef(null);

  useEffect(() => {
    if (!open) {
      return;
    }
    function onPress(e) {
      if (e.type === "keydown" ? e.key === "Escape" : !panelRef.current?.contains(e.target)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", onPress);
    document.addEventListener("keydown", onPress);
    return () => {
      document.removeEventListener("mousedown", onPress);
      document.removeEventListener("keydown", onPress);
    };
  }, [open]);

  return (
    <div className="relative" ref={panelRef}>
      <button
        onClick={() => setOpen(!open)}
        className={
          open
            ? "rounded bg-raised p-1.5 text-ink"
            : "rounded p-1.5 text-dim hover:bg-raised hover:text-ink"
        }
        aria-label="Settings"
        aria-expanded={open}
        title="Settings"
      >
        <GearIcon />
      </button>
      {open &&
        <div className="absolute right-0 top-full z-50 mt-1 flex w-48 flex-col gap-3 rounded border border-edge bg-surface p-3 shadow-lg">
          <SegmentedControl
            label="Theme"
            value={theme}
            onChange={setTheme}
            options={[
              { value: "light", label: "Light" },
              { value: "dark", label: "Dark" },
            ]}
          />
          <SegmentedControl
            label="Vim keybindings"
            value={vimEnabled}
            onChange={setVimEnabled}
            options={[
              { value: true, label: "On" },
              { value: false, label: "Off" },
            ]}
          />
        </div>
      }
    </div>
  );
}

Settings.propTypes = {
  theme: PropTypes.string,
  setTheme: PropTypes.func,
  vimEnabled: PropTypes.bool,
  setVimEnabled: PropTypes.func,
};

function TableLink({ table }) {
  return (
    <li>
      <NavLink to={`/tables/${table.name}`} className={navItemClass}>
        <span className="font-mono text-xs">{table.schema.name}</span>
      </NavLink>
    </li>
  );
}

TableLink.propTypes = {
  table: PropTypes.shape({
    name: PropTypes.string,
    schema: PropTypes.shape({ name: PropTypes.string }),
  }),
};

export default function Root() {

  let [tables, setTables] = useState(null);

  let [queries, setQueries] = useState(null);
  let [openQuery, setOpenQuery] = useState(null);

  let [theme, setThemeState] = useState(initialTheme);
  let [vimEnabled, setVimState] = useState(initialVim);

  let navigate = useNavigate();
  let loc = useLocation();

  // server-sent events: any session change (this UI, an agent via the API,
  // or an external writer touching the sidecar) bumps sessionVersion
  let [sessionVersion, setSessionVersion] = useState(0);
  useEffect(() => {
    const source = new EventSource("/api/events");
    source.onmessage = () => setSessionVersion((v) => v + 1);
    return () => source.close();
  }, []);

  // no client-side cache: the query list is refetched on navigation and on
  // every server-side change, so all writers stay in sync
  useEffect(() => {
    let cancelled = false;
    fetchQueries()
      .then((data) => {
        if (!cancelled) {
          setQueries(data.queries);
          setOpenQuery(data.open);
        }
      })
      .catch((e) => {
        console.error("Failed to load queries:", e);
        if (!cancelled) {
          setQueries([]);
        }
      });
    return () => { cancelled = true; };
  }, [loc.pathname, sessionVersion]);

  function setTheme(next) {
    setThemeState(next);
    applyTheme(next);
  }

  function setVimEnabled(next) {
    setVimState(next);
    applyVim(next);
  }

  useEffect(() => {
    if (loc.hash === "#new") {
      let name = decodeURIComponent(loc.pathname.split("/")[2] || "");
      let current = document.getElementById("query-link-" + name);
      if (current) {
        current.scrollIntoView({ block: "nearest" });
      }
    }
  }, [queries, loc]);

  async function newQuery() {
    try {
      const created = await createQuery({});
      navigate(queryPath(created.name) + "#new");
    } catch (e) {
      console.error("Failed to create query:", e);
    }
  }

  useEffect(() => {
    fetch(location.origin + "/tables.json",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        }
      }
    )
      .then((response) => response.json())
      .then((returned_tables) => {
        setTables(returned_tables);
      });
    // refetched on every session change too, so a table attached or detached
    // while the server runs appears here without a reload
  }, [sessionVersion])

  return (
    <div className="flex h-full min-h-0 bg-bg font-sans text-[13px] text-ink">
      <aside className="flex w-64 shrink-0 flex-col border-r border-edge bg-surface">
        <div className="flex h-12 shrink-0 items-center justify-between border-b border-edge px-4">
          <NavLink to="/" className="font-mono text-sm font-semibold tracking-tight text-ink">
            sqlnow<span className="text-accent">_</span>
          </NavLink>
          <Settings
            theme={theme} setTheme={setTheme}
            vimEnabled={vimEnabled} setVimEnabled={setVimEnabled}
          />
        </div>

        <nav className="min-h-0 flex-1 overflow-y-auto px-2 pb-4">
          <ul className="pt-2">
            <li>
              <NavLink to="/history" className={navItemClass}>History</NavLink>
            </li>
          </ul>

          <div className={groupLabelClass()}>
            <span>Queries</span>
            <button
              onClick={newQuery}
              className="rounded px-1.5 py-0.5 font-mono text-[10px] font-medium uppercase tracking-[0.14em] text-accent hover:bg-raised"
            >
              + new
            </button>
          </div>
          <ul className="max-h-56 overflow-y-auto">
            {queries && queries.map((query) => (
              <li key={query.name}>
                <NavLink
                  to={queryPath(query.name)}
                  id={`query-link-${query.name}`}
                  className={navItemClass}
                >
                  {query.name}
                </NavLink>
              </li>
            ))}
          </ul>

          {tables &&
            <>
              <div className={groupLabelClass()}>
                <span>Tables</span>
              </div>
              <ul>
                {tables.tables.filter((table) => !table.section).map((table) => (
                  <TableLink key={table.name} table={table} />
                ))}
              </ul>
              {tables.sections.map((section) => (
                <details key={section} className="group" open={tables.sections.length === 1}>
                  <summary className="flex cursor-pointer select-none items-center gap-1.5 rounded px-2 py-0.5 font-mono text-xs text-muted hover:bg-raised hover:text-ink">
                    <svg
                      className="h-3 w-3 shrink-0 text-muted group-open:rotate-90"
                      viewBox="0 0 12 12" fill="none" stroke="currentColor"
                      strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"
                      aria-hidden="true"
                    >
                      <path d="M4.5 2.5 8 6l-3.5 3.5" />
                    </svg>
                    <span className="truncate">{section}</span>
                  </summary>
                  <ul className="ml-3 border-l border-edge pl-1">
                    {tables.tables.filter((table) => table.section === section).map((table) => (
                      <TableLink key={table.name} table={table} />
                    ))}
                  </ul>
                </details>
              ))}
            </>
          }
        </nav>
      </aside>

      <Outlet context={{ tables, queries, openQuery, sessionVersion, theme, vimEnabled }} />
    </div>
  );
}
