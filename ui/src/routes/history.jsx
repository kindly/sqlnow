import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { createQuery, fetchHistory, queryPath } from "../utils";

export default function History() {
  const [entries, setEntries] = useState(null);
  const navigate = useNavigate();

  useEffect(() => {
    fetchHistory(200)
      .then((data) => setEntries(data.history))
      .catch((e) => {
        console.error("Failed to load history:", e);
        setEntries([]);
      });
  }, []);

  async function onHistoryClick(sql) {
    try {
      const created = await createQuery({ sql });
      navigate(queryPath(created.name) + "#new");
    } catch (e) {
      console.error("Failed to create query from history:", e);
    }
  }

  return (
    <main role="main" className="flex min-w-0 flex-1 flex-col bg-bg">
      <header className="flex h-12 shrink-0 items-center gap-3 border-b border-edge bg-surface px-4">
        <h1 className="text-[13px] font-medium">History</h1>
        {entries &&
          <span className="font-mono text-[11px] text-dim">
            {entries.length === 200
              ? "last 200 queries"
              : `${entries.length.toLocaleString()} ${entries.length === 1 ? "query" : "queries"}`}
          </span>
        }
      </header>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {entries && entries.length === 0 &&
          <div className="flex h-full items-center justify-center">
            <p className="font-mono text-xs text-dim">Queries you run appear here</p>
          </div>
        }
        {entries && entries.map((entry, i) => (
          <div key={i} className="flex items-start gap-4 border-b border-edge px-4 py-3">
            <pre className="min-w-0 flex-1 overflow-x-auto font-mono text-xs leading-5 text-muted">{entry.sql}</pre>
            <span className="shrink-0 font-mono text-[11px] text-dim">{entry.at}</span>
            <button
              className="shrink-0 rounded border border-edge px-2.5 py-1 font-mono text-[11px] text-muted hover:border-edge-strong hover:text-ink"
              onClick={() => onHistoryClick(entry.sql)}
            >
              Open as query
            </button>
          </div>
        ))}
      </div>
    </main>
  );
}
