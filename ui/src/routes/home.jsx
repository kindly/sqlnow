import { useEffect } from "react";
import { useNavigate, useOutletContext } from "react-router-dom";
import { queryPath } from "../utils";

export default function Home() {
  const { queries, openQuery } = useOutletContext();
  const navigate = useNavigate();

  // land on the session's open query, else the first one
  useEffect(() => {
    if (!queries || queries.length === 0) {
      return;
    }
    const target = openQuery && queries.some((q) => q.name === openQuery)
      ? openQuery
      : queries[0].name;
    navigate(queryPath(target), { replace: true });
  }, [queries, openQuery, navigate]);

  return (
    <main role="main" className="flex min-w-0 flex-1 items-center justify-center bg-bg">
      <div className="text-center">
        <p className="font-mono text-sm text-muted">
          Select a table, or create a query
        </p>
        <p className="mt-2 font-mono text-xs text-dim">
          ctrl+↵ runs the current query
        </p>
      </div>
    </main>
  );
}
