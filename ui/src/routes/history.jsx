import { useNavigate } from "react-router-dom";
import { addToHistory } from "../utils";

export default function History() {
  let history = localStorage.getItem("sqlnow-history-list") || "";
  let historyList = history.split(",").map((s) => s.trim());
  let navigate = useNavigate();

  function onHistoryClick(sql) {
    let oldSql = window.localStorage.getItem('sqlnow-sql-query-query');

    if (oldSql) {
      addToHistory(oldSql);
    }

    window.localStorage.setItem('sqlnow-sql-query-query', sql);
    navigate("/queries/query");
  }

  let historyHtml = historyList.map((hash) => {
    let sql = localStorage.getItem('sqlnow-history-' + hash);
    return <tr key={hash}>
      <td><a className="btn btn-sm" onClick={() => onHistoryClick(sql)}>query</a></td>
      <td><pre>{sql}</pre></td>
    </tr>
  });

  return (
    <main id="tab_content" role="main" className="w-full sm:w-1/2 md:w-2/3 xl:w-3/4 pt-1 px-2">
      <div className="flex justify-between mb-4">
        <div>
          <div className="overflow-x-auto">
          <table className="table">
              <thead>
                  <tr>
                      <th></th>
                      <th>SQL</th>
                      <th></th>
                  </tr>
              </thead>
              <tbody>
                {historyHtml}
              </tbody>
          </table>
          </div>
          
        </div>
      </div>
    </main>
  );
}