import { useNavigate } from "react-router-dom";
import { useOutletContext } from "react-router-dom";

export default function History() {
  let history = localStorage.getItem("sqlnow-history-list") || "";
  let historyList = history.split(",").map((s) => s.trim());
  let navigate = useNavigate();

  let {queries, setQueries} = useOutletContext();

  function onHistoryClick(sql) {

    let id = parseInt(window.localStorage.getItem('sqlnow-queryLastId'));
    let new_id = id + 1;

    window.localStorage.setItem('sqlnow-queryLastId', new_id.toString());

    let newQueries = [...queries];

    newQueries.push({"id": new_id, "name": "query " + new_id});

    setQueries(newQueries);

    window.localStorage.setItem('sqlnow-queries', JSON.stringify(newQueries));
    window.localStorage.setItem('sqlnow-sql-query-' + new_id.toString(), sql);

    navigate("/queries/" + new_id.toString() + "#new");
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