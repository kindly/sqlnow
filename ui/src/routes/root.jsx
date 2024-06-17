import { NavLink, useLocation } from "react-router-dom";
import { useEffect, useState } from "react";
import { Outlet } from "react-router-dom";
import { useNavigate } from "react-router-dom";

function activeButton ({isActive, isPending, isTransitioning}) {
  return [
    isPending ? "" : "",
    isActive ? "active" : "",
    isTransitioning ? "" : "",
  ].join(" ") ;
}


export default function Root() {

  let [ tables, setTables ] = useState(null);

  let [ queries, setQueries ] = useState(JSON.parse(localStorage.getItem('sqlnow-queries')));

  let navigate = useNavigate();
  let loc = useLocation();

  useEffect(() => {
    if (loc.hash == "#new") {
      let querySection = document.getElementById("query-section");
      querySection.scrollTop = querySection.scrollHeight;
    }
  [queries]});

  function newQuery() {
    let newQueries = [...queries];
    let newId = parseInt(localStorage.getItem('sqlnow-queryLastId')) + 1;
    localStorage.setItem('sqlnow-queryLastId', newId.toString());
    newQueries.push({"id": newId, "name": "query " + newId});
    setQueries(newQueries);
    localStorage.setItem('sqlnow-queries', JSON.stringify(newQueries));

    navigate("/queries/" + newId + "#new");
    // let querySection = document.getElementById("query-section");
    // querySection.scrollTop = querySection.scrollHeight;
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
  }, [])

  return (
    <div id="main">
      <div className="flex flex-row flex-wrap py-4">
        <ul className="menu menu-sm w-full w-56 rounded-box sm:w-1/2 md:w-1/3 xl:w-1/4 px-2">
          <li>
            <NavLink to="/history" className={activeButton}>history</NavLink>
          </li>
          <div className="border">
            <li className="menu-title">Queries</li>
          </div>
          <div className="border">
            <div id="query-section"className="max-h-32 overflow-auto">
              {queries.map((query) => (
                <li key={query.id}>
                  <NavLink to={`/queries/${query.id}`} className={activeButton}>{query.name}</NavLink>
                </li>
              ))}
            </div>
            <button className="btn btn-xs mt-2 w-full" onMouseDown={newQuery}>new query</button>
          </div>


          <div className="border mt-2">

          {tables && <li className="menu-title border">Tables</li>}
        
          <div className="overflow-auto" style={{height: "calc(100vh - 350px)"}}>

            {tables && tables.tables.filter((table) => !table.section).map((table) => (
              <li key={table.name}>
                <NavLink to={`/tables/${table.name}`} className={activeButton}>{table.schema.name}</NavLink>
              </li>
            ))}

            <div className="overflow-auto">
              {tables && tables.sections.map((section) => (
                <li key={section}>
                  <details>
                    <summary>{section}</summary>
                    <ul>
                      {tables.tables.filter((table) => table.section == section).map((table) => (
                        <li key={table.name}>
                          <NavLink to={`/tables/${table.name}`} className={activeButton}>{table.schema.name}</NavLink>
                        </li>
                      ))}
                    </ul>
                  </details>
                </li>
              ))}
            </div>
          </div>
          </div>
        </ul>

        <Outlet context={{tables, queries, setQueries}} />
        {/* {% include "table.html" %}  */}
      </div>
    </div>
  );
}

