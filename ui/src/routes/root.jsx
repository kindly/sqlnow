import { NavLink } from "react-router-dom";
import { useEffect, useState } from "react";
import { Outlet } from "react-router-dom";

function activeButton ({isActive, isPending, isTransitioning}) {
  return [
    isPending ? "" : "",
    isActive ? "active" : "",
    isTransitioning ? "" : "",
  ].join(" ") ;
}

export default function Root() {

  let [ tables, setTables ] = useState(null);

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
            <NavLink to="/queries/query" className={activeButton}>query</NavLink>
          </li>
          <li>
            <NavLink to="/history" className={activeButton}>history</NavLink>
          </li>

          {tables && <li className="menu-title">Tables</li>}

          {tables && tables.tables.filter((table) => !table.section).map((table) => (
            <li key={table.name}>
              <NavLink to={`/tables/${table.name}`} className={activeButton}>{table.schema.name}</NavLink>
            </li>
          ))}

          {tables && tables.sections.map((section) => (
            <li key={section}>
              <details open>
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
        </ul>

        <Outlet context={tables} />
        {/* {% include "table.html" %}  */}
      </div>
    </div>
  );
}

