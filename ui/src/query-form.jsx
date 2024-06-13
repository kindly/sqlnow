import { useMemo, useCallback, useState } from 'react';
import PropTypes from 'prop-types';
import { useOutletContext } from "react-router-dom";
import { addToHistory } from './utils';
import { DataEditor, GridCellKind } from '@glideapps/glide-data-grid';
import "@glideapps/glide-data-grid/dist/index.css";
import { vim } from "@replit/codemirror-vim"

import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';


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

  let tables = useOutletContext();

  let schema = undefined;
  let table = undefined;

  if (queryType == "table" && tables) {
    table = tables.tables.find((table) => table.name == queryName);
    if (table) {
      schema = table.schema;
    }
  }

  let initialSql = window.localStorage.getItem(`sqlnow-sql-${queryType}-${queryName}`);
  if (!initialSql && schema && queryType == "table") {
    initialSql = generateSql(schema, "select_fields");
  }
   
  const [sql, setSql] = useState(initialSql || '');

  let [columnWidths, setColumnWidths] = useState([])

  const [displayLimit, setDisplayLimit] = useState('500');

  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);

  function setSqlFromSchema(sqlType) {
    addToHistory(sql);
    setSql(generateSql(schema, sqlType));

    window.localStorage.setItem(`sqlnow-sql-${queryType}-${queryName}`, generateSql(schema, sqlType))
  }

  const onColumnResize = useCallback((column, newSize, colIndex) => {
    setColumnWidths(prevColumns => {
      let new_columns = [...prevColumns]
      new_columns[colIndex] = newSize
      return new_columns;
    });
  }, []);

  async function runQuery() {

    let formData = new URLSearchParams();
    formData.append('sql', sql); // replace with your actual form data
    formData.append('display_limit', displayLimit); // replace with your actual form data

    addToHistory(sql);

    let res = await fetch(location.origin + "/query.json",
      {
        method: "POST",
        body: formData,
      }
    )
    let resp = await res.json();
    setResults(resp.table_data);
    setColumnWidths(Array(resp.table_data.headers.length).fill(150));
    setError(resp.error);
  }

  function onSqlChange(e) {
    window.localStorage.setItem(`sqlnow-sql-${queryType}-${queryName}`, e.target.value)
    setSql(e.target.value);
  }

  let columns = useMemo(() => {
    if (!results) {
      return [];
    }
    return results.headers.map((header, i) => {return {
      "title": header,
      "id": header,
      "width": columnWidths[i] || 150
    }});

  }, [results, columnWidths]);

  function getCellContent(cell) {
    const [col, row] = cell;


    return {
      kind: GridCellKind.Text,
      data: results.rows[row][col],
      displayData: results.rows[row][col]
    }
  }



  return (
    <main id="tab_content" role="main" className="w-full sm:w-1/2 md:w-2/3 xl:w-3/4 pt-1 px-2">
        <form autoComplete="off" method="post" action="/outputs">
        <div>
            <div className="flex justify-between mb-4">
                <div>
                  {queryType === "table" &&
                    <>
                        <a className="btn btn-xs" onClick={() => setSqlFromSchema("select_star")}>SELECT *</a>
                        <a className="btn btn-xs" onClick={() => setSqlFromSchema("select_fields")}>SELECT fields</a>
                        <a className="btn btn-xs" onClick={() => setSqlFromSchema("select_fields_type")}>SELECT fields with types</a>
                    </>
                  }
                </div>
                <div>
                    <span>Display Limit:</span>
                    <input className="input input-bordered input-xs w-20" id="display_limit"
                    name="display_limit" value={displayLimit} 
                    onChange={(e) => setDisplayLimit(e.target.value)}/>
                </div>
            </div>

            <input type="hidden" name="sql" value={sql} />
            <CodeMirror value={sql} height="300px" extensions={[vim(), langs.sql()]} onChange={(value) => setSql(value)} />

            {/* <textarea 
                style={{"fontFamily": "monospace", "height": "400px"}} 
                className="w-full p-2 border border-slate-300 rounded-md mb-2" 
                //   onBlur="window.localStorage.setItem('sqlnow-sql-{{page_type}}-{{ name }}', this.value)"
                onChange={onSqlChange}
                placeholder="Enter your sql" 
                id="sql" 
                value={sql}
                name='sql'>
            </textarea> */}

            <div className="flex justify-between mt-2">
                <a className="btn btn-primary btn-sm btn-active" onClick={runQuery}>Run</a>

                <div>
                    <input className="btn btn-sm" type="submit" name="csv" value="CSV"/>
                    <input className="btn btn-sm" type="submit" name="tab" value="Tab delimited"/>
                    <input className="btn btn-sm" type="submit" name="jsonl" value="JSONL Stream"/>
                </div>
            </div> 
            <span id="results-spinner" className="hidden relative top-2 htmx-indicator loading loading-spinner loading-md"></span>

            {error && <div className="mt-2 text-red-500">{error}</div>}

            {results && 
              <div className='mt-2'>
                <DataEditor onChange={onSqlChange} getCellContent={getCellContent} 
                  getCellsForSelection={true} keybindings={{search: true}} 
                  columns={columns} rows={results.rows.length} width={"100%"} height={"calc(100vh - 450px)"} 
                  onColumnResize={onColumnResize}
                  theme={{"baseFontStyle": "12.5px", "headerFontStyle": "bold 13px",  lineHeight:1}} />
              </div>
            }
          </div>
          </form>
    </main>

  );
}

QueryForm.propTypes = {
  queryType: PropTypes.string,
  queryName: PropTypes.string,
};