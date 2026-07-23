import xxhash from 'xxhash-wasm';

// The server injects window.SQLNOW_SCOPE (the session sidecar id) into
// index.html, so stored state is keyed per session. Without one (plain
// in-memory runs, vite dev server) keys are unscoped, as before.
export function storageKey(suffix) {
  const scope = window.SQLNOW_SCOPE;
  return scope ? `sqlnow-${scope}-${suffix}` : `sqlnow-${suffix}`;
}

export function addToHistory(sql) {
  let history = localStorage.getItem(storageKey('history-list')) || '';
  let historyList = history.split(',');
  if (!history) {
    historyList = [];
  }

  xxhash().then(hasher => {
    let sqlhash = hasher.h64ToString(sql);
    localStorage.setItem(storageKey('history-' + sqlhash), sql);

    const index = historyList.indexOf(sqlhash);
    if (index > -1) {
      historyList.splice(index, 1);
    }
    historyList.unshift(sqlhash);
    localStorage.setItem(storageKey('history-list'), historyList.join(','));
  });
}



            //     {/* <div className="mt-2">
            //       <table className="table table-xs table-pin-rows mt-2">
            //         <thead>
            //           <tr>
            //             {results.headers.map((header, i) => (
            //                 <td key={i}><pre>{header}</pre></td>
            //             ))}
            //           </tr>
            //         </thead>
            //         <tbody>
            //           {results.rows.map((row, i) => (
            //             <tr key={i}>
            //               {row.map((cell, j) => (
            //                 <td key={j}><pre>{cell}</pre></td>
            //               ))}
            //             </tr>
            //           ))}
            //         </tbody>
            //       </table>
            //     </div>
            //   </div> */}