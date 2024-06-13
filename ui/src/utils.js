import xxhash from 'xxhash-wasm';

export function addToHistory(sql) {
  let history = localStorage.getItem('sqlnow-history-list') || '';
  let historyList = history.split(',');
  if (!history) {
    historyList = [];
  }

  xxhash().then(hasher => {
    let sqlhash = hasher.h64ToString(sql);
    localStorage.setItem('sqlnow-history-' + sqlhash, sql);

    const index = historyList.indexOf(sqlhash);
    if (index > -1) { 
      historyList.splice(index, 1); 
    }
    historyList.unshift(sqlhash);
    localStorage.setItem('sqlnow-history-list', historyList.join(','));
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