import React from 'react'
import ReactDOM from 'react-dom/client'
import {
  createBrowserRouter,
  RouterProvider,
} from "react-router-dom";
import './index.css'

import ErrorPage from "./error-page";
import Query from "./routes/query";
import Table from "./routes/table";
import Root from './routes/root';
import History from './routes/history';

let queries = window.localStorage.getItem('sqlnow-queries');
// let queryIndex = window.localStorage.getItem('sqlnow-queryIndex');

if (!queries) {
  window.localStorage.setItem('sqlnow-queryLastId', '1');
  queries = [{"id": 1, "name": "query 1"}];
  window.localStorage.setItem('sqlnow-queries', JSON.stringify(queries));
}

const router = createBrowserRouter([
  {
    path: "/",
    errorElement: <ErrorPage />,
    element: <Root />,
    children: [
      {
        path: "/history",
        errorElement: <ErrorPage />,
        element: <History />,
      },
      {
        path: "/queries/:query",
        errorElement: <ErrorPage />,
        element: <Query />,
      },
      {
        path: "/tables/:table",
        errorElement: <ErrorPage />,
        element: <Table />,
      },
    ]
  },
]);

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>,
)
