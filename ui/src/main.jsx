import React from 'react'
import ReactDOM from 'react-dom/client'
import {
  createBrowserRouter,
  RouterProvider,
} from "react-router-dom";

import '@fontsource/ibm-plex-sans/400.css';
import '@fontsource/ibm-plex-sans/500.css';
import '@fontsource/ibm-plex-sans/600.css';
import '@fontsource/ibm-plex-mono/400.css';
import '@fontsource/ibm-plex-mono/500.css';
import '@fontsource/ibm-plex-mono/600.css';
import './index.css'

import ErrorPage from "./error-page";
import Home from "./routes/home";
import Query from "./routes/query";
import Table from "./routes/table";
import Root from './routes/root';
import History from './routes/history';

import { initialTheme, applyTheme } from './theme';

// Set the theme class before first paint so the app never flashes
// the wrong background.
applyTheme(initialTheme());

const router = createBrowserRouter([
  {
    path: "/",
    errorElement: <ErrorPage />,
    element: <Root />,
    children: [
      {
        index: true,
        errorElement: <ErrorPage />,
        element: <Home />,
      },
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
