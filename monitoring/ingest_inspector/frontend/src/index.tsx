import React from "react";
import {createRoot} from 'react-dom/client';
import "./index.css";
import App from "./App";
import {RouterProvider, createBrowserRouter} from "react-router-dom";

const router = createBrowserRouter([
    {
        path: "/:ingestId",
        element: <App/>,
    },
    {
        path: "/",
        element: <App/>,
    },
]);

const container = document.getElementById('root');
const root = createRoot(container!);
root.render(<RouterProvider router={router}/>);
