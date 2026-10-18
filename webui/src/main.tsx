import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { AppProviders } from "./app/providers";
import { AppRoutes } from "./app/routes";
import "./i18n";
import "./styles/theme.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <AppProviders>
      <BrowserRouter basename="/ui">
        <AppRoutes />
      </BrowserRouter>
    </AppProviders>
  </StrictMode>,
);
