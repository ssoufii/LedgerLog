import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";
import { startMotion } from "./motion";
import "./index.css";

const container = document.getElementById("root");
if (container === null) {
  throw new Error("index.html has no #root to mount into");
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);

// After the first render is queued, so the observer finds the sections; any
// mounted later are picked up as they arrive (see motion.ts).
startMotion();
