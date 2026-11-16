import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

/**
 * The demo is a static site: one page, one worker, and the Pyodide runtime and
 * engine source that `scripts/sync-assets.mjs` copies into `public/`.
 *
 * The worker is built as an ES module so it can use a dynamic import to load
 * the interpreter at runtime, and the build target is set high enough for
 * top-level await in that worker rather than left to the default.
 */
export default defineConfig({
  plugins: [react()],
  build: {
    target: "es2022",
    // The wasm module and the stdlib archive are served as files from public/,
    // so nothing here should be inlined into the bundle.
    assetsInlineLimit: 0,
  },
  worker: {
    format: "es",
  },
});
