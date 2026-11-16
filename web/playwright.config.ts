import { defineConfig, devices } from "@playwright/test";

/**
 * Tests run against `vite preview`, which serves the production build, because
 * that is the thing being shipped: the worker is bundled, the Pyodide runtime
 * and the engine source are served out of `dist/`, and a dev server would test
 * none of that. Run `npm run build` first (the CI job does).
 *
 * Booting CPython in wasm is slow on a cold cache, so the engine specs ask for
 * their own timeout rather than raising it for every test.
 */
const PORT = 4173;

export default defineConfig({
  testDir: "./tests",
  outputDir: "./test-results/artifacts",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI
    ? [["list"], ["html", { outputFolder: "test-results/report", open: "never" }]]
    : [["list"]],
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    trace: "off",
    launchOptions: {
      // The CI container and the dev container both run the browser as root,
      // where Chromium's own sandbox cannot start.
      args: ["--no-sandbox"],
    },
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `npm run preview -- --port ${PORT} --strictPort`,
    url: `http://127.0.0.1:${PORT}`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
