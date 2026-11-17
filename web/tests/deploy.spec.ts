import { readFileSync } from "node:fs";
import { join } from "node:path";

import { expect, test } from "@playwright/test";

/**
 * Story M11.10's contract: the demo is ready to deploy as a static site, and it
 * needs nothing at runtime but its own files and the font.
 */

const BOOT_TIMEOUT = 30_000;

/** The one other origin the page may talk to: Fontshare, for Pilcrow Rounded. */
const ALLOWED = /^https:\/\/(api|cdn)\.fontshare\.com\//;

test("the page asks no origin but its own and the font provider", async ({ page, baseURL }) => {
  const own = new URL(baseURL ?? "http://127.0.0.1:4173").origin;
  const foreign = new Set<string>();
  page.on("request", (request) => {
    const url = request.url();
    if (url.startsWith("data:") || url.startsWith("blob:")) {
      return;
    }
    if (new URL(url).origin !== own && !ALLOWED.test(url)) {
      foreign.add(url);
    }
  });

  // The whole life of a visit: boot the engine, use it, run a crash scenario,
  // and scroll to the end, so anything fetched lazily is fetched here too.
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
  await page.getByRole("button", { name: "put", exact: true }).click();
  await page.getByRole("button", { name: "flush", exact: true }).click();
  await page.locator('[data-scenario="torn-write"]').click();
  await expect(page.locator('[data-scenario="torn-write"]')).toHaveAttribute("data-done", "true");
  await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight));
  await page.waitForLoadState("networkidle");

  expect([...foreign]).toEqual([]);
});

test("render.yaml publishes web/dist as a static site with the cache policy the story sets", () => {
  const blueprint = readFileSync(join("..", "render.yaml"), "utf8");

  expect(blueprint).toMatch(/runtime:\s*static/);
  expect(blueprint).toMatch(/rootDir:\s*web\b/);
  expect(blueprint).toMatch(/buildCommand:\s*npm ci && npm run build/);
  expect(blueprint).toMatch(/staticPublishPath:\s*dist\b/);
  expect(blueprint).toMatch(
    /path:\s*\/assets\/\*\s*\n\s*name:\s*Cache-Control\s*\n\s*value:\s*public, max-age=31536000, immutable/,
  );
  expect(blueprint).toMatch(/path:\s*\/index\.html\s*\n\s*name:\s*Cache-Control\s*\n\s*value:\s*no-cache/);
});
