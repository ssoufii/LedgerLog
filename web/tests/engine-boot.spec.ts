import { expect, test } from "@playwright/test";

/**
 * The one claim of story M11.2: the real engine boots in the browser and a
 * write goes through it.
 *
 * The assertions deliberately read the page's own rendering of the snapshot
 * rather than calling the bridge from the test. What has to be true is that a
 * visitor sees the engine's state, and a test that reached into the worker
 * would pass on a page that showed nothing.
 */

// Booting CPython in wasm from a cold cache is the slowest thing on the page,
// and the story fixes the budget at thirty seconds.
const BOOT_TIMEOUT = 30_000;

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
});

test("the engine boots and reports the Python it is running", async ({ page }) => {
  const info = page.getByTestId("engine-info");

  await expect(info).toContainText("python 3.");
  await expect(info).toContainText("pyodide ");
  // The engine's own modules, imported from the copied source rather than
  // reimplemented: the package plus the eight modules it is built from.
  await expect(info).toContainText("engine modules imported");
  await expect(page.getByTestId("boot-error")).toHaveCount(0);
});

test("a put then a get round-trips a value through the engine", async ({ page }) => {
  await page.getByLabel("key").fill("user:1042:login");
  await page.getByLabel("value").fill("2026-11-16T05:00:00Z");
  await page.getByRole("button", { name: "put" }).click();
  await expect(page.getByTestId("last-action")).toContainText("logged at offset");

  await page.getByRole("button", { name: "get", exact: true }).click();

  await expect(page.getByTestId("get-result")).toHaveText("2026-11-16T05:00:00Z");
});

test("the snapshot lists the key in the memtable, with its tower level", async ({ page }) => {
  await page.getByLabel("key").fill("alpha");
  await page.getByLabel("value").fill("one");
  await page.getByRole("button", { name: "put" }).click();
  await expect(page.getByTestId("last-action")).toContainText("logged at offset");

  const entry = page.getByTestId("memtable-entries").locator("li[data-key='alpha']");

  await expect(entry).toHaveCount(1);
  const level = Number(await entry.getAttribute("data-level"));
  // The height is drawn by the engine's own skip list, so the test can only say
  // that it is a real level, not which one.
  expect(level).toBeGreaterThanOrEqual(1);
  expect(level).toBeLessThanOrEqual(16);
});

test("the log shows the record the write went into first", async ({ page }) => {
  await page.getByLabel("key").fill("alpha");
  await page.getByLabel("value").fill("one");
  await page.getByRole("button", { name: "put" }).click();

  const records = page.getByTestId("wal-records").locator("li");

  await expect(records).toHaveCount(1);
  await expect(records.first()).toContainText("put alpha");
  await expect(records.first()).toContainText("crc32");
});

test("a flush turns the memtable into a table on disk", async ({ page }) => {
  await page.getByLabel("key").fill("alpha");
  await page.getByLabel("value").fill("one");
  await page.getByRole("button", { name: "put" }).click();
  await expect(page.getByTestId("memtable-entries").locator("li")).toHaveCount(1);

  await page.getByRole("button", { name: "flush" }).click();

  await expect(page.getByTestId("sstables").locator("li")).toHaveCount(1);
  await expect(page.getByTestId("sstables").locator("li").first()).toContainText("1 records");
  await expect(page.getByTestId("memtable-entries").locator("li")).toHaveCount(0);
  // The record is still readable, now from the table rather than from memory.
  await page.getByRole("button", { name: "get", exact: true }).click();
  await expect(page.getByTestId("get-result")).toHaveText("one");
});

test("a reset gives the visitor an empty database back", async ({ page }) => {
  await page.getByLabel("key").fill("alpha");
  await page.getByLabel("value").fill("one");
  await page.getByRole("button", { name: "put" }).click();
  await expect(page.getByTestId("memtable-entries").locator("li")).toHaveCount(1);

  await page.getByRole("button", { name: "reset" }).click();

  await expect(page.getByTestId("memtable-entries").locator("li")).toHaveCount(0);
  await expect(page.getByTestId("wal-records").locator("li")).toHaveCount(0);
  await page.getByRole("button", { name: "get", exact: true }).click();
  await expect(page.getByTestId("get-result")).toHaveText("not found");
});
