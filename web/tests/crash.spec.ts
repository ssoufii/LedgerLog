import { expect, test, type Page } from "@playwright/test";

import { auditPage } from "./support/house-rules";

/**
 * Story M11.7's contract: the tiles run real recovery on a copy of the
 * visitor's database, and report what the engine made of it.
 *
 * The damage and the recovery are `ledgerlog.faults`, which `tests/test_faults.py`
 * asserts on directly. What is left for these tests is what only a browser can
 * show: that pressing a tile runs the scenario in the worker, that the result
 * on the tile is the engine's, and that the visitor's own database comes
 * through it unchanged.
 */

const BOOT_TIMEOUT = 30_000;
// Copying a database, merging tables and reopening an engine inside a wasm
// interpreter is slower than any other call the page makes.
const CRASH_TIMEOUT = 30_000;

async function openReady(page: Page): Promise<void> {
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
}

async function write(page: Page, key: string, value: string): Promise<void> {
  await page.getByLabel("key").fill(key);
  await page.getByLabel("value").fill(value);
  await page.getByRole("button", { name: "put", exact: true }).click();
  await expect(page.getByTestId("last-action")).toContainText(`${key} logged at offset`);
}

function tile(page: Page, scenario: string) {
  return page.locator(`[data-scenario="${scenario}"]`);
}

test.beforeEach(async ({ page }) => {
  await openReady(page);
});

test("the board is the heading tile and one tile per scenario", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await expect(page.locator(".crash-tile--heading")).toHaveText(/pull the plug/);
  await expect(page.locator("[data-scenario]")).toHaveCount(6);

  // Four across at desktop width, with the heading tile taking two of them.
  const lefts = await page
    .locator(".crash__board > *")
    .evaluateAll((nodes) => nodes.map((node) => Math.round(node.getBoundingClientRect().left)));
  expect(new Set(lefts).size).toBe(4);

  // Every tile names a test. That the test exists is checked by pytest; that
  // the tile says so is checked here.
  for (const scenario of [
    "torn-write",
    "flipped-byte",
    "crash-mid-flush",
    "crash-mid-compaction",
    "missing-footer",
    "expired-record",
  ]) {
    await expect(tile(page, scenario).locator("code")).toHaveText(/^tests\/test_\w+\.py::test_\w+$/);
  }
});

test("a torn write keeps the whole records and drops the torn one", async ({ page }) => {
  await write(page, "alpha", "one");
  await write(page, "bravo", "two");
  await write(page, "charlie", "three");

  await tile(page, "torn-write").click();

  const torn = tile(page, "torn-write");
  await expect(torn).toHaveAttribute("data-done", "true", { timeout: CRASH_TIMEOUT });
  await expect(torn.locator('[data-row="before"] dd')).toContainText("alpha, bravo, charlie");
  // The last record written is the one the tear cut, so it is the one lost and
  // the earlier ones are not.
  await expect(torn.locator('[data-row="dropped"] dd')).toHaveText("charlie");
  const recovered = torn.locator('[data-row="recovered"] dd');
  await expect(recovered).toContainText("alpha");
  await expect(recovered).toContainText("bravo");
  await expect(recovered).not.toContainText("charlie");
  // And the engine said why, rather than the page deciding.
  await expect(torn).toContainText("record claims");
});

test("the visitor's own database comes through a scenario unchanged", async ({ page }) => {
  await write(page, "alpha", "one");
  await write(page, "bravo", "two");
  const logBefore = await page.getByTestId("wal-records").locator("li").allTextContents();
  const towersBefore = await page
    .getByTestId("memtable-entries")
    .locator("li.tower")
    .evaluateAll((nodes) => nodes.map((node) => node.getAttribute("data-key")));

  await tile(page, "torn-write").click();
  await expect(tile(page, "torn-write")).toHaveAttribute("data-done", "true", {
    timeout: CRASH_TIMEOUT,
  });

  // Nothing on the page moved, because nothing in the visitor's directory did.
  expect(await page.getByTestId("wal-records").locator("li").allTextContents()).toEqual(logBefore);
  expect(
    await page
      .getByTestId("memtable-entries")
      .locator("li.tower")
      .evaluateAll((nodes) => nodes.map((node) => node.getAttribute("data-key"))),
  ).toEqual(towersBefore);
  await page.getByRole("button", { name: "get", exact: true }).click();
  await expect(page.getByTestId("get-result")).toHaveText("two");
});

test("a flush killed before its footer loses nothing, and the engine says why", async ({
  page,
}) => {
  await write(page, "alpha", "one");

  await tile(page, "crash-mid-flush").click();

  const flush = tile(page, "crash-mid-flush");
  await expect(flush).toHaveAttribute("data-done", "true", { timeout: CRASH_TIMEOUT });
  await expect(flush.locator('[data-row="dropped"] dd')).toHaveText("none");
  await expect(flush.locator('[data-row="dropped"]')).toHaveAttribute("data-dropped", "false");
  await expect(flush).toContainText("footer");
  await expect(flush).toContainText(".sst");
});

test("an expired record is the one thing that goes without damage", async ({ page }) => {
  await write(page, "alpha", "one");

  await tile(page, "expired-record").click();

  const expired = tile(page, "expired-record");
  await expect(expired).toHaveAttribute("data-done", "true", { timeout: CRASH_TIMEOUT });
  // Something was dropped, and no file was rejected: the record is gone
  // because its deadline passed, not because anything broke.
  await expect(expired.locator('[data-row="dropped"]')).toHaveAttribute("data-dropped", "true");
  await expect(expired.locator('[data-row="table"]')).toHaveCount(0);
});

test("a scenario the database cannot stage says so instead of inventing a result", async ({
  page,
}) => {
  // A reset leaves an empty log, and an empty log has no record to tear.
  await page.getByRole("button", { name: "reset", exact: true }).click();
  await expect(page.getByTestId("wal-records").locator("li")).toHaveCount(0);

  await tile(page, "torn-write").click();

  const torn = tile(page, "torn-write");
  await expect(torn).toHaveAttribute("data-done", "true", { timeout: CRASH_TIMEOUT });
  await expect(torn.locator('[data-refused="true"]')).toContainText("no records to tear");
  await expect(torn.locator(".crash-tile__row")).toHaveCount(0);
});

test("every tile is operable by keyboard", async ({ page }) => {
  await write(page, "alpha", "one");

  await tile(page, "flipped-byte").focus();
  await page.keyboard.press("Enter");

  await expect(tile(page, "flipped-byte")).toHaveAttribute("data-done", "true", {
    timeout: CRASH_TIMEOUT,
  });
});

test("the board holds the house rules, with results on it, at every width", async ({ page }) => {
  await write(page, "alpha", "one");
  await tile(page, "torn-write").click();
  await expect(tile(page, "torn-write")).toHaveAttribute("data-done", "true", {
    timeout: CRASH_TIMEOUT,
  });

  for (const [width, height] of [
    [1440, 900],
    [768, 1024],
    [390, 844],
  ] as const) {
    await page.setViewportSize({ width, height });
    const report = await page.evaluate(auditPage);
    expect(report.violations, `violations at ${String(width)}`).toEqual([]);
    expect(report.scrollWidth).toBeLessThanOrEqual(report.clientWidth);
  }
});
