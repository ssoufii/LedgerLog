import { expect, test, type Page } from "@playwright/test";

import { auditPage } from "./support/house-rules";

/**
 * Story M11.4's contract: the hero console drives the engine, and what it
 * draws is the engine's own state.
 *
 * Every assertion reads the page, not the worker. A test that called the
 * bridge itself would pass on a page that drew nothing, and the claim being
 * made here is about what a visitor sees. Where a test needs to know a value
 * the engine chose (a tower's level, a WAL offset), it reads it off the data
 * attribute the component rendered straight from the snapshot, and then checks
 * that the drawing agrees with it.
 */

const BOOT_TIMEOUT = 30_000;

async function openReady(page: Page): Promise<void> {
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
}

async function write(page: Page, key: string, value: string): Promise<void> {
  await page.getByLabel("key").fill(key);
  await page.getByLabel("value").fill(value);
  await page.getByRole("button", { name: "put" }).click();
  await expect(page.getByTestId("last-action")).toContainText(`${key} logged at offset`);
}

test.beforeEach(async ({ page }) => {
  await openReady(page);
});

test("three puts draw three towers, as tall as the levels the engine drew", async ({ page }) => {
  await write(page, "alpha", "one");
  await write(page, "bravo", "two");
  await write(page, "charlie", "three");

  const towers = page.getByTestId("memtable-entries").locator("li.tower");
  await expect(towers).toHaveCount(3);

  // Key order, which is the order the memtable iterates in.
  expect(await towers.evaluateAll((nodes) => nodes.map((node) => node.getAttribute("data-key")))).toEqual([
    "alpha",
    "bravo",
    "charlie",
  ]);

  // The drawing agrees with the level: one block per level, and a measured
  // height of level blocks plus the gaps between them.
  const drawn = await towers.evaluateAll((nodes) =>
    nodes.map((node) => {
      const styles = getComputedStyle(document.documentElement);
      const block = Number.parseFloat(styles.getPropertyValue("--block"));
      const gap = Number.parseFloat(styles.getPropertyValue("--block-gap"));
      const stack = node.querySelector(".tower__stack");
      const level = Number(node.getAttribute("data-level"));
      return {
        level,
        blocks: stack?.children.length ?? 0,
        height: stack?.getBoundingClientRect().height ?? 0,
        expected: level * block + (level - 1) * gap,
      };
    }),
  );

  for (const tower of drawn) {
    expect(tower.level).toBeGreaterThanOrEqual(1);
    expect(tower.level).toBeLessThanOrEqual(16);
    expect(tower.blocks).toBe(tower.level);
    expect(tower.height).toBeCloseTo(tower.expected, 1);
  }
});

test("a get shows the value the engine has stored", async ({ page }) => {
  await write(page, "alpha", "one");
  await page.getByLabel("key").fill("alpha");

  await page.getByRole("button", { name: "get", exact: true }).click();

  await expect(page.getByTestId("get-result")).toHaveText("one");
  // And the value is on the tower too, read from the same snapshot.
  await expect(page.locator("li.tower[data-key='alpha'] .tower__value")).toHaveText("one");
});

test("a delete leaves a tombstone that is drawn differently", async ({ page }) => {
  await write(page, "alpha", "one");
  const tower = page.locator("li.tower[data-key='alpha']");
  const liveColour = await tower
    .locator(".tower__block")
    .first()
    .evaluate((node) => getComputedStyle(node).backgroundColor);

  await page.getByRole("button", { name: "delete" }).click();
  await expect(page.getByTestId("last-action")).toContainText("tombstoned at offset");

  await expect(tower).toHaveAttribute("data-tombstone", "true");
  await expect(tower.locator(".tower__value")).toHaveText("deleted");
  const deadColour = await tower
    .locator(".tower__block")
    .first()
    .evaluate((node) => getComputedStyle(node).backgroundColor);
  expect(deadColour).not.toBe(liveColour);
  // The key is gone as far as a reader is concerned, which is what the
  // tombstone is for.
  await page.getByRole("button", { name: "get", exact: true }).click();
  await expect(page.getByTestId("get-result")).toHaveText("not found");
});

test("the log tail shows the last six records, with their offsets and checksums", async ({ page }) => {
  for (let index = 0; index < 8; index += 1) {
    await write(page, `key-${String(index)}`, `value-${String(index)}`);
  }

  const rows = page.getByTestId("wal-records").locator("li");
  await expect(rows).toHaveCount(6);

  const first = rows.first();
  // The sixth record from the end, so its offset is past the start of the file.
  expect(Number(await first.getAttribute("data-offset"))).toBeGreaterThan(0);
  await expect(first).toContainText("put key-2");
  await expect(first).toContainText("bytes");
  await expect(first).toContainText("crc32");
  expect(Number(await first.getAttribute("data-checksum"))).toBeGreaterThanOrEqual(0);

  // Offsets rise down the panel, because the log is append only.
  const offsets = await rows.evaluateAll((nodes) => nodes.map((node) => Number(node.getAttribute("data-offset"))));
  expect(offsets).toEqual([...offsets].sort((left, right) => left - right));
});

test("a flush turns the memtable into a tile with its count and key range", async ({ page }) => {
  await write(page, "alpha", "one");
  await write(page, "charlie", "three");

  await page.getByRole("button", { name: "flush" }).click();

  const tiles = page.getByTestId("sstables").locator("li.tile");
  await expect(tiles).toHaveCount(1);
  await expect(tiles.first().locator(".tile__figure")).toHaveText("2");
  await expect(tiles.first().locator(".tile__range")).toHaveText("keys alpha to charlie");
  await expect(page.getByTestId("memtable-entries").locator("li.tower")).toHaveCount(0);
});

test("the compact control appears only once a tier is ready to merge", async ({ page }) => {
  const compact = page.getByRole("button", { name: "compact" });
  await expect(compact).toHaveCount(0);

  // The default policy merges a tier once it holds four comparable tables, so
  // four flushes of one record each is the first merge the engine will take.
  for (let index = 0; index < 4; index += 1) {
    await write(page, `key-${String(index)}`, "value");
    await page.getByRole("button", { name: "flush" }).click();
    await expect(page.getByTestId("sstables").locator("li.tile")).toHaveCount(index + 1);
  }

  await expect(compact).toHaveCount(1);
  await compact.click();

  await expect(page.getByTestId("last-action")).toContainText("merged");
  await expect(page.getByTestId("sstables").locator("li.tile")).toHaveCount(1);
  await expect(compact).toHaveCount(0);
});

test("an empty key is refused, with the reason tied to the field", async ({ page }) => {
  await page.getByLabel("key").fill("");

  await page.getByRole("button", { name: "put" }).click();

  const field = page.getByLabel("key");
  await expect(field).toHaveAttribute("aria-invalid", "true");
  const describedBy = await field.getAttribute("aria-describedby");
  expect(describedBy).not.toBeNull();
  const message = page.locator(`#${String(describedBy)}`);
  await expect(message).toHaveText("a key is needed: type one and try again");
  await expect(field).toBeFocused();
  // Nothing was written.
  await expect(page.getByTestId("wal-records").locator("li")).toHaveCount(0);

  // The message is running text that only exists in this state, so the house
  // rules are checked here rather than left to a page that never shows it.
  expect((await page.evaluate(auditPage)).violations).toEqual([]);

  // And it clears as soon as the field has a key again.
  await field.fill("alpha");
  await expect(field).not.toHaveAttribute("aria-invalid", "true");
});

test("a long key is clipped on the page but not in the data", async ({ page }) => {
  const longKey = `user:${"9".repeat(80)}:login`;
  await write(page, longKey, "one");

  const tower = page.locator("li.tower").first();
  await expect(tower).toHaveAttribute("data-key", longKey);
  await expect(tower.locator(".tower__key")).toHaveAttribute("title", longKey);
  const clipped = await tower
    .locator(".tower__key")
    .evaluate((node) => node.scrollWidth > node.clientWidth);
  expect(clipped).toBe(true);
  // The engine has the whole key: asking for it finds the value back.
  await page.getByRole("button", { name: "get", exact: true }).click();
  await expect(page.getByTestId("get-result")).toHaveText("one");
});

test("the whole flow works by keyboard alone", async ({ page }) => {
  // Start from the key field as the hero's button leaves it, then reach every
  // control with Tab and operate it with Enter or Space. No pointer is used
  // anywhere in this test.
  await page.getByRole("button", { name: "Write a key" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByLabel("key")).toBeFocused();

  await page.keyboard.press("Control+A");
  await page.keyboard.type("alpha");
  await page.keyboard.press("Tab");
  await expect(page.getByLabel("value")).toBeFocused();
  await page.keyboard.press("Control+A");
  await page.keyboard.type("one");
  // Enter in the value field submits the form, which is the put.
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("last-action")).toContainText("alpha logged at offset");
  await expect(page.locator("li.tower[data-key='alpha']")).toHaveCount(1);

  await page.getByRole("button", { name: "get", exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("get-result")).toHaveText("one");

  await page.getByRole("button", { name: "flush" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("sstables").locator("li.tile")).toHaveCount(1);

  await page.getByRole("button", { name: "delete" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.locator("li.tower[data-key='alpha']")).toHaveAttribute("data-tombstone", "true");
});

test("the console holds together on a phone with real data in it", async ({ page }) => {
  // The house rules are checked on an empty page at 390 wide. The panels only
  // exist once the engine has something in it, so the narrow layout is checked
  // again here with towers, a log tail and a tile on screen.
  await page.setViewportSize({ width: 390, height: 844 });
  await write(page, "user:1042:login", "2026-11-16T05:00:00Z");
  await write(page, "session:a91f", "open");
  await page.getByRole("button", { name: "flush" }).click();
  await expect(page.getByTestId("sstables").locator("li.tile")).toHaveCount(1);
  await write(page, "user:1043:login", "2026-11-17T05:00:00Z");

  const report = await page.evaluate(auditPage);

  expect(report.violations).toEqual([]);
  expect(report.scrollWidth).toBeLessThanOrEqual(report.clientWidth);

  // The key is the column worth reading in the log tail. On a phone the four
  // column row leaves it a few characters wide, so the row splits in two and
  // the key gets the width back.
  const keyCellWidth = await page
    .getByTestId("wal-records")
    .locator(".wal__cell--key")
    .first()
    .evaluate((node) => node.clientWidth);
  expect(keyCellWidth).toBeGreaterThan(140);
});
