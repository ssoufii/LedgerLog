import { expect, test, type Page } from "@playwright/test";

import { auditPage } from "./support/house-rules";

/**
 * Story M11.5's contract: the strip follows one real record through the four
 * stages of the write path, and shows nothing it was not told.
 *
 * The assertions compare what a card says against what the console says about
 * the same engine, or against the engine's own numbers read off the page's
 * data attributes. Nothing here checks a card against a constant the component
 * also uses, which would only prove the component is self consistent.
 */

const BOOT_TIMEOUT = 30_000;

/** The key `session.py` writes when it opens the database. */
const DEMO_KEY = "user:1042:login";

async function openReady(page: Page): Promise<void> {
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
  await expect(card(page, "log")).toContainText("offset");
}

function field(page: Page, name: string) {
  return page.locator(`.strip__field[data-field="${name}"]`);
}

function card(page: Page, name: string) {
  return field(page, name).locator(".record-card");
}

function row(page: Page, name: string, label: string) {
  return card(page, name).locator(`[data-row="${label}"] dd`);
}

async function write(page: Page, key: string, value: string): Promise<void> {
  await page.getByLabel("key").fill(key);
  await page.getByLabel("value").fill(value);
  await page.getByRole("button", { name: "put", exact: true }).click();
  await expect(page.getByTestId("last-action")).toContainText(`${key} logged at offset`);
}

test.beforeEach(async ({ page }) => {
  await openReady(page);
});

test("the strip is four fields, one card each, in the colour steps the spec sets", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await expect(page.locator(".strip__field")).toHaveCount(4);
  await expect(page.locator(".strip .record-card")).toHaveCount(4);

  const grounds = await page
    .locator(".strip__field")
    .evaluateAll((nodes) => nodes.map((node) => getComputedStyle(node).backgroundColor));
  // Four distinct fields, stepping rather than repeating.
  expect(new Set(grounds).size).toBe(4);

  // Side by side at desktop width, with no gaps between them.
  const boxes = await page
    .locator(".strip__field")
    .evaluateAll((nodes) => nodes.map((node) => node.getBoundingClientRect()));
  for (let index = 1; index < boxes.length; index += 1) {
    expect(boxes[index]?.top).toBeCloseTo(boxes[0]?.top ?? -1, 0);
    expect(boxes[index]?.left).toBeCloseTo((boxes[index - 1]?.right ?? -1), 0);
  }

  // One clause per field, in the order section 6 gives them.
  await expect(page.locator(".strip__clause")).toHaveText([
    "First it lands in the log.",
    "Then it is sorted in memory.",
    "Then it is flushed to disk.",
    "Then compaction merges it.",
  ]);
});

test("before the visitor writes, the strip follows the record the engine wrote at boot", async ({
  page,
}) => {
  await expect(card(page, "log")).toContainText(DEMO_KEY);
  await expect(card(page, "memory")).toContainText(DEMO_KEY);

  // The log card's numbers are the engine's: the same record is in the console's
  // own log tail, which reads the snapshot rather than the locate call.
  const walRow = page.getByTestId("wal-records").locator("li").last();
  const offset = await walRow.getAttribute("data-offset");
  const checksum = await walRow.getAttribute("data-checksum");
  await expect(row(page, "log", "offset")).toHaveText(String(offset));
  await expect(row(page, "log", "crc32")).toHaveText(String(checksum));
  await expect(row(page, "log", "kind")).toHaveText("put");
});

test("card one's offset is the offset the engine gave the visitor's write", async ({ page }) => {
  await write(page, "session:a91f", "open");

  const walRow = page.getByTestId("wal-records").locator("li").last();
  const offset = await walRow.getAttribute("data-offset");
  const length = await walRow.textContent();

  await expect(card(page, "log")).toContainText("session:a91f");
  await expect(row(page, "log", "offset")).toHaveText(String(offset));
  await expect(row(page, "log", "length")).toHaveText(/^\d+ bytes$/);
  expect(length).toContain(String(offset));
  // And the console agrees about where the record went.
  await expect(page.getByTestId("last-action")).toContainText(`logged at offset ${String(offset)}`);
});

test("card two draws the tower at the level the memtable gave the key", async ({ page }) => {
  await write(page, "session:a91f", "open");

  const tower = page.locator("li.tower[data-key='session:a91f']");
  const level = Number(await tower.getAttribute("data-level"));

  await expect(row(page, "memory", "level")).toHaveText(String(level));
  await expect(row(page, "memory", "value")).toHaveText("open");
  await expect(row(page, "memory", "held in")).toHaveText("the active table");
  // The card's own tower is drawn to the same height as the console's.
  const blocks = await card(page, "memory").locator(".tower__block").count();
  expect(blocks).toBe(level);
});

test("a stage the record has not reached says so and offers the action", async ({ page }) => {
  // Nothing has been flushed, so the key is not on disk and no merge has run.
  await expect(card(page, "disk")).toContainText("not flushed yet");
  await expect(card(page, "merge")).toContainText("a tier merges at");
  await expect(card(page, "disk")).toHaveAttribute("data-present", "false");

  // The card still names the key it is waiting for, and shows no numbers for it.
  await expect(card(page, "disk")).toContainText(DEMO_KEY);
  await expect(card(page, "disk").locator(".record-card__row")).toHaveCount(0);

  // The offered action is the one that gets the record there.
  await card(page, "disk").getByRole("button", { name: "flush it" }).click();

  await expect(card(page, "disk")).toHaveAttribute("data-present", "true");
  await expect(card(page, "disk")).not.toContainText("not flushed yet");
});

test("after a flush, card three names the table and how it is found in it", async ({ page }) => {
  await write(page, "session:a91f", "open");
  await page.getByRole("button", { name: "flush", exact: true }).click();

  const tile = page.getByTestId("sstables").locator("li.tile").first();
  const sequence = await tile.getAttribute("data-sequence");

  await expect(row(page, "disk", "table")).toHaveText(String(sequence));
  // Sparse, so the index either points at this key or sends a reader scanning
  // from an earlier one. Both are real answers; an invented third is not.
  await expect(row(page, "disk", "sparse index")).toHaveText(/points at it|scans from .+/);
  // The filter cannot give a false negative, so every one of the key's bits is
  // set in the table that holds it.
  const bloom = await row(page, "disk", "bloom bits").textContent();
  const [set, total] = (bloom ?? "").match(/(\d+) of (\d+) set/)?.slice(1) ?? [];
  expect(Number(set)).toBeGreaterThan(0);
  expect(Number(set)).toBe(Number(total));

  // And the record has left memory, which the memory card says rather than
  // leaving its old numbers on screen.
  await expect(card(page, "memory")).toContainText("flushed out of memory");
  await expect(card(page, "memory").locator(".record-card__row")).toHaveCount(0);
});

test("after a compaction, card four reports the merge the engine ran", async ({ page }) => {
  // Four comparable tables is what the default policy merges at.
  for (let index = 0; index < 4; index += 1) {
    await write(page, "session:a91f", `open-${String(index)}`);
    await page.getByRole("button", { name: "flush", exact: true }).click();
    await expect(page.getByTestId("sstables").locator("li.tile")).toHaveCount(index + 1);
  }

  await expect(card(page, "merge")).toContainText("a tier is ready to merge");
  await card(page, "merge").getByRole("button", { name: "merge it" }).click();

  await expect(page.getByTestId("sstables").locator("li.tile")).toHaveCount(1);
  const merged = page.getByTestId("sstables").locator("li.tile").first();
  const sequence = await merged.getAttribute("data-sequence");

  await expect(row(page, "merge", "into")).toHaveText(String(sequence));
  await expect(row(page, "merge", "key now in")).toHaveText(String(sequence));
  await expect(row(page, "merge", "merged")).toHaveText(/\d+(, \d+)+/);
  // Four copies of one key merge to one, so three were dropped.
  await expect(row(page, "merge", "dropped")).toHaveText("3 records");
  // The console's own readout agrees about what the merge did.
  await expect(page.getByTestId("last-action")).toContainText("dropping 3 records");
});

test("the strip and the console never show two different engines", async ({ page }) => {
  await write(page, "alpha", "one");
  await write(page, "bravo", "two");

  // The strip follows the most recent key, not the first one.
  await expect(card(page, "log")).toContainText("bravo");
  await expect(card(page, "log")).not.toContainText("alpha");
  const level = Number(await page.locator("li.tower[data-key='bravo']").getAttribute("data-level"));
  await expect(row(page, "memory", "level")).toHaveText(String(level));
});

test("the heading and the first card open on the same margin", async ({ page }) => {
  for (const [width, height] of [
    [1440, 900],
    [768, 1024],
    [390, 844],
  ] as const) {
    await page.setViewportSize({ width, height });
    const edges = await page.evaluate(() => ({
      heading: document.querySelector(".strip__heading")?.getBoundingClientRect().left ?? -1,
      card: document.querySelector(".strip .record-card")?.getBoundingClientRect().left ?? -2,
      headline: document.querySelector(".hero__title")?.getBoundingClientRect().left ?? -3,
    }));
    expect(edges.card, `card edge at ${String(width)}`).toBeCloseTo(edges.heading, 0);
    expect(edges.heading, `heading edge at ${String(width)}`).toBeCloseTo(edges.headline, 0);
  }
});

test("the strip holds the house rules at every width", async ({ page }) => {
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
