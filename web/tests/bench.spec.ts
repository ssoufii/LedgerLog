import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { expect, test, type Page } from "@playwright/test";

import { auditPage } from "./support/house-rules";

/**
 * Story M11.6's contract: the durability dial and the numbers slider show
 * measured figures, and the figures are the README's.
 *
 * The page's numbers are compared against the README parsed here, with a
 * regex written for this test rather than imported from the generator. A test
 * that read `bench.json` and compared it to the page would prove the page can
 * read a file; what has to be true is that the figure a visitor sees is the
 * one the bench recorded and wrote down.
 */

const BOOT_TIMEOUT = 30_000;
const README = readFileSync(join("..", "README.md"), "utf8");

/** The throughput the README's table gives for one policy. */
function readmeThroughput(policy: string): number {
  const row = new RegExp(`^\\|\\s*\`${policy}\`[^|]*\\|\\s*([\\d,]+) records/s`, "m").exec(README);
  expect(row, `the README has no throughput row for ${policy}`).not.toBeNull();
  return Number((row?.[1] ?? "0").replace(/,/g, ""));
}

async function openReady(page: Page): Promise<void> {
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
}

test("bench.json and the README agree", () => {
  // The generator re-derives the file and exits non-zero if the committed copy
  // has drifted, so the check and the build share one parser.
  execFileSync("node", ["scripts/bench-data.mjs", "--check"], { stdio: "pipe" });
});

test("the README has the three policies this page shows", () => {
  expect(readmeThroughput("never")).toBe(64466);
  expect(readmeThroughput("interval")).toBe(67053);
  expect(readmeThroughput("always")).toBe(23419);
});

test.describe("the durability dial", () => {
  test.beforeEach(async ({ page }) => {
    await openReady(page);
  });

  test("choosing each policy shows the figure the README recorded for it", async ({ page }) => {
    for (const policy of ["never", "interval", "always"] as const) {
      await page.locator(`.dial__option[data-policy="${policy}"]`).click();

      const expected = readmeThroughput(policy).toLocaleString("en-GB");
      await expect(page.getByTestId("durability-figures")).toContainText(expected);
      await expect(page.getByTestId("durability-figures")).toContainText("records/s");
    }
  });

  test("the heading swaps one word, and the field swaps ground", async ({ page }) => {
    const word = page.getByTestId("durability-word");
    const section = page.locator("#durability");
    await expect(word).toHaveText("and");
    await expect(section).toContainText("fast and durable?");
    const resting = await section.evaluate((node) => getComputedStyle(node).backgroundColor);

    await page.locator('.dial__option[data-policy="always"]').click();

    await expect(word).toHaveText("not");
    await expect(section).toContainText("durable, not fast");
    await expect(section).toHaveAttribute("data-ground", "deep");
    // The ground crossfades, so the painted colour is polled rather than read
    // once: reading it on the click would catch the transition's first frame.
    await expect
      .poll(async () =>
        section.evaluate((node) => getComputedStyle(node).backgroundColor),
      )
      .not.toBe(resting);

    await page.locator('.dial__option[data-policy="never"]').click();
    await expect(section).toContainText("fast, not durable");
    await expect(section).toHaveAttribute("data-ground", "grey");
  });

  test("arrow keys move between the options", async ({ page }) => {
    await page.locator('.dial__option[data-policy="never"]').focus();
    await page.keyboard.press("ArrowRight");

    await expect(page.locator('.dial__option[data-policy="interval"]')).toHaveAttribute(
      "aria-checked",
      "true",
    );
    await expect(page.locator('.dial__option[data-policy="interval"]')).toBeFocused();

    await page.keyboard.press("ArrowRight");
    await expect(page.locator('.dial__option[data-policy="always"]')).toHaveAttribute(
      "aria-checked",
      "true",
    );

    // And it wraps, so the group can be walked in one direction.
    await page.keyboard.press("ArrowRight");
    await expect(page.locator('.dial__option[data-policy="never"]')).toHaveAttribute(
      "aria-checked",
      "true",
    );

    await page.keyboard.press("ArrowLeft");
    await expect(page.locator('.dial__option[data-policy="always"]')).toHaveAttribute(
      "aria-checked",
      "true",
    );
  });

  test("the invocation on the page is the one in the README", async ({ page }) => {
    const shown = await page
      .locator("#durability .invocation code")
      .evaluate((node) => node.textContent ?? "");

    expect(README).toContain(shown);
    expect(shown).toContain("--fsync-policy {never|interval|always}");
  });
});

test.describe("the numbers slider", () => {
  test.beforeEach(async ({ page }) => {
    await openReady(page);
  });

  test("at one million records the figure is 15.5 s", async ({ page }) => {
    await expect(page.getByTestId("records-count")).toHaveText("1,000,000");
    await expect(page.getByTestId("records-seconds")).toHaveText("15.5s");

    // And that is the division it says it is.
    const rate = readmeThroughput("never");
    expect((1_000_000 / rate).toFixed(1)).toBe("15.5");
    await expect(page.locator("#numbers")).toContainText(
      `1,000,000 records divided by ${rate.toLocaleString("en-GB")} records per second`,
    );
  });

  test("arrow keys move the slider, and the figure follows", async ({ page }) => {
    const slider = page.getByTestId("records-slider");
    await slider.focus();
    const before = await page.getByTestId("records-count").textContent();

    await page.keyboard.press("ArrowRight");

    const after = await page.getByTestId("records-count").textContent();
    expect(after).not.toBe(before);
    expect(Number((after ?? "0").replace(/,/g, ""))).toBeGreaterThan(
      Number((before ?? "0").replace(/,/g, "")),
    );

    await page.keyboard.press("ArrowLeft");
    await expect(page.getByTestId("records-count")).toHaveText(String(before));
  });

  test("the slider is logarithmic across its whole range", async ({ page }) => {
    const slider = page.getByTestId("records-slider");
    await slider.focus();
    await page.keyboard.press("Home");
    await expect(page.getByTestId("records-count")).toHaveText("10,000");
    // Ten thousand records at the measured rate.
    await expect(page.getByTestId("records-seconds")).toHaveText("0.155s");

    await page.keyboard.press("End");
    await expect(page.getByTestId("records-count")).toHaveText("100,000,000");
    await expect(page.getByTestId("records-seconds")).toHaveText("1,551s");
  });
});

test("the new sections hold the house rules at every width", async ({ page }) => {
  await openReady(page);
  await page.locator('.dial__option[data-policy="always"]').click();

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
