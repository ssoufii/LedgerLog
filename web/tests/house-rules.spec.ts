import { readFileSync, mkdirSync, readdirSync } from "node:fs";
import { join } from "node:path";

import { expect, test } from "@playwright/test";

import { auditPage, stampFocusable } from "./support/house-rules";

/**
 * Story M11.3's contract: the house rules in section 7 of
 * docs/FRONTEND_DESIGN.md hold at 1440, 768 and 390 pixels wide, with and
 * without the font provider, and under a reduced motion preference (which the
 * second Playwright project supplies, running this same file).
 *
 * The audit itself is in `tests/support/house-rules.ts` and runs in the page.
 * What is left here is what only Playwright can do: set a viewport, block a
 * request, walk the tab order with real key presses, use Playwright's own
 * accessible name computation, and save the screenshots the spec asks a human
 * to open.
 */

// Booting CPython in wasm is the slowest thing on the page. The rules are
// checked on the page a visitor actually ends up with, which means after the
// engine has reported itself ready: before that, every console control is
// disabled, and a disabled control is not in the tab order.
const BOOT_TIMEOUT = 30_000;

const WIDTHS: { width: number; height: number }[] = [
  { width: 1440, height: 900 },
  { width: 768, height: 1024 },
  { width: 390, height: 844 },
];

const SCREENSHOT_DIR = join("test-results", "screenshots");

async function openReady(page: import("@playwright/test").Page): Promise<void> {
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
}

for (const { width, height } of WIDTHS) {
  test(`the house rules hold at ${String(width)} wide`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height });
    await openReady(page);

    const report = await page.evaluate(auditPage);

    expect(report.violations).toEqual([]);
    expect(report.h1Count).toBe(1);
    expect(report.mainCount).toBe(1);
    expect(report.sectionCount).toBeGreaterThan(0);
    // No horizontal overflow at any width.
    expect(report.scrollWidth).toBeLessThanOrEqual(report.clientWidth);
    // The page is built, not empty: the headline and the lead are there, and so
    // are the console's controls.
    expect(report.headings.length).toBeGreaterThan(0);
    expect(report.paragraphs.length).toBeGreaterThan(0);
    expect(report.focusable.length).toBeGreaterThan(8);

    // Section 5: equal outer margins on both sides, at every width, and the
    // same margin in all three bands of the page. The frame's own padding says
    // whether the margins are equal; the rects of the elements on the first
    // column say whether anything inside has shifted the content off it.
    const frame = await page.evaluate(() => {
      const padding = (selector: string): { left: number; right: number } | null => {
        const element = document.querySelector(selector);
        if (element === null) {
          return null;
        }
        const style = getComputedStyle(element);
        return { left: Number.parseFloat(style.paddingLeft), right: Number.parseFloat(style.paddingRight) };
      };
      const left = (selector: string): number | null =>
        document.querySelector(selector)?.getBoundingClientRect().left ?? null;
      return {
        bands: { nav: padding(".site-nav"), hero: padding(".hero"), footer: padding(".footer") },
        titleLeft: left(".hero__title"),
        creditLeft: left(".footer__copy"),
      };
    });

    const bands = Object.entries(frame.bands);
    expect(bands.map(([name]) => name)).toEqual(["nav", "hero", "footer"]);
    for (const [name, padding] of bands) {
      expect(padding, `${name} has no padding to measure`).not.toBeNull();
      expect(padding?.left, `${name} left margin`).toBeGreaterThan(0);
      expect(padding?.right, `${name} margins are not equal`).toBeCloseTo(padding?.left ?? -1, 1);
    }
    const margin = frame.bands.nav?.left ?? -1;
    expect(frame.bands.hero?.left).toBeCloseTo(margin, 1);
    expect(frame.bands.footer?.left).toBeCloseTo(margin, 1);
    expect(frame.titleLeft).toBeCloseTo(margin, 0);
    expect(frame.creditLeft).toBeCloseTo(margin, 0);

    mkdirSync(SCREENSHOT_DIR, { recursive: true });
    await page.screenshot({
      path: join(SCREENSHOT_DIR, `${String(width)}-${testInfo.project.name}.png`),
      fullPage: true,
      animations: "disabled",
    });
  });
}

test("the page falls back cleanly with the font provider blocked", async ({ page }) => {
  // A Playwright run may have no network at all, so the rules have to hold on
  // the fallback stack as well as on Pilcrow Rounded. Aborting rather than
  // fulfilling, because that is what an unreachable provider looks like.
  await page.route(/fontshare\.com/, (route) => route.abort());
  await page.setViewportSize({ width: 1440, height: 900 });
  await openReady(page);

  const report = await page.evaluate(auditPage);

  expect(report.violations).toEqual([]);
  expect(report.scrollWidth).toBeLessThanOrEqual(report.clientWidth);
  // The fallback is the rounded system stack, not the webfont and not a serif.
  const family = await page.evaluate(() => getComputedStyle(document.body).fontFamily);
  expect(family).toContain("ui-rounded");
});

test("every control is named and reachable by keyboard", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await openReady(page);

  const expected = await page.evaluate(stampFocusable);
  expect(expected).toBeGreaterThan(8);

  // Named: asked of Playwright, so the check uses the real accessible name
  // computation rather than a guess at one.
  const controls = await page.locator("[data-kb-stamp]").all();
  for (const control of controls) {
    const snapshot = await control.ariaSnapshot();
    expect(snapshot, `a control has no accessible name: ${snapshot}`).toMatch(/"[^"]+"/);
  }

  // Reachable: a real tab walk, from nothing focused, collecting the stamps it
  // lands on. One extra press so that a control skipped mid-order shows up as
  // a missing stamp rather than as a short walk.
  await page.evaluate(() => {
    if (document.activeElement instanceof HTMLElement) {
      document.activeElement.blur();
    }
  });
  const reached = new Set<string>();
  for (let press = 0; press < expected + 1; press += 1) {
    await page.keyboard.press("Tab");
    const stamp = await page.evaluate(() =>
      document.activeElement instanceof HTMLElement ? (document.activeElement.dataset.kbStamp ?? null) : null,
    );
    if (stamp !== null) {
      reached.add(stamp);
    }
  }

  expect(reached.size).toBe(expected);
});

test("the hero button sends focus to the console", async ({ page }) => {
  await openReady(page);

  await page.getByRole("button", { name: "Write a key", exact: true }).click();

  await expect(page.getByLabel("key")).toBeFocused();
});

test("only the token file holds a raw colour or type size", () => {
  // The rule is in docs/FRONTEND_DESIGN.md sections 3 and 4 and in story
  // M11.3: components reference tokens, so the palette and the scale stay one
  // decision each. Checked by reading the stylesheets rather than the page,
  // because a literal that happens to equal a token is still a literal.
  const styleDir = join("src", "styles");
  const tokenFile = "tokens.css";
  const sheets = readdirSync(styleDir).filter((name) => name.endsWith(".css") && name !== tokenFile);
  expect(sheets.length).toBeGreaterThan(0);

  const offences: string[] = [];
  for (const name of sheets) {
    // Comments explain the geometry and quote the spec, so they are not source.
    const body = readFileSync(join(styleDir, name), "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
    for (const [index, line] of body.split("\n").entries()) {
      const where = `${name}:${String(index + 1)}`;
      if (/#[0-9a-fA-F]{3,8}\b/.test(line)) {
        offences.push(`${where} has a hex colour: ${line.trim()}`);
      }
      if (/\b(?:rgba?|hsla?|color-mix|oklch)\(/.test(line)) {
        offences.push(`${where} has a colour function: ${line.trim()}`);
      }
      if (/font-size:(?!\s*var\()/.test(line)) {
        offences.push(`${where} has a literal font size: ${line.trim()}`);
      }
      if (/(?<![-\w])(?:color|background|background-color|fill|stroke):(?!\s*(?:var\(|inherit|transparent|none|currentcolor))/i.test(line)) {
        offences.push(`${where} has a literal colour: ${line.trim()}`);
      }
      if (/font-family:(?!\s*var\()/.test(line)) {
        offences.push(`${where} has a literal font family: ${line.trim()}`);
      }
    }
  }

  expect(offences).toEqual([]);
});
