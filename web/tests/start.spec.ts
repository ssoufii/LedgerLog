import { readFileSync } from "node:fs";
import { join } from "node:path";

import { expect, test, type Page } from "@playwright/test";

import { auditPage } from "./support/house-rules";

/**
 * Story M11.8's contract: the page ends with code for the choices the visitor
 * made, and copying it takes one press.
 *
 * That the snippet *runs* is `tests/test_snippet.py`'s job, which executes it
 * for every fsync choice. What is checked here is that the page renders the
 * same template those tests run, that the choices reach it, and that the copy
 * button really writes to the clipboard rather than only saying it did.
 */

const BOOT_TIMEOUT = 30_000;
const TEMPLATE = JSON.parse(
  readFileSync(join("src", "data", "snippet.json"), "utf8"),
) as { template: string; default_directory: string; policies: string[] };

async function openReady(page: Page): Promise<void> {
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
}

/**
 * The dialog's own radios.
 *
 * Scoped, because the durability dial further up the page is also a radio
 * group whose options are named never, interval and always. Two controls that
 * offer the same three choices is the page being consistent, not a mistake, so
 * the test says which one it means.
 */
function policyChoice(page: Page, policy: string) {
  return page.locator(".start__dialog").getByRole("radio", { name: policy, exact: true });
}

/**
 * The segment a visitor presses, which is the label around the radio.
 *
 * The radio itself is visually hidden and the label is what receives the
 * click, which is how a segmented control made of radios behaves for everyone;
 * the test presses what a visitor presses.
 */
function policySegment(page: Page, policy: string) {
  return page.locator(".start__option").filter({ hasText: policy });
}

/** The snippet the template says the page should be showing. */
function expected(directory: string, policy: string): string {
  return TEMPLATE.template
    .replace("__DIRECTORY__", directory.replace(/\\/g, "\\\\").replace(/"/g, '\\"'))
    .replace("__POLICY__", policy);
}

test.beforeEach(async ({ page }) => {
  await openReady(page);
});

test("the dialog is open on the page, with the three parts section 6 names", async ({ page }) => {
  const dialog = page.locator(".start__dialog");
  await expect(dialog).toBeVisible();
  await expect(dialog).toHaveAttribute("open", "");
  await expect(page.getByLabel("directory")).toHaveValue(TEMPLATE.default_directory);
  await expect(dialog.getByRole("radio")).toHaveCount(TEMPLATE.policies.length);
  await expect(page.getByTestId("copy-the-code")).toHaveText("copy the code");
  await expect(dialog.getByRole("link")).toHaveAttribute(
    "href",
    "https://github.com/ssoufii/LedgerLog",
  );
  // The closing line is two lines, as section 6 sets it.
  await expect(page.locator(".start__word")).toHaveText(["start", "writing."]);
});

test("the snippet is the template the pytest suite runs", async ({ page }) => {
  await expect(page.getByTestId("start-snippet")).toHaveText(expected("./data", "always"));
});

test("changing the directory changes the snippet", async ({ page }) => {
  await page.getByLabel("directory").fill("/var/lib/ledger");

  await expect(page.getByTestId("start-snippet")).toHaveText(
    expected("/var/lib/ledger", "always"),
  );
  await expect(page.getByTestId("start-snippet")).toContainText('LedgerLog("/var/lib/ledger"');
});

test("changing the fsync choice changes the snippet", async ({ page }) => {
  for (const policy of TEMPLATE.policies) {
    await policySegment(page, policy).click();

    await expect(page.getByTestId("start-snippet")).toHaveText(expected("./data", policy));
    await expect(page.getByTestId("start-snippet")).toContainText(`fsync_policy="${policy}"`);
  }
});

test("a directory with a quote in it is escaped, not pasted through", async ({ page }) => {
  await page.getByLabel("directory").fill('./a "quoted" path');

  await expect(page.getByTestId("start-snippet")).toContainText('"./a \\"quoted\\" path"');
});

test("the copy button writes the snippet to the clipboard and says so for a moment", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await policySegment(page, "never").click();
  const button = page.getByTestId("copy-the-code");

  await button.click();

  await expect(button).toHaveText("copied");
  const clipboard = await page.evaluate(() => navigator.clipboard.readText());
  expect(clipboard).toBe(expected("./data", "never"));

  // Section 6 fixes the confirmation at 1.5 seconds, so it goes on its own.
  await expect(button).toHaveText("copy the code", { timeout: 4000 });
});

test("the whole dialog works by keyboard", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);

  await page.getByLabel("directory").focus();
  await page.keyboard.press("Control+A");
  await page.keyboard.type("./from-the-keyboard");
  await expect(page.getByTestId("start-snippet")).toContainText('"./from-the-keyboard"');

  // Arrow keys move within a radio group, which is what the platform gives a
  // set of radios sharing a name.
  await policyChoice(page, "never").focus();
  await page.keyboard.press("ArrowRight");
  await expect(policyChoice(page, "interval")).toBeChecked();
  await expect(page.getByTestId("start-snippet")).toContainText('fsync_policy="interval"');

  await page.getByTestId("copy-the-code").focus();
  await page.keyboard.press("Enter");

  await expect(page.getByTestId("copy-the-code")).toHaveText("copied");
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(
    expected("./from-the-keyboard", "interval"),
  );
});

test("the start section holds the house rules at every width", async ({ page }) => {
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
