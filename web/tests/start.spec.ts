import { expect, test, type Page } from "@playwright/test";

import { auditPage } from "./support/house-rules";

/**
 * Story M11.8's contract: the page ends with the code a visitor needs, written
 * for their choices, and one press copies it.
 *
 * The snippet is checked against literal Python written out here, not against
 * the template the component fills in, so a template edit that broke the call
 * would fail this spec as well as pytest. That the code runs is pytest's job
 * (`tests/test_snippets.py` executes it for every fsync policy).
 */

const REPO_URL = "https://github.com/ssoufii/LedgerLog";

function snippet(page: Page) {
  return page.getByTestId("start-snippet");
}

/**
 * The constructor call as the snippet lays it out, one argument per line so
 * that it reads whole inside a 440px dialog. Written with the directory as
 * Python source (already quoted), the way it should appear.
 */
function call(directorySource: string, policy: string): string {
  return `with LedgerLog(\n    ${directorySource},\n    fsync_policy="${policy}",\n) as db:`;
}

function option(page: Page, policy: string) {
  return page.locator(`.start-dialog__option[data-policy="${policy}"]`);
}

test.beforeEach(async ({ page, context, baseURL }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"], { origin: baseURL ?? "" });
  await page.goto("/");
  await page.locator("#start").scrollIntoViewIfNeeded();
});

test("the start section is the closing line beside an open new database dialog", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });

  const section = page.locator("#start");
  await expect(section.getByRole("heading", { level: 2 })).toHaveText("start writing.");
  const dialog = section.getByRole("dialog", { name: "new database" });
  await expect(dialog).toBeVisible();
  await expect(dialog).toHaveAttribute("data-ui", /.+/);

  // The line on the left, the dialog on the right, as section 6 lays them out.
  const heading = await section.getByRole("heading", { level: 2 }).boundingBox();
  const box = await dialog.boundingBox();
  expect(heading).not.toBeNull();
  expect(box).not.toBeNull();
  expect(box?.x ?? 0).toBeGreaterThan(1440 / 2);
  expect((heading?.x ?? 0) + (heading?.width ?? 0)).toBeLessThan(box?.x ?? 0);
  expect(box?.width).toBeLessThanOrEqual(440);

  // The default snippet is the call the spec quotes, one argument per line.
  await expect(snippet(page)).toContainText(call('"./data"', "always"));
  await expect(option(page, "always")).toHaveAttribute("aria-checked", "true");
});

test("changing the directory and the fsync choice changes the snippet", async ({ page }) => {
  const directory = page.getByLabel("directory");
  await directory.fill("/var/lib/orders");
  await expect(snippet(page)).toContainText(call('"/var/lib/orders"', "always"));

  await option(page, "never").click();
  await expect(snippet(page)).toContainText(call('"/var/lib/orders"', "never"));

  await option(page, "interval").click();
  await expect(snippet(page)).toContainText(call('"/var/lib/orders"', "interval"));

  // A quote typed into the field is escaped, not spliced into the code.
  await directory.fill('my "db"');
  await expect(snippet(page)).toContainText(call('"my \\"db\\""', "interval"));

  // The put and the get are there whatever was chosen.
  await expect(snippet(page)).toContainText('db.put(b"user:1042", b"hello")');
  await expect(snippet(page)).toContainText('db.get(b"user:1042")');
});

test("the copy button writes the snippet to the clipboard and says copied for 1.5 seconds", async ({
  page,
}) => {
  await page.getByLabel("directory").fill("./orders");
  await option(page, "never").click();
  const shown = await snippet(page).innerText();

  const copy = page.getByTestId("start-copy");
  await copy.click();

  await expect(copy).toHaveText("copied");
  const clipboard = await page.evaluate(() => navigator.clipboard.readText());
  expect(clipboard.trim()).toBe(shown.trim());
  expect(clipboard).toContain(call('"./orders"', "never"));

  // Still saying so a second in, and back to its label shortly after 1.5s.
  await page.waitForTimeout(1000);
  await expect(copy).toHaveText("copied");
  await expect(copy).toHaveText("copy the code", { timeout: 2000 });
});

test("the whole dialog works by keyboard", async ({ page }) => {
  const directory = page.getByLabel("directory");
  await directory.focus();
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.type("./kb");

  // Tab into the radio group lands on the chosen option; arrows move the choice.
  await page.keyboard.press("Tab");
  await expect(option(page, "always")).toBeFocused();
  await page.keyboard.press("ArrowLeft");
  await expect(option(page, "interval")).toBeFocused();
  await expect(option(page, "interval")).toHaveAttribute("aria-checked", "true");
  await page.keyboard.press("ArrowRight");
  await page.keyboard.press("ArrowRight");
  await expect(option(page, "never")).toHaveAttribute("aria-checked", "true");
  await expect(snippet(page)).toContainText(call('"./kb"', "never"));

  // Past the code block to the button, and Enter copies.
  await page.keyboard.press("Tab");
  await expect(snippet(page)).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(page.getByTestId("start-copy")).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("start-copy")).toHaveText("copied");
  const clipboard = await page.evaluate(() => navigator.clipboard.readText());
  expect(clipboard).toContain(call('"./kb"', "never"));
});

test("an empty directory is refused with a message tied to the field", async ({ page }) => {
  const directory = page.getByLabel("directory");
  await directory.fill("   ");

  await expect(directory).toHaveAttribute("aria-invalid", "true");
  await expect(directory).toHaveAccessibleDescription("name a directory for the database");
  await expect(page.getByTestId("start-copy")).toBeDisabled();
});

test("a link under the dialog goes to the repository", async ({ page }) => {
  const link = page.getByRole("link", { name: "read the source on github" });
  await expect(link).toHaveAttribute("href", REPO_URL);
});

for (const width of [1440, 768, 390]) {
  test(`the start section keeps the house rules at ${String(width)} wide`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.locator("#start").scrollIntoViewIfNeeded();

    const report = await page.evaluate(auditPage);

    expect(report.violations).toEqual([]);
    expect(report.scrollWidth).toBeLessThanOrEqual(report.clientWidth);
    const box = await page.getByRole("dialog", { name: "new database" }).boundingBox();
    expect((box?.x ?? -1) >= 0 && (box?.x ?? 0) + (box?.width ?? 0) <= width).toBe(true);
  });
}
