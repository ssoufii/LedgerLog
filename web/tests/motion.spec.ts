import { expect, test, type Page } from "@playwright/test";

/**
 * Story M11.9's contract: the motion in section 8 of docs/FRONTEND_DESIGN.md
 * is built, follows its rules, and never costs a visitor any content.
 *
 * The rule checks read the CSSOM the page actually loaded rather than the
 * source files, so they see exactly what the browser applies, including
 * anything a dependency injected.
 */

const BOOT_TIMEOUT = 30_000;

async function openReady(page: Page): Promise<void> {
  await page.goto("/");
  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
}

const reduced = (name: string): boolean => name.includes("reduced");

test("no transition: all, no ease-in, and nothing animates layout", async ({ page }) => {
  await page.goto("/");

  const { offences, transitions, keyframes } = await page.evaluate(() => {
    const found: string[] = [];
    let transitionCount = 0;
    let keyframeCount = 0;
    const LAYOUT = /^(width|height|top|left|right|bottom|margin.*|padding.*|inset.*|min-.*|max-.*)$/;
    const EASE_IN = /\bease-in\b(?!-out)/;

    const declarations = (text: string, name: string): string[] => {
      const values: string[] = [];
      const pattern = new RegExp(`(?:^|[;{\\s])${name}\\s*:\\s*([^;}]+)`, "g");
      for (const match of text.matchAll(pattern)) {
        values.push((match[1] ?? "").trim());
      }
      return values;
    };

    // The first word of each comma-separated transition is its property,
    // unless the list starts with a duration (then it applies to all).
    const transitioned = (value: string): string[] =>
      value.split(/,(?![^(]*\))/).map((part) => {
        const first = part.trim().split(/\s+/)[0] ?? "";
        return /^[\d.]|^var\(--t-/.test(first) ? "all" : first;
      });

    const walk = (rules: CSSRuleList, where: string): void => {
      for (const rule of Array.from(rules)) {
        if (rule instanceof CSSKeyframesRule) {
          keyframeCount += 1;
          for (const frame of Array.from(rule.cssRules)) {
            const style = (frame as CSSKeyframeRule).style;
            for (const property of Array.from(style)) {
              if (LAYOUT.test(property)) {
                found.push(`@keyframes ${rule.name} animates ${property}`);
              }
            }
          }
          continue;
        }
        if (rule instanceof CSSGroupingRule) {
          walk(rule.cssRules, where);
          continue;
        }
        if (!(rule instanceof CSSStyleRule)) {
          continue;
        }
        const text = rule.cssText;
        const selector = rule.selectorText;
        for (const value of [...declarations(text, "transition"), ...declarations(text, "transition-property")]) {
          transitionCount += 1;
          for (const property of transitioned(value)) {
            if (property === "all") {
              found.push(`${selector} transitions all (${value})`);
            } else if (LAYOUT.test(property)) {
              found.push(`${selector} transitions ${property}`);
            }
          }
        }
        for (const name of ["transition", "transition-timing-function", "animation", "animation-timing-function"]) {
          for (const value of declarations(text, name)) {
            if (EASE_IN.test(value)) {
              found.push(`${selector} uses ease-in (${value})`);
            }
          }
        }
      }
    };

    for (const sheet of Array.from(document.styleSheets)) {
      let rules: CSSRuleList;
      try {
        rules = sheet.cssRules;
      } catch {
        continue; // The font provider's sheet is cross-origin and unreadable.
      }
      walk(rules, sheet.href ?? "inline");
    }
    return { offences: found, transitions: transitionCount, keyframes: keyframeCount };
  });

  // The walk saw the page's motion, so an empty list means something.
  expect(transitions).toBeGreaterThan(10);
  expect(keyframes).toBeGreaterThanOrEqual(6);
  expect(offences).toEqual([]);
});

test("hover styles live only behind the hover media query", async ({ page }) => {
  await page.goto("/");

  const bare = await page.evaluate(() => {
    const found: string[] = [];
    const walk = (rules: CSSRuleList, guarded: boolean): void => {
      for (const rule of Array.from(rules)) {
        if (rule instanceof CSSMediaRule) {
          const hover = /\(hover:\s*hover\)/.test(rule.conditionText);
          walk(rule.cssRules, guarded || hover);
        } else if (rule instanceof CSSGroupingRule) {
          walk(rule.cssRules, guarded);
        } else if (rule instanceof CSSStyleRule && rule.selectorText.includes(":hover") && !guarded) {
          found.push(rule.selectorText);
        }
      }
    };
    for (const sheet of Array.from(document.styleSheets)) {
      try {
        walk(sheet.cssRules, false);
      } catch {
        // cross-origin
      }
    }
    return found;
  });

  expect(bare).toEqual([]);
});

test("at least eight elements animate while the page loads and is scrolled", async ({ page }, testInfo) => {
  test.skip(reduced(testInfo.project.name), "reduced motion is checked by its own test");

  await page.addInitScript(() => {
    const seen = new Set<Element>();
    (window as unknown as { __animated: Set<Element> }).__animated = seen;
    const sample = () => {
      for (const animation of document.getAnimations()) {
        const target = (animation.effect as KeyframeEffect | null)?.target;
        if (target) {
          seen.add(target);
        }
      }
      requestAnimationFrame(sample);
    };
    requestAnimationFrame(sample);
  });
  await openReady(page);
  for (let y = 0; y < 8000; y += 300) {
    await page.mouse.wheel(0, 300);
    await page.waitForTimeout(60);
  }

  const count = await page.evaluate(() => (window as unknown as { __animated: Set<Element> }).__animated.size);
  expect(count).toBeGreaterThanOrEqual(8);
});

test("the hero stage opens with the shutter, once", async ({ page }, testInfo) => {
  test.skip(reduced(testInfo.project.name), "there is no shutter under reduced motion");
  await page.goto("/");

  const names = await page
    .locator(".stage")
    .evaluate((stage) => stage.getAnimations().map((animation) => (animation as CSSAnimation).animationName));
  expect(names).toContain("stage-shutter");

  await expect(page.getByTestId("boot-status")).toHaveText("ready", { timeout: BOOT_TIMEOUT });
  await page.getByRole("button", { name: "put", exact: true }).click();
  await page.waitForTimeout(1500);
  const running = await page
    .locator(".stage")
    .evaluate((stage) => stage.getAnimations().filter((animation) => animation.playState === "running").length);
  expect(running).toBe(0);
});

test("under reduced motion everything is visible at once and nothing travels or scales", async ({
  page,
}, testInfo) => {
  test.skip(!reduced(testInfo.project.name), "runs in the reduced motion project");
  await openReady(page);

  const hidden = await page.evaluate(() =>
    Array.from(document.querySelectorAll<HTMLElement>("[data-reveal]"))
      .filter((element) => {
        const style = getComputedStyle(element);
        return style.opacity !== "1" || style.transform !== "none";
      })
      .map((element) => element.className),
  );
  expect(hidden).toEqual([]);

  // Drive the console the way a visitor would, then look at every animation
  // that ran: none may move or scale anything.
  await page.getByLabel("key").fill("reduced");
  await page.getByRole("button", { name: "put", exact: true }).click();
  await page.getByRole("button", { name: "flush", exact: true }).click();
  await page.locator('.dial__option[data-policy="always"]').click();

  const moving = await page.evaluate(() =>
    document.getAnimations().flatMap((animation) => {
      const effect = animation.effect as KeyframeEffect | null;
      const frames = effect?.getKeyframes() ?? [];
      const travels = frames.some((frame) => "transform" in frame || "translate" in frame || "scale" in frame);
      const transition = (animation as CSSTransition).transitionProperty;
      return travels || transition === "transform" ? [String(effect?.target?.className ?? "")] : [];
    }),
  );
  expect(moving).toEqual([]);
});

test("no content stays hidden when the motion script cannot run", async ({ page }) => {
  // The entrances need IntersectionObserver; without it the script stops
  // before applying any waiting state, which is the failure this guards.
  await page.addInitScript(() => {
    delete (window as unknown as { IntersectionObserver?: unknown }).IntersectionObserver;
  });
  await page.goto("/");
  await expect(page.locator(".start__heading")).toBeVisible();

  const hidden = await page.evaluate(() =>
    Array.from(document.querySelectorAll<HTMLElement>("[data-reveal]"))
      .filter((element) => getComputedStyle(element).opacity !== "1")
      .map((element) => element.className),
  );
  expect(hidden).toEqual([]);
  expect(await page.evaluate(() => document.documentElement.classList.contains("motion"))).toBe(false);
});

test("a keyboard put fades the tower in, a pointer put builds it", async ({ page }, testInfo) => {
  test.skip(reduced(testInfo.project.name), "under reduced motion both fade");
  await openReady(page);

  const transformsOn = (key: string) =>
    page.locator(`li.tower[data-key='${key}'] .tower__block`).first().evaluate((block) =>
      block
        .getAnimations()
        .some((animation) =>
          ((animation.effect as KeyframeEffect | null)?.getKeyframes() ?? []).some((frame) => "transform" in frame),
        ),
    );

  // Keyboard: Enter in the value field puts (rule 8: no travel).
  await page.getByLabel("key").fill("by-keyboard");
  await page.getByLabel("value").focus();
  await page.keyboard.press("Enter");
  const keyboardTower = page.locator("li.tower[data-key='by-keyboard'] .tower__stack");
  await expect(keyboardTower).toHaveAttribute("data-enter", "fade");
  expect(await transformsOn("by-keyboard")).toBe(false);

  // Pointer: the put button builds the tower block by block.
  await page.getByLabel("key").fill("by-pointer");
  await page.getByRole("button", { name: "put", exact: true }).click();
  const pointerTower = page.locator("li.tower[data-key='by-pointer'] .tower__stack");
  await expect(pointerTower).toHaveAttribute("data-enter", "travel");

  // The flush moves the towers out and a tile rises where they went.
  await page.getByRole("button", { name: "flush", exact: true }).click();
  await expect(page.getByTestId("sstables").locator("li.tile")).toHaveCount(1);
  await expect(page.getByTestId("sstables").locator("li.tile")).toHaveAttribute("data-enter", "travel");
});

test("the segment indicator slides for a pointer and does not travel for the keyboard", async ({
  page,
}, testInfo) => {
  test.skip(reduced(testInfo.project.name), "under reduced motion the indicator never slides");
  await page.goto("/");
  const dial = page.getByTestId("durability-dial");
  await dial.scrollIntoViewIfNeeded();
  const indicator = dial.locator(".segment-indicator");

  const transitioning = () =>
    indicator.evaluate((element) =>
      element.getAnimations().map((animation) => (animation as CSSTransition).transitionProperty),
    );

  await page.locator('.dial__option[data-policy="never"]').click();
  await page.waitForTimeout(400);
  await page.locator('.dial__option[data-policy="always"]').click();
  expect(await transitioning()).toContain("transform");

  await page.waitForTimeout(400);
  await page.keyboard.press("ArrowLeft");
  expect(await transitioning()).not.toContain("transform");
  await expect(page.locator('.dial__option[data-policy="interval"]')).toHaveAttribute("aria-checked", "true");
});

test("every pressable thing gives press feedback", async ({ page }, testInfo) => {
  test.skip(reduced(testInfo.project.name), "presses do not scale under reduced motion");
  await openReady(page);

  const pressables = page.locator("main button:not([disabled]), a[href]");
  const count = await pressables.count();
  expect(count).toBeGreaterThan(10);

  const flat: string[] = [];
  for (let index = 0; index < count; index += 1) {
    const target = pressables.nth(index);
    if (!(await target.isVisible())) {
      continue;
    }
    await target.scrollIntoViewIfNeeded();
    await target.hover();
    await page.mouse.down();
    await page.waitForTimeout(200);
    const transform = await target.evaluate((element) => getComputedStyle(element).transform);
    // Move off before letting go, so the press does not become a click.
    await page.mouse.move(0, 0);
    await page.mouse.up();
    if (!transform.startsWith("matrix(0.97")) {
      flat.push(`${await target.evaluate((element) => element.outerHTML.slice(0, 80))} -> ${transform}`);
    }
  }
  expect(flat).toEqual([]);
});
