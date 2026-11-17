import type { Page } from "@playwright/test";

/**
 * Scroll the page through once, top to bottom, and wait for every entrance to
 * finish, then return to the top.
 *
 * Section entrances (docs/FRONTEND_DESIGN.md section 8) run when a section is
 * scrolled into view, the way a visitor meets it. An audit or a full-page
 * screenshot taken straight after load would otherwise see the sections below
 * the fold in their waiting state, which no visitor ever sees.
 */
export async function revealAll(page: Page): Promise<void> {
  await page.evaluate(async () => {
    const step = Math.max(200, Math.floor(window.innerHeight / 2));
    for (let y = 0; y <= document.documentElement.scrollHeight; y += step) {
      window.scrollTo(0, y);
      await new Promise((resolve) => requestAnimationFrame(() => setTimeout(resolve, 30)));
    }
    window.scrollTo(0, document.documentElement.scrollHeight);
    await new Promise((resolve) => setTimeout(resolve, 60));
    await Promise.all(
      document
        .getAnimations()
        .filter((animation) => animation.effect?.getTiming().iterations !== Infinity)
        .map((animation) => animation.finished.catch(() => undefined)),
    );
    window.scrollTo(0, 0);
  });
}
