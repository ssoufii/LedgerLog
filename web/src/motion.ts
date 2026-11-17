/**
 * The motion layer's script: section entrances, input modality, and the one
 * group move that has to be run from code. Section 8 of
 * docs/FRONTEND_DESIGN.md is the rule book; everything that can be CSS is CSS
 * (see the motion block at the end of page.css), and this file only does what
 * a stylesheet cannot.
 *
 * Why entrances are opt-in from here rather than hidden by the stylesheet:
 * rule 11 says content must never stay hidden if a script fails. The resting
 * "not yet entered" state is only applied under the `motion` class, and only
 * this script adds that class, after it has confirmed it can observe
 * intersections and the visitor has not asked for reduced motion. If anything
 * here throws first, or is never loaded, every section is simply visible.
 *
 * Why the input modality is tracked: rule 8 says keyboard-initiated actions do
 * not animate travel. The page cannot tell from a click event alone whether
 * the visitor is on a keyboard (Enter on a button fires `click`), so the last
 * kind of input is recorded on the root element, and the moves that travel
 * read it and fall back to a fade.
 */
import { useState } from "react";

type Entrance = "travel" | "fade";

const REVEAL_SELECTOR = "[data-reveal]";
const OBSERVED = "data-reveal-observed";

function reducedMotion(): boolean {
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

/**
 * How something entering right now should enter: travel for a pointer, a fade
 * for the keyboard or under reduced motion.
 */
export function entranceStyle(): Entrance {
  if (reducedMotion()) {
    return "fade";
  }
  return document.documentElement.dataset.input === "keyboard" ? "fade" : "travel";
}

/**
 * The entrance an element was born with, fixed for its lifetime.
 *
 * Fixed rather than read on every render because the entrance is a CSS
 * keyframe animation chosen by an attribute: if the attribute flipped when the
 * visitor later switched from mouse to keyboard, the browser would see a new
 * animation name and replay the entrance on everything already on screen.
 */
export function useEntrance(): Entrance {
  const [entrance] = useState(entranceStyle);
  return entrance;
}

/**
 * Move a group off as one, then resolve so the caller can replace it.
 *
 * Used for the flush (section 8: "a flush moves the towers out as one group,
 * --ease-in-out, 220ms"). Travel for a pointer; a fade alone for the keyboard
 * and for reduced motion. Exit is faster than any entrance on the page
 * (rule 10). Resolves at once if there is nothing to move, so a flush never
 * waits on the animation to happen.
 */
export async function moveOut(element: Element | null): Promise<void> {
  if (element === null || element.childElementCount === 0) {
    return;
  }
  const travel = entranceStyle() === "travel";
  const animation = element.animate(
    travel
      ? [
          { opacity: 1, transform: "translateX(0)" },
          { opacity: 0, transform: "translateX(32px)" },
        ]
      : [{ opacity: 1 }, { opacity: 0 }],
    { duration: 220, easing: "cubic-bezier(0.77, 0, 0.175, 1)", fill: "forwards" },
  );
  try {
    await animation.finished;
  } catch {
    // Cancelled because the element went away; nothing left to wait for.
  }
}

/** Undo a `moveOut` once the group has been replaced, so the list shows again. */
export function settle(element: Element | null): void {
  for (const animation of element?.getAnimations() ?? []) {
    animation.cancel();
  }
}

/**
 * Start the page's motion: record input modality, and stage section entrances.
 *
 * Entrances run once per element, when it is 100px into the viewport
 * (section 8: `rootMargin: -100px`). New `[data-reveal]` elements are picked up
 * as React mounts them, so a section rendered later still enters.
 */
export function startMotion(): void {
  const root = document.documentElement;
  const record = (kind: "keyboard" | "pointer") => () => {
    root.dataset.input = kind;
  };
  document.addEventListener("keydown", record("keyboard"), true);
  document.addEventListener("pointerdown", record("pointer"), true);

  if (reducedMotion() || !("IntersectionObserver" in window)) {
    return;
  }

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting) {
          entry.target.setAttribute("data-revealed", "");
          observer.unobserve(entry.target);
        }
      }
    },
    { rootMargin: "0px 0px -100px 0px" },
  );

  const observeNew = () => {
    for (const element of document.querySelectorAll(`${REVEAL_SELECTOR}:not([${OBSERVED}])`)) {
      element.setAttribute(OBSERVED, "");
      observer.observe(element);
    }
  };

  observeNew();
  new MutationObserver(observeNew).observe(document.body, { childList: true, subtree: true });
  // Only now that entrances can actually run is the waiting state applied.
  root.classList.add("motion");
}
