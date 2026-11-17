/**
 * The house rules of docs/FRONTEND_DESIGN.md section 7, as one audit that runs
 * inside the page.
 *
 * It lives in `tests/support/` rather than beside the spec because Playwright
 * only collects `*.spec.ts` from the test directory, and because the same audit
 * is run at three widths, with the font provider blocked, and under a reduced
 * motion preference. It is written as a single self-contained function with no
 * references to module scope, because `page.evaluate` serialises it and ships
 * the source to the browser.
 *
 * Every rule returns a violation with the element it was found on, so a failure
 * names the thing to fix rather than only the rule that broke.
 */

export interface Violation {
  rule: string;
  detail: string;
}

export interface AuditReport {
  violations: Violation[];
  h1Count: number;
  sectionCount: number;
  mainCount: number;
  scrollWidth: number;
  clientWidth: number;
  innerWidth: number;
  focusable: string[];
  headings: { selector: string; text: string; lines: number }[];
  paragraphs: { selector: string; fontSize: number; contrast: number }[];
}

export function auditPage(): AuditReport {
  const violations: Violation[] = [];
  const add = (rule: string, detail: string): void => {
    violations.push({ rule, detail });
  };

  const describe = (element: Element): string => {
    const tag = element.tagName.toLowerCase();
    const id = element.id === "" ? "" : `#${element.id}`;
    const className = element.getAttribute("class");
    const classes = className === null || className.trim() === "" ? "" : `.${className.trim().split(/\s+/).join(".")}`;
    return `${tag}${id}${classes}`;
  };

  const isVisible = (element: HTMLElement): boolean => {
    const style = getComputedStyle(element);
    if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") {
      return false;
    }
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };

  const everything = Array.from(document.body.querySelectorAll<HTMLElement>("*"));
  const shown = everything.filter(isVisible);

  // Colour parsing and WCAG contrast, for the body text rule.
  const parseColour = (value: string): [number, number, number, number] | null => {
    const match = /^rgba?\(([^)]+)\)$/.exec(value.trim());
    if (match === null) {
      return null;
    }
    const parts = (match[1] ?? "").split(/[\s,/]+/).filter((part) => part !== "");
    const red = Number(parts[0]);
    const green = Number(parts[1]);
    const blue = Number(parts[2]);
    const alpha = parts[3] === undefined ? 1 : Number(parts[3]);
    if ([red, green, blue, alpha].some((channel) => Number.isNaN(channel))) {
      return null;
    }
    return [red, green, blue, alpha];
  };

  const relativeLuminance = (rgb: [number, number, number]): number => {
    const channels = rgb.map((channel) => {
      const scaled = channel / 255;
      return scaled <= 0.03928 ? scaled / 12.92 : Math.pow((scaled + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * (channels[0] ?? 0) + 0.7152 * (channels[1] ?? 0) + 0.0722 * (channels[2] ?? 0);
  };

  /** The first painted background behind an element, walking up the tree. */
  const backgroundBehind = (element: HTMLElement): [number, number, number] => {
    let node: HTMLElement | null = element;
    while (node !== null) {
      const parsed = parseColour(getComputedStyle(node).backgroundColor);
      if (parsed !== null && parsed[3] > 0) {
        return [parsed[0], parsed[1], parsed[2]];
      }
      node = node.parentElement;
    }
    return [0, 0, 0];
  };

  const contrastRatio = (foreground: [number, number, number], background: [number, number, number]): number => {
    const first = relativeLuminance(foreground);
    const second = relativeLuminance(background);
    const lighter = Math.max(first, second);
    const darker = Math.min(first, second);
    return (lighter + 0.05) / (darker + 0.05);
  };

  // Rule: headlines two lines or fewer.
  const headings: { selector: string; text: string; lines: number }[] = [];
  for (const heading of Array.from(document.querySelectorAll<HTMLElement>("h1,h2,h3,h4,h5,h6"))) {
    const style = getComputedStyle(heading);
    const lineHeight = Number.parseFloat(style.lineHeight);
    const height = heading.getBoundingClientRect().height;
    const lines = Number.isFinite(lineHeight) && lineHeight > 0 ? height / lineHeight : 1;
    headings.push({ selector: describe(heading), text: heading.innerText, lines: Math.round(lines * 100) / 100 });
    // A hair over two, because a two line block whose line boxes round up by a
    // fraction of a pixel is still two lines.
    if (lines > 2.02) {
      add("headlines two lines or fewer", `${describe(heading)} runs ${String(Math.round(lines * 100) / 100)} lines`);
    }
  }

  // Rule: one h1, a heading in every section.
  const h1Count = document.querySelectorAll("h1").length;
  const sections = Array.from(document.querySelectorAll("section"));
  for (const section of sections) {
    if (section.querySelector("h1,h2,h3,h4,h5,h6") === null) {
      add("a heading in every section", `${describe(section)} has none`);
    }
  }

  for (const element of shown) {
    const selector = describe(element);
    const style = getComputedStyle(element);
    const inFragment = element.closest("[data-ui]") !== null;
    const inOpenDialog = element.closest("dialog[open],[role='dialog']") !== null;

    // Rule: no gradients, including on the generated boxes.
    for (const pseudo of [null, "::before", "::after"]) {
      const image = getComputedStyle(element, pseudo).backgroundImage;
      if (image.includes("gradient")) {
        add("no gradients", `${selector}${pseudo ?? ""} paints ${image}`);
      }
    }

    // Rule: no shadows at rest, except inside an open dialog in a fragment.
    if (!(inFragment && inOpenDialog)) {
      if (style.boxShadow !== "none") {
        add("no shadows at rest", `${selector} has box-shadow ${style.boxShadow}`);
      }
      if (style.textShadow !== "none") {
        add("no shadows at rest", `${selector} has text-shadow ${style.textShadow}`);
      }
    }

    // Rule: no monospace outside pre and code.
    if (style.fontFamily.toLowerCase().includes("mono") && element.closest("pre,code") === null) {
      add("no monospace outside pre and code", `${selector} is set in ${style.fontFamily}`);
    }

    // Rule: no thin rules or outlined boxes outside a fragment.
    if (!inFragment) {
      for (const side of ["Top", "Right", "Bottom", "Left"] as const) {
        const width = Number.parseFloat(style.getPropertyValue(`border-${side.toLowerCase()}-width`));
        const lineStyle = style.getPropertyValue(`border-${side.toLowerCase()}-style`);
        const colour = parseColour(style.getPropertyValue(`border-${side.toLowerCase()}-color`));
        const painted = lineStyle !== "none" && lineStyle !== "hidden" && (colour === null || colour[3] > 0);
        if (painted && width > 0 && width <= 1) {
          add("no thin rules outside a fragment", `${selector} has a ${String(width)}px ${side.toLowerCase()} border`);
        }
      }
      // Section 5: corners everywhere 0, except the stepped notch. A rounded
      // corner outside a fragment is the pill badge section 7 rules out.
      for (const corner of ["top-left", "top-right", "bottom-left", "bottom-right"]) {
        const radius = Number.parseFloat(style.getPropertyValue(`border-${corner}-radius`));
        if (Number.isFinite(radius) && radius > 0) {
          add("no rounded corners outside a fragment", `${selector} has a ${String(radius)}px ${corner} radius`);
        }
      }
    }

    // Rule: no eyebrows, no tiny uppercase tracked labels.
    if (style.textTransform === "uppercase") {
      const fontSize = Number.parseFloat(style.fontSize);
      const tracking = Number.parseFloat(style.letterSpacing);
      if (Number.isFinite(tracking) && fontSize > 0 && tracking / fontSize > 0.05) {
        add("no eyebrows or tracked uppercase labels", `${selector} is uppercase with ${style.letterSpacing} tracking`);
      }
    }

    // Rule: no glass.
    const backdrop = style.backdropFilter === "" ? style.getPropertyValue("-webkit-backdrop-filter") : style.backdropFilter;
    if (backdrop !== "" && backdrop !== "none") {
      add("no glass", `${selector} has backdrop-filter ${backdrop}`);
    }
  }

  // Rule: body text contrast at least 4.5:1, size at least 16px.
  const paragraphs: { selector: string; fontSize: number; contrast: number }[] = [];
  for (const paragraph of Array.from(document.querySelectorAll<HTMLElement>("p"))) {
    if (!isVisible(paragraph)) {
      continue;
    }
    const style = getComputedStyle(paragraph);
    const fontSize = Number.parseFloat(style.fontSize);
    const foreground = parseColour(style.color);
    const ratio =
      foreground === null
        ? 21
        : contrastRatio([foreground[0], foreground[1], foreground[2]], backgroundBehind(paragraph));
    paragraphs.push({
      selector: describe(paragraph),
      fontSize,
      contrast: Math.round(ratio * 100) / 100,
    });
    if (fontSize < 16) {
      add("body text at least 16px", `${describe(paragraph)} is ${String(fontSize)}px`);
    }
    if (ratio < 4.5) {
      add("body text contrast at least 4.5:1", `${describe(paragraph)} is ${String(Math.round(ratio * 100) / 100)}:1`);
    }
  }

  // Rule: no emoji, no index numbering. Read off the leaves, so that a number
  // is judged where it is written rather than inside a sentence that contains
  // it.
  const leaves = shown.filter((element) => element.children.length === 0);
  for (const leaf of leaves) {
    const text = (leaf.textContent ?? "").trim();
    if (text === "") {
      continue;
    }
    if (/\p{Extended_Pictographic}/u.test(text)) {
      add("no emoji", `${describe(leaf)} contains ${text}`);
    }
    if (/^0\d$/.test(text)) {
      add("no index numbering", `${describe(leaf)} is numbered ${text}`);
    }
  }

  // Rule: every control reachable by keyboard and named. The selectors go back
  // to the spec, which checks the names against Playwright's own accessible
  // name computation and walks the tab order.
  const focusableSelector =
    "a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex='-1'])";
  const focusable = Array.from(document.querySelectorAll<HTMLElement>(focusableSelector))
    .filter(isVisible)
    .map(describe);

  return {
    violations,
    h1Count,
    sectionCount: sections.length,
    mainCount: document.querySelectorAll("main").length,
    scrollWidth: document.documentElement.scrollWidth,
    clientWidth: document.documentElement.clientWidth,
    innerWidth: window.innerWidth,
    focusable,
    headings,
    paragraphs,
  };
}

/** Stamp every focusable element so a tab walk can say which one it reached. */
export function stampFocusable(): number {
  const focusableSelector =
    "a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex='-1'])";
  const elements = Array.from(document.querySelectorAll<HTMLElement>(focusableSelector));
  elements.forEach((element, index) => {
    element.dataset.kbStamp = String(index);
  });
  return elements.length;
}
