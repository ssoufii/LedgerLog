# LedgerLog web demo: art direction and design spec

This is the authoritative design spec for everything under `web/` (Milestone 11). It was produced
with the taste skill (direction drawn: **One motif**, seed 900810855, sector dev-tool) and Emil
Kowalski's design engineering principles. The agents that build the frontend cannot load those
skills, so everything they need is written here. When this file and a blueprint idea disagree, this
file wins. When this file is silent, prefer less: fewer sizes, fewer colours, fewer moves.

## 1. The idea

> On this page, **the memtable's skip-list towers** (each key's random-height stack of levels, drawn
> from the live engine's real coin flips) become **the one motif**: the stepped notch cut into every
> stage, the bullet, the loader, and the block-built wordmark in the footer.

The motif is a **tower**: a column of equal square blocks with bottoms on one line, heights 1 to 4,
4px gaps between blocks. A row of towers of differing heights is a skip list seen from the side. It is
the only shape on the page with character. Never add a circle, blob, hexagon, or a second motif.

Where it appears, at four scales:

1. **Hero stage notch** (huge): the top-left corner of the hero stage is cut as a three-step staircase
   (`clip-path: polygon(...)`), each step one block wide (`--block-lg`, 72px desktop, 40px phone).
2. **Live towers** (true size): the hero console draws the memtable as towers, one per key, height =
   the node's real level from the engine. This is data, not decoration.
3. **Bullets and loader** (text size): list bullets are a 3-block mini tower (heights 1,2,3) at
   0.5em; the boot loader is the same three blocks rising in sequence.
4. **Footer wordmark** (huge): "ledgerlog" built from solid blocks of differing heights, each letter
   knocked out of its own rectangle, 4px gaps, bottoms aligned.

**The one live thing:** the visitor drives the real Python engine (Pyodide, in a Web Worker) from the
hero: put, get, delete, flush, compact, and pull the plug. Every number and tower on screen comes from
the engine's actual state.

## 2. The argument (sections in order)

Each section exists because it proves one claim. Do not add sections without a claim.

| Section | Claim | Proof on the page |
| --- | --- | --- |
| Hero | LedgerLog is a real storage engine you can drive right here. | The live console on the stage, running the real Python code. |
| How it works | A write goes log, then memory, then disk, then compaction. | The visitor's own record shown at each of the four stages, read from the engine. |
| Durability (showcase) | Durability is a dial, and its cost is measured. | The fsync policy switch, blown up, with the measured throughput of each setting. |
| Crash proof | It survives a crash mid-write, mid-flush, mid-compaction. | Tiles that run each crash in the browser engine and show what recovered. |
| Numbers (stats) | You can work out what it costs for your workload. | A slider for your record count and the time it takes at the measured rate. |
| Start (cta) | Using it is a few lines of Python. | An open "new database" dialog that writes the snippet for you. |
| Footer | It is LedgerLog. | The block-built wordmark, links, credit. |

## 3. Palette (maroon, black, dark grey)

Dark page. `color-scheme: dark` on `:root`. Never `#fff` on `#000`.

```css
:root {
  --ground:        #0D0B0C; /* page ground, near-black with a warm tint */
  --surface-1:     #181516; /* one step up: tiles, panels on the ground */
  --surface-2:     #232021; /* two steps up: inputs, hovered rows */
  --surface-3:     #2E2A2B; /* dark grey field */
  --maroon-deep:   #3B0F17; /* deep maroon field */
  --maroon:        #6E1A26; /* the accent field: hero stage, cta field, heading tile */
  --maroon-bright: #8C2433; /* the one button fill and the active segment */
  --maroon-text:   #D65A6C; /* accent used as text on the ground (5.2:1), sparingly */
  --ink:           #EFE8E6; /* all running text (16.2:1 on ground, 7.2:1 on maroon-bright) */
  --muted:         #B3A9A7; /* secondary notes only; never on --maroon-bright (3.8:1 fails) */
}
```

Rules:
- One ground, one ink, one accent family. Maroon is the accent: the hero stage, the primary button,
  the active segment, the heading tile, the cta field. Nothing else is maroon.
- Surfaces are separated by a step in tone, never by a line or a shadow.
- Section grounds change between sections (ground, dark grey, maroon-deep, maroon). Nothing fades
  into anything: no gradients anywhere, including text and buttons.
- Running text is `--ink` at 16px or more. `--muted` must keep 4.5:1 against whatever it sits on.

## 4. Type

One family: **Pilcrow Rounded** (Fontshare), weights 400, 500, 700. Load it from Fontshare's CSS API
(`https://api.fontshare.com/v2/css?f[]=pilcrow-rounded@400,500,700&display=swap`) with preconnects,
unless the font's licence file explicitly allows self-hosting, in which case self-host under
`web/public/fonts/`. Fall back to `ui-rounded, system-ui, sans-serif`, and make sure the page still
passes the house rules when the font fails to load (Playwright runs may have no network).

- Display: 700, tracking -0.05em, leading 0.9, lowercase, one or two words per line.
- Text: 400 body, 500 emphasis, leading 1.5, tracking 0. Lowercase is allowed in headings only.
- Code (`<pre>`, `<code>` only): `ui-monospace, "SF Mono", Menlo, monospace`. The only monospace.
- Tabular figures (`font-variant-numeric: tabular-nums`) on every number that updates.

Five sizes, no more:

```css
:root {
  --step-small: clamp(0.8125rem, 0.79rem + 0.1vw, 0.875rem);   /* 13-14px: data labels in fragments */
  --step-body:  clamp(1.0625rem, 1rem + 0.25vw, 1.1875rem);    /* 17-19px: running text */
  --step-lead:  clamp(1.25rem, 1.1rem + 0.7vw, 1.625rem);      /* 20-26px: supporting sentences */
  --step-head:  clamp(2rem, 1.3rem + 3.2vw, 4rem);             /* 32-64px: section headings */
  --step-hero:  clamp(3.25rem, 7vw, 8rem);                      /* 52-128px: h1, cta line, figures */
}
```

The stats figure may use `--step-hero` scaled to `clamp(140px, 19vw, 320px)`; that is the same step
role, not a sixth size.

## 5. Frame and grid

- 12-column grid, outer margins `clamp(16px, 3vw, 40px)`, equal on both sides at every width.
- Section gap 120 to 240px on desktop, 80 to 120px on a phone.
- Every section opens with a heading that names it. One `h1` on the page, in the hero.
- Navigation: plain row 28px from the top. Wordmark left, four text links right (how it works,
  durability, crash proof, github), 15px. No bar chrome, no blur, no pill.
- Buttons: square corners (radius 0), flat `--maroon-bright` fill, `--ink` label. One button shape on
  the whole page. Secondary actions are text buttons.
- Corners everywhere: 0, except the stepped notch.

## 6. Sections (blueprints)

Geometry per section. Interface fragments are real, working React components at true size, with
`data-ui` on their root. Inside a `data-ui` fragment, product conventions apply (inputs and tables may
have borders; an open dialog may cast a shadow).

### Hero: fragment on a shaped stage
- Headline, h1, two lines at `--step-hero`, columns 1-6, starting about 150px down:
  "a storage engine / you can drive." Under it (32-48px below), one sentence at `--step-lead`, max
  40ch: "LedgerLog is a log-structured key-value store written from scratch in Python. This is the
  real engine, running in your browser." Then one button: "Write a key" (focuses the console input).
- The stage: a flat `--maroon` field spanning columns 5-12, starting 42% down, bleeding off the right
  and bottom edges, top-left corner cut by the three-step notch.
- On the stage, 56px in: the **console** fragment, about 620px wide, cropped by the bottom edge. Key and
  value inputs, put / get / delete buttons, and below them the live memtable towers and a WAL tail
  (last 6 records: offset, kind, key, byte length, checksum). A small status line shows the boot
  state ("starting python", "ready") with the tower loader.
- Phone: headline at 10vw on two lines, sentence 16px, button, then the stage full width bleeding off
  both sides, notch steps 40px, the console 16px inside at true size, cropped on the right if wider.

### How it works: one card, four fields
- Four full-height colour fields side by side (25vw each, 84svh), no gaps, lightness steps:
  `--ground`, `--surface-3`, `--maroon-deep`, `--maroon`.
- The same record card (260px wide, true size, `data-ui`) appears once per field, 38% down: the
  visitor's most recent key (or `user:1042:login` before they write one), shown as
  1. a WAL record: offset, length, crc32, kind;
  2. a memtable node: its tower at its real height and its level;
  3. an SSTable entry: table sequence, whether the sparse index points at it, bloom bits set;
  4. after compaction: the merged table, and older versions or tombstones that were dropped.
- One clause per field at the bottom, 28px in, two lines at `--step-lead`: "First it lands in the
  log." "Then it is sorted in memory." "Then it is flushed to disk." "Then compaction merges it."
- Heading top-left in the first field only, `--step-head`, 700: "how a write travels".
- Phone: fields stack, each full width, 64svh.

### Durability: one control, blown up
- A full-bleed field, 92svh, `--maroon-deep` or `--surface-3` (the control swaps between them).
- The fsync policy segmented control (`never`, `interval`, `always`), built in em, enlarged until it is
  about 46vh tall, anchored right of centre and cropped by the right and bottom edges by about a fifth.
- Operating it swaps the field colour and one word of the heading: "fast **and** durable?" becomes
  "fast, **not** durable", "fast **and** durable", "durable, **not** fast" (one face, one colour; the
  word changes, it is not styled).
- Under the heading, one sentence and the measured figure for the chosen policy, taken verbatim from
  the README's Performance section, with its bench invocation shown in a `<code>` block.
- Phone: the control at about 130vw, anchored bottom-left, cropped on the right.

### Crash proof: large marks on tiles
- A board of filled tiles, four across, 8px gaps, each about 3:2, `--surface-1`. The first tile is
  twice as wide, `--maroon`, holding the heading "pull the plug".
- Each other tile names one failure the test suite covers, set in type at 28-32px 700, top-left, 24px
  padding: "torn write", "flipped byte", "crash mid-flush", "crash mid-compaction", "missing footer",
  "expired record". Clicking a tile runs that scenario on a copy of the visitor's in-browser database
  (in the worker) and replaces the tile body with the result: keys before, keys after recovery, and
  what recovery dropped. No hover lift. Every claim on a tile must correspond to a real test in
  `tests/`; name the test file in the tile's details.
- Phone: two per row, heading tile full width first, at most eight tiles.

### Numbers: figure set by a slider
- A field of at least 88svh, `--ground`. Top-left question as heading at `--step-head`: "how many
  records?" Under it, a slider (400px, `data-ui`) preset to 1,000,000, range 10,000 to 100,000,000,
  logarithmic.
- Bottom-left the result at `clamp(140px, 19vw, 320px)`, 700, leading 0.8, tabular: seconds to write
  that many records at the measured `fsync=never` rate from the README (64,466 records/s), unit "s"
  at 0.22em on the same baseline. Bottom-right, 30ch at `--step-body`: the formula in words and the
  invocation that produced the rate.
- Every figure on the page comes from `web/src/data/bench.json`, generated from the README's
  Performance section by a script, so the numbers cannot drift. No other figures in this section.

### Start: first step, already open
- Full-bleed `--maroon` field, 90svh. Right, columns 7-11, vertically centred: a "new database"
  dialog (440px, `data-ui`, may cast its own shadow): a directory field ("./data", editable), a
  segmented fsync choice, and the page's last primary button "copy the code", which copies a
  generated snippet (`LedgerLog("./data", fsync_policy="always")` plus put/get) and shows "copied"
  for 1.5s. A text link under it goes to the GitHub repo.
- Left, columns 1-5, bottom-anchored: the closing line, two lines at `--step-hero`: "start / writing."
  and one sentence at 17px saying what the snippet does.

### Footer: the mark, built large
- About 34svh, the wordmark "ledgerlog" built from blocks: each letter knocked out of its own solid
  rectangle in `--surface-3` or `--maroon`, heights 60-100% of the band (the heights are a real skip
  list's levels), 4px gaps, bottoms aligned. Below, 56px lower: links (GitHub, architecture doc,
  roadmap, licence) at 15px, and the copyright left.

## 7. House rules (checked by Playwright, see `web/tests/house-rules.spec.ts`)

| Rule | Check |
| --- | --- |
| Headlines two lines or fewer at 1440, 768, 390 | every h1-h6: `height / lineHeight <= 2` |
| One h1, a heading in every `section` | DOM count |
| No horizontal overflow at any width | `scrollWidth <= innerWidth` |
| No gradients | no computed `background-image` containing `gradient` |
| No shadows at rest | no computed `box-shadow` / `text-shadow` except inside an open dialog in `[data-ui]` |
| No monospace outside `pre` and `code` | computed `font-family` |
| No thin rules or outlined boxes outside `[data-ui]` | no computed border width of 1px or less that is visible |
| Body text contrast at least 4.5:1, size at least 16px | computed colour and size on `p` |
| No eyebrows, no pill badges, no tiny uppercase tracked labels | no `text-transform: uppercase` with letter-spacing > 0.05em |
| No emoji, no index numbering (01, 02), no glass (`backdrop-filter`) | text scan and computed style |
| Every control reachable by keyboard and named | tab order plus accessible name check |

## 8. Motion (Emil Kowalski principles, made checkable)

Tokens:

```css
:root {
  --ease-out:    cubic-bezier(0.23, 1, 0.32, 1);    /* entrances, UI responses */
  --ease-in-out: cubic-bezier(0.77, 0, 0.175, 1);   /* things moving while visible, the shutter */
  --ease-soft:   cubic-bezier(0.33, 1, 0.68, 1);    /* slow settle inside the shutter */
  --t-press: 140ms;
  --t-hover: 180ms;
  --t-ui:    220ms;
  --t-reveal: 700ms;
  --t-open:  1100ms;
  --stagger: 70ms;
}
```

Rules:
1. **Every animation has a purpose**: feedback, state change, explanation, or preventing a jarring
   change. If it is "looks cool" and seen often, it does not animate.
2. **Only `transform`, `opacity` and `clip-path` animate.** Never width, height, margin, top, left.
   No `transition: all`; name the properties.
3. **Never `ease-in`** for UI. Entering or responding: `--ease-out`. Moving on screen: `--ease-in-out`.
   Colour or hover: `ease`. Constant motion (loader): `linear`.
4. **UI responses stay under 300ms.** Press 140ms, hover 180ms, panels and segments 220ms. Only the
   signature shutter and section entrances may be longer.
5. **Press feedback on every pressable thing:** `transform: scale(0.97)` on `:active`.
6. **Never animate from `scale(0)`.** Enter from `scale(0.95)` with `opacity: 0`, or translate 16-24px.
7. **Interruptible:** state-driven UI uses CSS transitions (or WAAPI), not keyframes, so rapid
   toggling retargets instead of restarting.
8. **Keyboard-initiated actions do not animate** (pressing Enter in the console to put a key updates
   instantly; the new tower may fade in at 140ms, nothing slides).
9. **Hover styles only under `@media (hover: hover) and (pointer: fine)`.** Hover raises contrast.
10. **Exit faster than enter.**
11. **Reduced motion:** under `prefers-reduced-motion: reduce`, drop all travel and scale, keep short
    opacity fades. Content must never stay hidden if a script fails or motion is reduced.

The moves:
- **Signature: shutter.** On load, the hero stage opens from the bottom edge
  (`clip-path: inset(100% 0 0 0)` to `inset(0)`, 1100ms `--ease-in-out`) while the console inside
  settles from `scale(1.04)` to rest (1500ms `--ease-soft`). Once only.
- **Section entrances:** heading, then text, then fragment, staggered 70ms, rising 20px with opacity,
  `--t-reveal`, `--ease-out`, once, via IntersectionObserver (`rootMargin: -100px`).
- **Tower insert:** a new key's tower builds block by block (each block 140ms, staggered 40ms).
  A tombstone turns the tower to `--surface-3`. A flush moves the towers out as one group
  (`--ease-in-out`, 220ms) and a new SSTable tile appears.
- **Segmented controls:** the active segment indicator slides (`transform`, 220ms `--ease-in-out`).
  The durability field colour crossfades (`ease`, 220ms).
- **Figures:** the stats figure updates instantly on input (no count-up on input; it is
  keyboard and pointer driven). A one-time count-up when the section first enters is allowed.

## 9. Copy rules

- Lowercase headings are the style; running text is sentence case.
- No em dashes anywhere (code, copy, commits). Commas or parentheses instead.
- No invented proof: no users, quotes, logos, stars, or "trusted by". Every number comes from
  `bench.json` or from the live engine.
- Headlines about 32 characters or fewer.

## 10. Verification workflow (Playwright)

Every frontend story ends with:
1. `npm run lint`, `npm run typecheck`, `npm run build` in `web/` all pass.
2. `npx playwright test` passes against the production build (`vite preview`), not the dev server.
3. Screenshots at 1440, 768 and 390 wide are saved by the house-rules spec to
   `web/test-results/screenshots/` and uploaded as a CI artifact. The agent opens them and checks:
   headline lines, margins, empty stretches, cropped fragments, that nothing failed to appear.
4. The reduced-motion project (`reducedMotion: 'reduce'`) passes the same specs.
