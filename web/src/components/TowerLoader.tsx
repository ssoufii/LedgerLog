/**
 * The motif at text size: three blocks rising in sequence.
 *
 * Why this and not a spinner: section 1 of docs/FRONTEND_DESIGN.md gives the
 * page one shape, the skip list tower, and asks for it at four scales. This is
 * the smallest of them, and a spinner would be the second motif the spec rules
 * out. The movement lives in CSS (see `.loader` in page.css) so that a reduced
 * motion preference can swap the travel for a fade without touching React.
 *
 * Decorative: the boot phase beside it is the text a screen reader announces,
 * so the blocks are hidden from the accessibility tree.
 */
export function TowerLoader() {
  return (
    <span className="loader" aria-hidden="true">
      <span className="loader__block" />
      <span className="loader__block" />
      <span className="loader__block" />
    </span>
  );
}
