/**
 * The bench command that produced a figure, shown exactly as it would be typed.
 *
 * Not wrapped. A long shell command that soft wraps breaks after a hyphen,
 * which turns `--value-size` into `-` and `-value-size` on the next line and
 * makes the command look like something else. The block scrolls sideways
 * instead, which is the usual way a code block keeps its lines intact.
 *
 * A scrolling region is operated with the keyboard, so it is a labelled,
 * focusable region rather than a silent one: a keyboard user can tab to it and
 * scroll it, and a screen reader says what it is.
 */
export function Invocation({ command, label }: { command: string; label: string }) {
  return (
    <pre className="invocation" tabIndex={0} role="region" aria-label={label}>
      <code>{command}</code>
    </pre>
  );
}
