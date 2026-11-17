import { useEffect, useId, useRef, useState } from "react";

import { REPO_URL } from "../links";
import {
  DEFAULT_DIRECTORY,
  DEFAULT_POLICY,
  SNIPPET_POLICIES,
  renderSnippet,
  type SnippetPolicy,
} from "../data/snippet";

/**
 * Using it is a few lines of Python.
 *
 * The page's last section, and its first step: a "new database" dialog that
 * is already open, with the code written for whatever the visitor chooses.
 * The snippet comes from `snippets.json`, the same template pytest fills in
 * and runs for every fsync policy, so what is copied here is code that has
 * been executed against this commit's engine.
 *
 * The dialog is not modal and never closes. It is a dialog in look and in
 * role, because section 6 asks for the familiar shape of "new database"; it
 * is open from the start because the point is that there is nothing between
 * reading the page and having the code.
 */
const COPIED_FOR_MS = 1500;

export function StartSection() {
  const directoryId = useId();
  const hintId = useId();
  const titleId = useId();
  const [directory, setDirectory] = useState(DEFAULT_DIRECTORY);
  const [policy, setPolicy] = useState<SnippetPolicy>(DEFAULT_POLICY);
  const [copy, setCopy] = useState<"idle" | "copied" | "failed">("idle");
  const timer = useRef<number | null>(null);

  // The only effect: clearing the "copied" timer if the section goes away
  // while it is running, so it cannot set state on an unmounted component.
  useEffect(
    () => () => {
      if (timer.current !== null) {
        window.clearTimeout(timer.current);
      }
    },
    [],
  );

  const empty = directory.trim() === "";
  const snippet = renderSnippet(directory, policy);

  const copySnippet = async () => {
    if (timer.current !== null) {
      window.clearTimeout(timer.current);
    }
    try {
      await navigator.clipboard.writeText(snippet);
      setCopy("copied");
    } catch {
      // A browser can refuse the clipboard (no permission, an insecure
      // origin). The code is on screen either way; say so rather than
      // pretending it was copied.
      setCopy("failed");
    }
    timer.current = window.setTimeout(() => {
      setCopy("idle");
      timer.current = null;
    }, COPIED_FOR_MS);
  };

  const choose = (next: SnippetPolicy) => {
    setPolicy(next);
    document.querySelector<HTMLElement>(`.start-dialog__option[data-policy="${next}"]`)?.focus();
  };

  return (
    <section className="start" id="start" aria-labelledby="start-heading">
      <div className="start__copy">
        <h2 className="start__heading" id="start-heading">
          <span className="start__line">start</span>{" "}
          <span className="start__line">writing.</span>
        </h2>
        <p className="start__sentence">
          The code opens a database in the directory you name, with the fsync policy you pick,
          writes one key and reads it back.
        </p>
      </div>

      <div className="start-dialog" role="dialog" aria-labelledby={titleId} data-ui="new-database">
        <h3 className="start-dialog__title" id={titleId}>
          new database
        </h3>

        <div className="start-dialog__field">
          <label className="start-dialog__label" htmlFor={directoryId}>
            directory
          </label>
          <input
            id={directoryId}
            className="start-dialog__input"
            type="text"
            value={directory}
            spellCheck={false}
            autoComplete="off"
            onChange={(event) => setDirectory(event.target.value)}
            aria-invalid={empty}
            aria-describedby={empty ? hintId : undefined}
            data-testid="start-directory"
          />
          {empty ? (
            <span className="start-dialog__hint" id={hintId}>
              name a directory for the database
            </span>
          ) : null}
        </div>

        <div className="start-dialog__field">
          <span className="start-dialog__label" id={`${titleId}-fsync`}>
            fsync policy
          </span>
          <div
            className="start-dialog__segments"
            role="radiogroup"
            aria-labelledby={`${titleId}-fsync`}
            data-testid="start-policy"
          >
            {SNIPPET_POLICIES.map((option, index) => (
              <button
                key={option}
                type="button"
                role="radio"
                className="start-dialog__option"
                aria-checked={policy === option}
                tabIndex={policy === option ? 0 : -1}
                data-policy={option}
                onClick={() => setPolicy(option)}
                onKeyDown={(event) => {
                  const step =
                    event.key === "ArrowRight" || event.key === "ArrowDown"
                      ? 1
                      : event.key === "ArrowLeft" || event.key === "ArrowUp"
                        ? -1
                        : 0;
                  if (step === 0) {
                    return;
                  }
                  event.preventDefault();
                  const count = SNIPPET_POLICIES.length;
                  const next = SNIPPET_POLICIES[(index + step + count) % count];
                  if (next !== undefined) {
                    choose(next);
                  }
                }}
              >
                {option}
              </button>
            ))}
          </div>
        </div>

        <pre className="start-dialog__code" data-testid="start-snippet" tabIndex={0} aria-label="the generated code">
          <code>{snippet}</code>
        </pre>

        <div className="start-dialog__actions">
          <button
            type="button"
            className="start-dialog__copy"
            disabled={empty}
            onClick={() => void copySnippet()}
            data-testid="start-copy"
          >
            {copy === "copied" ? "copied" : "copy the code"}
          </button>
          <span className="start-dialog__status" role="status" data-testid="start-copy-status">
            {copy === "copied"
              ? "copied to the clipboard"
              : copy === "failed"
                ? "the browser refused the clipboard, so select the code above"
                : ""}
          </span>
        </div>

        <a className="start-dialog__link" href={REPO_URL}>
          read the source on github
        </a>
      </div>
    </section>
  );
}
