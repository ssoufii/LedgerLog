import { useEffect, useRef, useState } from "react";

import { SNIPPET, renderSnippet } from "../data/snippet";
import { REPO_URL } from "../links";

/**
 * Using it is a few lines of Python.
 *
 * The page ends with the code for the choices the visitor made, already
 * written. The snippet is rendered from `snippet.json`, the same template
 * `tests/test_snippet.py` renders and then runs for every fsync choice, so
 * what is copied from here is code that has been executed, not code that
 * looks right.
 *
 * The dialog is open and on the page rather than behind a trigger, which is
 * section 6's "first step, already open": there is no step before the step.
 * It is a real `<dialog open>` so that the shadow section 7 allows inside an
 * open dialog is allowed here, and so the thing announced is the thing it is.
 */
const COPIED_MS = 1500;

export function StartDialog() {
  const [directory, setDirectory] = useState(SNIPPET.default_directory);
  const [policy, setPolicy] = useState(SNIPPET.policies[2] ?? "always");
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // The confirmation is a timer, and a component that is taken off the page
  // while one is pending would leave it to fire into nothing.
  useEffect(
    () => () => {
      if (timer.current !== null) {
        clearTimeout(timer.current);
      }
    },
    [],
  );

  const snippet = renderSnippet(directory, policy);

  const onCopy = () => {
    void (async () => {
      try {
        await navigator.clipboard.writeText(snippet);
      } catch {
        // A browser that refuses the clipboard (no permission, no secure
        // context) must not leave the button claiming it copied.
        setCopied(false);
        return;
      }
      setCopied(true);
      if (timer.current !== null) {
        clearTimeout(timer.current);
      }
      timer.current = setTimeout(() => {
        setCopied(false);
      }, COPIED_MS);
    })();
  };

  return (
    <section className="start" id="start" aria-labelledby="start-heading">
      <div className="start__closing">
        <h2 className="start__line" id="start-heading">
          <span className="start__word">start</span>
          <span className="start__word">writing.</span>
        </h2>
        <p className="start__sentence">
          The snippet opens a database in the directory you name, writes one record and reads it
          back. It is the whole of the API you need to begin.
        </p>
      </div>

      <dialog className="start__dialog" data-ui="new-database" open aria-label="new database">
        <span className="start__dialog-title">new database</span>

        <div className="start__field">
          <label className="console__label" htmlFor="start-directory">
            directory
          </label>
          <input
            className="console__input"
            id="start-directory"
            name="directory"
            value={directory}
            onChange={(event) => setDirectory(event.target.value)}
          />
        </div>

        <fieldset className="start__fieldset">
          <legend className="console__label">fsync policy</legend>
          <div className="start__choice">
            {SNIPPET.policies.map((option) => (
              <label className="start__option" key={option} data-chosen={option === policy}>
                <input
                  type="radio"
                  name="start-policy"
                  value={option}
                  checked={option === policy}
                  onChange={() => setPolicy(option)}
                />
                {option}
              </label>
            ))}
          </div>
        </fieldset>

        <pre className="start__snippet" data-testid="start-snippet">
          <code>{snippet}</code>
        </pre>

        <button type="button" onClick={onCopy} data-testid="copy-the-code">
          {copied ? "copied" : "copy the code"}
        </button>
        <span className="start__status" role="status">
          {copied ? "the snippet is on your clipboard" : ""}
        </span>

        <a className="nav-link start__repo" href={REPO_URL}>
          read the source on github
        </a>
      </dialog>
    </section>
  );
}
