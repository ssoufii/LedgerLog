import { useState } from "react";

import { BENCH, POLICIES, figuresFor, type FsyncPolicy } from "../data/bench";
import { Invocation } from "./Invocation";

/**
 * Durability is a dial, and its cost is measured.
 *
 * One control, blown up until it is the section (section 6). Operating it
 * swaps the field's colour and one word of the heading, and shows the figure
 * the README recorded for that policy, with the invocation that produced it,
 * so a visitor can run the same command and get the same number.
 *
 * Every figure comes from `bench.json`, which is generated from the README.
 * Nothing here computes a rate.
 *
 * The control is a radio group rather than a row of buttons, because that is
 * what a segmented control is: one choice among three. That also gives the
 * arrow key behaviour for free from the platform rather than from a key
 * handler that would have to reimplement it.
 */
const HEADINGS: Record<
  FsyncPolicy,
  { before: string; word: string; after: string }
> = {
  never: { before: "fast,", word: "not", after: "durable" },
  interval: { before: "fast", word: "and", after: "durable" },
  always: { before: "durable,", word: "not", after: "fast" },
};

const RESTING = { before: "fast", word: "and", after: "durable?" };

const SENTENCES: Record<FsyncPolicy, string> = {
  never:
    "The log is written and the write is acknowledged. The operating system decides when it reaches the disk.",
  interval:
    "The log is synced on a timer. A crash can lose only the writes since the last sync.",
  always:
    "Every write is synced before it is acknowledged. Nothing acknowledged is ever lost.",
};

/**
 * Which field colour a policy takes.
 *
 * Section 6 gives the section two grounds and says the control swaps between
 * them. The split that means something is whether the policy syncs at all: the
 * accent field marks the settings that make a durability promise.
 */
function ground(policy: FsyncPolicy | null): string {
  return policy === "interval" || policy === "always" ? "deep" : "grey";
}

export function DurabilityDial() {
  const [policy, setPolicy] = useState<FsyncPolicy | null>(null);
  const heading = policy === null ? RESTING : HEADINGS[policy];
  const figures = policy === null ? null : figuresFor(policy);

  return (
    <section
      className="dial-section"
      id="durability"
      data-ground={ground(policy)}
    >
      <div className="dial-section__copy">
        <h2 className="dial-section__heading">
          {heading.before}{" "}
          <span data-testid="durability-word">{heading.word}</span>{" "}
          {heading.after}
        </h2>

        <p className="dial-section__sentence" data-testid="durability-sentence">
          {policy === null
            ? "LedgerLog syncs the write-ahead log on whichever of three policies you pick. Choose one to see what it measured."
            : SENTENCES[policy]}
        </p>

        {figures === null ? null : (
          <div
            className="dial-section__figures"
            data-numeric
            data-testid="durability-figures"
          >
            <span className="dial-section__rate">
              {figures.records_per_second.toLocaleString("en-GB")}
            </span>
            <span className="dial-section__unit">records/s</span>
            <span className="dial-section__aside">
              {figures.mib_per_second} MiB/s, {figures.elapsed_seconds} s for
              100,000 records, on {BENCH.machine}
            </span>
          </div>
        )}

        <Invocation
          command={BENCH.invocation}
          label="the bench invocation behind these figures"
        />
      </div>

      <div
        className="dial"
        role="radiogroup"
        aria-label="write-ahead log fsync policy"
        data-testid="durability-dial"
      >
        {POLICIES.map((option) => (
          <button
            key={option}
            type="button"
            role="radio"
            className="dial__option"
            aria-checked={policy === option}
            // Roving tabindex: the group is one tab stop, and the arrow keys
            // move within it, which is how a radio group behaves natively and
            // what section 7's keyboard rule asks of every control.
            tabIndex={
              policy === option || (policy === null && option === POLICIES[0])
                ? 0
                : -1
            }
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
              const current = policy === null ? 0 : POLICIES.indexOf(policy);
              const next =
                POLICIES[(current + step + POLICIES.length) % POLICIES.length];
              if (next !== undefined) {
                setPolicy(next);
                document
                  .querySelector<HTMLElement>(
                    `.dial__option[data-policy="${next}"]`,
                  )
                  ?.focus();
              }
            }}
            data-policy={option}
          >
            {option}
          </button>
        ))}
      </div>
    </section>
  );
}
