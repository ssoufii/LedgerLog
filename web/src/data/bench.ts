/**
 * The measured figures, typed.
 *
 * `bench.json` is generated from the README's Performance section by
 * `scripts/bench-data.mjs` and committed, because TypeScript imports it and
 * `npm run typecheck` runs before `npm run build`. The build regenerates it, so
 * a build cannot ship figures that disagree with the README, and
 * `tests/bench-data.spec.ts` fails if the committed copy has drifted.
 *
 * Nothing on the page may state a performance number that did not come through
 * here (docs/FRONTEND_DESIGN.md section 9).
 */

import data from "./bench.json";

export type FsyncPolicy = "never" | "interval" | "always";

export interface PolicyFigures {
  policy: FsyncPolicy;
  records_per_second: number;
  mib_per_second: number;
  elapsed_seconds: number;
  reads_per_second: number;
  read_p50_ms: number;
  read_p99_ms: number;
}

export interface BenchData {
  source: string;
  machine: string;
  machine_note: string;
  invocation: string;
  policies: PolicyFigures[];
}

export const BENCH = data as BenchData;

/** The three policies, in the order the README's tables list them. */
export const POLICIES: FsyncPolicy[] = ["never", "interval", "always"];

export function figuresFor(policy: FsyncPolicy): PolicyFigures {
  const found = BENCH.policies.find((entry) => entry.policy === policy);
  if (found === undefined) {
    // The generator fails the build before this can happen; this is here so
    // that a hand-edited bench.json cannot put `undefined` on the page.
    throw new Error(`bench.json has no figures for the ${policy} policy`);
  }
  return found;
}
