// Generates web/src/data/bench.json from the README's Performance section.
//
// Why generate rather than type the numbers into a component: the page claims
// its figures are measured, and docs/FRONTEND_DESIGN.md section 9 forbids
// invented proof. A number typed into TSX is a number that can drift from the
// one the bench actually produced, silently, the first time the bench is rerun
// and only the README is updated. Here there is one source, the README table,
// and the page reads what this script found in it.
//
// Why parse the README rather than run the bench: the bench measures the
// machine it runs on, and the figures on the page are from one named machine
// (an M1 laptop), not from a CI runner or from whoever's browser is open. The
// README is where that run was written down, so the README is the source.
//
// Run by the prebuild and predev npm scripts, so a build cannot ship figures
// that disagree with the README. The output is committed as well, because it is
// imported by TypeScript and `npm run typecheck` runs before `npm run build`.
// `--check` re-derives the file and exits non-zero if the committed copy has
// drifted, which is what the test in web/tests/bench-data.spec.ts runs.

import { readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const webRoot = resolve(here, "..");
const repoRoot = resolve(webRoot, "..");
const readmePath = join(repoRoot, "README.md");
const outputPath = join(webRoot, "src", "data", "bench.json");

/** Fail loudly rather than emitting a file with a hole in it. */
function required(value, what) {
  if (value === undefined || value === null) {
    throw new Error(
      `could not find ${what} in README.md. The Performance section has been ` +
        "restructured, and web/src/data/bench.json cannot be generated from it.",
    );
  }
  return value;
}

/** "64,466 records/s" -> 64466. */
function toNumber(text) {
  return Number(text.replace(/,/g, ""));
}

/**
 * The text under `heading`, up to the next heading at `stopAt` depth or above.
 *
 * The depth is a parameter because "## Performance" has to keep its own
 * subsections, while "### Sequential write throughput" has to stop at the next
 * one. Cutting both at the same depth would leave the Performance section as
 * its own first paragraph and nothing else.
 */
function section(readme, heading, stopAt) {
  const start = readme.indexOf(heading);
  if (start < 0) {
    return null;
  }
  const rest = readme.slice(start + heading.length);
  const end = rest.search(stopAt);
  return end < 0 ? rest : rest.slice(0, end);
}

function parse(readme) {
  const performance = required(section(readme, "\n## Performance", /\n## /), "the Performance section");

  // The Machine line names the hardware in its first sentence and then
  // qualifies it. Both are kept, verbatim, so the page can show the short form
  // beside a figure and the caveat where there is room for it.
  const machineLine = required(
    /\*\*Machine\*\*:\s*(.+)/.exec(performance)?.[1]?.trim(),
    "the Machine line",
  );
  const machineBreak = machineLine.indexOf(". ");
  const machine = machineBreak < 0 ? machineLine : machineLine.slice(0, machineBreak + 1);
  const machineNote = machineBreak < 0 ? "" : machineLine.slice(machineBreak + 2);

  // The shared invocation, in the fenced block before the tables.
  const invocation = required(
    /```\n(python -m ledgerlog\.bench[\s\S]*?)\n```/.exec(performance)?.[1]?.trim(),
    "the bench invocation",
  );

  const writes = required(
    section(performance, "### Sequential write throughput", /\n#{2,3} /),
    "the write throughput table",
  );
  const reads = required(
    section(performance, "### Random read latency", /\n#{2,3} /),
    "the read latency table",
  );

  // | `never` | 64,466 records/s | 7.75 MiB/s | 1.551 s |
  const throughput = new Map();
  const writeRow =
    /^\|\s*`(never|interval|always)`[^|]*\|\s*([\d,]+) records\/s\s*\|\s*([\d.]+) MiB\/s\s*\|\s*([\d.]+) s[^|]*\|/gm;
  for (const row of writes.matchAll(writeRow)) {
    throughput.set(row[1], {
      records_per_second: toNumber(row[2]),
      mib_per_second: Number(row[3]),
      elapsed_seconds: Number(row[4]),
    });
  }

  // | `never` | 9,755 | 0.0992 ms | 0.2185 ms |
  const latency = new Map();
  const readRow =
    /^\|\s*`(never|interval|always)`\s*\|\s*([\d,]+)\s*\|\s*([\d.]+) ms\s*\|\s*([\d.]+) ms\s*\|/gm;
  for (const row of reads.matchAll(readRow)) {
    latency.set(row[1], {
      reads_per_second: toNumber(row[2]),
      read_p50_ms: Number(row[3]),
      read_p99_ms: Number(row[4]),
    });
  }

  const policies = ["never", "interval", "always"].map((policy) => ({
    policy,
    ...required(throughput.get(policy), `the ${policy} write throughput row`),
    ...required(latency.get(policy), `the ${policy} read latency row`),
  }));

  return {
    source: "README.md, Performance section",
    machine,
    machine_note: machineNote,
    invocation,
    policies,
  };
}

const readme = await readFile(readmePath, "utf8");
const data = parse(readme);
const serialized = `${JSON.stringify(data, null, 2)}\n`;

if (process.argv.includes("--check")) {
  const committed = await readFile(outputPath, "utf8").catch(() => null);
  if (committed !== serialized) {
    console.error(
      "web/src/data/bench.json does not match README.md. Run `npm run bench-data` in web/.",
    );
    process.exit(1);
  }
  console.log("bench.json matches the README");
} else {
  await writeFile(outputPath, serialized);
  console.log(`wrote bench.json for ${String(data.policies.length)} policies`);
}
