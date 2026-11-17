import { useState } from "react";

import scenarioData from "../data/crash-scenarios.json";
import type { CrashReport, CrashResult } from "../engine/protocol";
import { engineBridge, useEngineStatus } from "../engine/useEngine";

/**
 * Pull the plug.
 *
 * Six tiles, one per failure the engine's own test suite covers. Pressing one
 * copies the visitor's database inside the worker, damages the copy in that
 * one way, opens a fresh engine on it and reports what came back. The
 * visitor's own database is not touched, which a pytest asserts by fingerprint
 * on every scenario.
 *
 * None of the damage is implemented here. It is `ledgerlog.faults`, the same
 * module the crash tests in `tests/` drive, so a tile cannot demonstrate a
 * recovery the test suite does not also assert. Each tile names the pytest
 * that covers the same failure, and a pytest checks that every name given here
 * is a test that exists.
 */
interface Scenario {
  id: string;
  title: string;
  detail: string;
  test: string;
}

const SCENARIOS: Scenario[] = scenarioData.scenarios;

function keyList(keys: string[]): string {
  return keys.length === 0 ? "none" : keys.join(", ");
}

function Result({ report }: { report: CrashReport }) {
  const rejected = report.tables_rejected[0];
  return (
    <dl className="crash-tile__result" data-numeric>
      <div className="crash-tile__row" data-row="before">
        <dt>before</dt>
        <dd>{keyList(report.keys_before)}</dd>
      </div>
      <div className="crash-tile__row" data-row="recovered">
        <dt>recovered</dt>
        <dd>{keyList(report.keys_after)}</dd>
      </div>
      <div className="crash-tile__row" data-row="dropped" data-dropped={report.dropped.length > 0}>
        <dt>dropped</dt>
        <dd>{keyList(report.dropped)}</dd>
      </div>
      {report.recovery.reason === null ? null : (
        <div className="crash-tile__row" data-row="log">
          <dt>the log</dt>
          <dd>{report.recovery.reason}</dd>
        </div>
      )}
      {rejected === undefined ? null : (
        <div className="crash-tile__row" data-row="table">
          <dt>the table</dt>
          <dd>
            {rejected.name}: {rejected.reason ?? rejected.status}
          </dd>
        </div>
      )}
    </dl>
  );
}

export function CrashTiles() {
  const bridge = engineBridge();
  const status = useEngineStatus();
  const [results, setResults] = useState<Record<string, CrashResult>>({});
  const [running, setRunning] = useState<string | null>(null);

  const run = (id: string) => {
    setRunning(id);
    void (async () => {
      try {
        const result = await bridge.crash(id);
        setResults((previous) => ({ ...previous, [id]: result }));
      } catch (error) {
        setResults((previous) => ({
          ...previous,
          [id]: {
            ok: false,
            reason: error instanceof Error ? error.message : String(error),
          },
        }));
      } finally {
        setRunning(null);
      }
    })();
  };

  const busy = status.phase !== "ready";

  return (
    <section className="crash" id="crash-proof" aria-labelledby="crash-heading">
      <div className="crash__board">
        <div className="crash-tile crash-tile--heading">
          <h2 className="crash__heading" id="crash-heading">
            pull the plug
          </h2>
          <p className="crash__lead">
            Each tile damages a copy of your database in one way, opens the engine on it and
            shows what recovery kept. Your own database is not touched.
          </p>
        </div>

        {SCENARIOS.map((scenario) => {
          const result = results[scenario.id];
          return (
            <button
              key={scenario.id}
              type="button"
              className="crash-tile"
              data-scenario={scenario.id}
              data-done={result !== undefined}
              disabled={busy || running !== null}
              onClick={() => {
                run(scenario.id);
              }}
            >
              <span className="crash-tile__title">{scenario.title}</span>
              {result === undefined ? (
                <span className="crash-tile__detail">
                  {running === scenario.id ? "running it" : scenario.detail}
                </span>
              ) : result.ok ? (
                <Result report={result.report} />
              ) : (
                <span className="crash-tile__detail" data-refused="true">
                  {result.reason}
                </span>
              )}
              {/* A real <code>, because it is a path and a test name, and because
                  the house rules allow monospace nowhere else. */}
              <code className="crash-tile__test" title={scenario.test}>
                {scenario.test}
              </code>
            </button>
          );
        })}
      </div>
    </section>
  );
}
