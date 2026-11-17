import { useCallback, useEffect, useState } from "react";

import type { Snapshot } from "../engine/protocol";
import { engineBridge, useEngineStatus } from "../engine/useEngine";
import { TowerLoader } from "./TowerLoader";

/**
 * The console fragment on the hero stage: the one live thing on the page.
 *
 * This is the scaffold's controls (story M11.2) moved onto the designed stage
 * and given the fragment's own conventions, not a rewrite of them. Everything
 * shown still comes from the engine through the bridge, and nothing here
 * computes anything about storage. Story M11.4 turns the three panels at the
 * bottom into the memtable towers and the WAL tail that section 6 describes;
 * until then they stay as the labelled rows the engine's snapshot already
 * fills, because a panel that showed less than the snapshot knows would be a
 * step backwards.
 *
 * The root carries `data-ui`, which is what section 7 keys its exceptions off:
 * inside a fragment an input may carry a rule and a secondary action may be a
 * filled control, while the page around it keeps no lines and no shadows.
 */
const KEY_INPUT_ID = "console-key";

/** Where the hero's button sends focus. Exported so the hero cannot guess it. */
export const CONSOLE_KEY_INPUT_ID = KEY_INPUT_ID;

export function Console() {
  const bridge = engineBridge();
  const status = useEngineStatus();
  const [key, setKey] = useState("user:1042:login");
  const [value, setValue] = useState("2026-11-16T05:00:00Z");
  const [action, setAction] = useState("");
  const [found, setFound] = useState("");
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);

  const refresh = useCallback(async () => {
    setSnapshot(await bridge.snapshot());
  }, [bridge]);

  // The engine is the external system here: once it reports itself ready, the
  // page reads one snapshot from it so that what is on screen is the engine's
  // state and not an empty placeholder. Every later refresh is driven by a
  // control the visitor pressed.
  useEffect(() => {
    if (status.phase !== "ready") {
      return undefined;
    }
    let live = true;
    void bridge.snapshot().then((next) => {
      if (live) {
        setSnapshot(next);
      }
    });
    return () => {
      live = false;
    };
  }, [status.phase, bridge]);

  const run = useCallback(
    (label: string, call: () => Promise<string>) => {
      void (async () => {
        try {
          setAction(`${label}: ${await call()}`);
          await refresh();
        } catch (error) {
          setAction(`${label} failed: ${error instanceof Error ? error.message : String(error)}`);
        }
      })();
    },
    [refresh],
  );

  const onPut = () => {
    run("put", async () => {
      const result = await bridge.put(key, value);
      return `${result.key} logged at offset ${String(result.wal_offset)}`;
    });
  };

  const onGet = () => {
    run("get", async () => {
      const result = await bridge.get(key);
      setFound(result.found ? (result.value ?? "") : "not found");
      return result.found ? `${result.key} is ${String(result.value_length)} bytes` : "not found";
    });
  };

  const onDelete = () => {
    run("delete", async () => {
      const result = await bridge.delete(key);
      return `${result.key} tombstoned at offset ${String(result.wal_offset)}`;
    });
  };

  const onFlush = () => {
    run("flush", async () => {
      const result = await bridge.flush();
      if (result.tables.length === 0) {
        return "nothing in memory to flush";
      }
      return result.tables
        .map((table) => `${table.name} holds ${String(table.record_count)} records`)
        .join(", ");
    });
  };

  const onCompact = () => {
    run("compact", async () => {
      const result = await bridge.compactOnce();
      if (result === null) {
        return "no tier is ready to merge";
      }
      return `merged ${result.source_sequences.join(", ")} into ${String(
        result.output_sequence,
      )}, dropping ${String(result.records_dropped)} records`;
    });
  };

  const onReset = () => {
    run("reset", async () => {
      const result = await bridge.reset();
      setFound("");
      return `new database at ${result.directory}`;
    });
  };

  const busy = status.phase !== "ready";
  const memtableEntries = snapshot?.memtable.entries ?? [];
  const walRecords = (snapshot?.wal.records ?? []).slice(-6);
  const tables = snapshot?.sstables ?? [];

  return (
    <div className="console" data-ui="console">
      <div className="console__status">
        {busy ? <TowerLoader /> : null}
        <div>
          <div className="console__phase" data-testid="boot-status" role="status">
            {status.message}
          </div>
          {status.info ? (
            <div className="console__note" data-testid="engine-info" data-numeric>
              python {status.info.pythonVersion} on pyodide {status.info.pyodideVersion},{" "}
              {status.info.modules.length} engine modules imported
            </div>
          ) : null}
          {status.error ? (
            <div className="console__note" data-testid="boot-error">
              {status.error}
            </div>
          ) : null}
        </div>
      </div>

      <div className="console__fields">
        <div className="console__field">
          <label className="console__label" htmlFor={KEY_INPUT_ID}>
            key
          </label>
          <input
            className="console__input"
            id={KEY_INPUT_ID}
            name="key"
            value={key}
            onChange={(event) => setKey(event.target.value)}
          />
        </div>
        <div className="console__field">
          <label className="console__label" htmlFor="console-value">
            value
          </label>
          <input
            className="console__input"
            id="console-value"
            name="value"
            value={value}
            onChange={(event) => setValue(event.target.value)}
          />
        </div>
      </div>

      <div className="console__actions">
        <button type="button" onClick={onPut} disabled={busy}>
          put
        </button>
        <button type="button" className="button--secondary" onClick={onGet} disabled={busy}>
          get
        </button>
        <button type="button" className="button--secondary" onClick={onDelete} disabled={busy}>
          delete
        </button>
        <button type="button" className="button--secondary" onClick={onFlush} disabled={busy}>
          flush
        </button>
        <button type="button" className="button--secondary" onClick={onCompact} disabled={busy}>
          compact
        </button>
        <button type="button" className="button--secondary" onClick={onReset} disabled={busy}>
          reset
        </button>
      </div>

      <div className="console__readout" data-numeric>
        <span data-testid="last-action">{action}</span>
        <span data-testid="get-result">{found}</span>
      </div>

      <div className="console__panels" data-numeric>
        <div>
          <span className="console__panel-label" id="panel-memtable">
            in memory
          </span>
          <ul className="console__rows" data-testid="memtable-entries" aria-labelledby="panel-memtable">
            {memtableEntries.map((entry) => (
              <li key={entry.key} data-key={entry.key} data-level={entry.level}>
                {entry.key} is {entry.tombstone ? "a tombstone" : (entry.value ?? "")} at level{" "}
                {entry.level}
              </li>
            ))}
          </ul>
          {memtableEntries.length === 0 ? (
            <span className="console__empty">nothing in memory yet</span>
          ) : null}
        </div>

        <div>
          <span className="console__panel-label" id="panel-log">
            log
          </span>
          <ul className="console__rows" data-testid="wal-records" aria-labelledby="panel-log">
            {walRecords.map((record) => (
              <li key={record.offset}>
                offset {record.offset}, {record.kind} {record.key}, {record.length} bytes, crc32{" "}
                {record.checksum}
              </li>
            ))}
          </ul>
          {walRecords.length === 0 ? (
            <span className="console__empty">the log is empty</span>
          ) : null}
        </div>

        <div>
          <span className="console__panel-label" id="panel-tables">
            on disk
          </span>
          <ul className="console__rows" data-testid="sstables" aria-labelledby="panel-tables">
            {tables.map((table) => (
              <li key={table.sequence}>
                {table.name} holds {table.record_count} records in {table.size_bytes} bytes, keys{" "}
                {table.min_key ?? "none"} to {table.max_key ?? "none"}
              </li>
            ))}
          </ul>
          <span className="console__empty" data-testid="compaction-state">
            {snapshot === null
              ? ""
              : `${String(snapshot.compaction.tiers.length)} tiers, ${
                  snapshot.compaction.needs_compaction ? "one is ready to merge" : "none ready"
                }`}
          </span>
        </div>
      </div>
    </div>
  );
}
