import { useCallback, useEffect, useState } from "react";

import type { Snapshot } from "../engine/protocol";
import { engineBridge, useEngineStatus } from "../engine/useEngine";
import { Tower } from "./Tower";
import { TowerLoader } from "./TowerLoader";

/**
 * The console fragment on the hero stage: the one live thing on the page.
 *
 * Every number, tower and row below comes from one snapshot of the engine,
 * taken after each call. Nothing here decides anything about storage: the
 * tower heights are the levels the skip list drew, the log rows are the bytes
 * the write-ahead log actually holds, and the tiles are the tables a flush
 * wrote. A panel that computed any of that would be the JavaScript imitation
 * the project exists not to be.
 *
 * The root carries `data-ui`, which is what section 7 of
 * docs/FRONTEND_DESIGN.md keys its exceptions off: inside a fragment an input
 * may carry a rule and a secondary action may be a filled control, while the
 * page around it keeps no lines and no shadows.
 */
const KEY_INPUT_ID = "console-key";
const KEY_ERROR_ID = "console-key-error";

/** Where the hero's button sends focus. Exported so the hero cannot guess it. */
export const CONSOLE_KEY_INPUT_ID = KEY_INPUT_ID;

/** The log tail the stage has room for, and the length section 6 asks for. */
const WAL_TAIL = 6;

export function Console() {
  const bridge = engineBridge();
  const status = useEngineStatus();
  const [key, setKey] = useState("user:1042:login");
  const [value, setValue] = useState("2026-11-16T05:00:00Z");
  const [action, setAction] = useState("");
  const [found, setFound] = useState("");
  const [keyError, setKeyError] = useState("");
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

  /**
   * The three calls that name a key refuse an empty one here rather than at
   * the engine, so that the visitor is told which field is wrong while the
   * cursor is still in it. The message is tied to the input with
   * `aria-describedby`, so it is read out with the field and not only seen.
   */
  const withKey = (label: string, call: () => Promise<string>): void => {
    if (key === "") {
      setKeyError("a key is needed: type one and try again");
      document.getElementById(KEY_INPUT_ID)?.focus();
      return;
    }
    setKeyError("");
    run(label, call);
  };

  const onPut = () => {
    withKey("put", async () => {
      const result = await bridge.put(key, value);
      return `${result.key} logged at offset ${String(result.wal_offset)}`;
    });
  };

  const onGet = () => {
    withKey("get", async () => {
      const result = await bridge.get(key);
      setFound(result.found ? (result.value ?? "") : "not found");
      return result.found ? `${result.key} is ${String(result.value_length)} bytes` : "not found";
    });
  };

  const onDelete = () => {
    withKey("delete", async () => {
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
      setKeyError("");
      return `new database at ${result.directory}`;
    });
  };

  const busy = status.phase !== "ready";
  const memtableEntries = snapshot?.memtable.entries ?? [];
  const walRecords = (snapshot?.wal.records ?? []).slice(-WAL_TAIL);
  const tables = snapshot?.sstables ?? [];
  // Section 6: the compact control appears once there is a merge to run. The
  // engine's own plan decides that, so the button cannot offer a merge that
  // `compactOnce` would decline.
  const canCompact = snapshot?.compaction.needs_compaction ?? false;

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

      {/* A form, so that Enter in either field writes the record: that is the
          shortest path through the console and the one a keyboard takes. */}
      <form
        className="console__form"
        onSubmit={(event) => {
          event.preventDefault();
          onPut();
        }}
      >
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
              aria-invalid={keyError === "" ? undefined : true}
              aria-describedby={keyError === "" ? undefined : KEY_ERROR_ID}
              onChange={(event) => {
                setKey(event.target.value);
                setKeyError("");
              }}
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

        {keyError === "" ? null : (
          <p className="console__error" id={KEY_ERROR_ID} role="alert">
            {keyError}
          </p>
        )}

        <div className="console__actions">
          <button type="submit" disabled={busy}>
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
          {canCompact ? (
            <button type="button" className="button--secondary" onClick={onCompact} disabled={busy}>
              compact
            </button>
          ) : null}
          <button type="button" className="button--secondary" onClick={onReset} disabled={busy}>
            reset
          </button>
        </div>
      </form>

      <div className="console__readout" data-numeric>
        <span data-testid="last-action">{action}</span>
        <span data-testid="get-result">{found}</span>
      </div>

      <div className="console__panels" data-numeric>
        <div>
          <span className="console__panel-label" id="panel-memtable">
            in memory
          </span>
          <ul className="towers" data-testid="memtable-entries" aria-labelledby="panel-memtable">
            {memtableEntries.map((entry) => (
              <li
                className="tower"
                key={entry.key}
                data-key={entry.key}
                data-level={entry.level}
                data-tombstone={entry.tombstone ? "true" : "false"}
              >
                <Tower level={entry.level} tombstone={entry.tombstone} />
                {/* The title carries the whole key, because the label below is
                    clipped to the column and the data never is. */}
                <span className="tower__key" title={entry.key}>
                  {entry.key}
                </span>{" "}
                <span className="tower__value" title={entry.tombstone ? "deleted" : (entry.value ?? "")}>
                  {entry.tombstone ? "deleted" : (entry.value ?? "")}
                </span>{" "}
                <span className="tower__level">level {entry.level}</span>
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
              <li key={record.offset} data-offset={record.offset} data-checksum={record.checksum}>
                <span className="wal__cell">offset {record.offset}</span>{" "}
                <span className="wal__cell wal__cell--key" title={`${record.kind} ${record.key}`}>
                  {record.kind} {record.key}
                </span>{" "}
                <span className="wal__cell">{record.length} bytes</span>{" "}
                <span className="wal__cell">crc32 {record.checksum}</span>
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
          <ul className="tiles" data-testid="sstables" aria-labelledby="panel-tables">
            {tables.map((table) => (
              <li className="tile" key={table.sequence} data-sequence={table.sequence}>
                <span className="tile__name" title={table.name}>
                  {table.name}
                </span>{" "}
                <span className="tile__figure">{table.record_count}</span>{" "}
                <span className="tile__unit">records</span>{" "}
                <span className="tile__range">
                  keys {table.min_key ?? "none"} to {table.max_key ?? "none"}
                </span>
              </li>
            ))}
          </ul>
          <span className="console__empty" data-testid="compaction-state">
            {snapshot === null
              ? ""
              : `${String(snapshot.compaction.tiers.length)} tiers, ${
                  canCompact ? "one is ready to merge" : "none ready"
                }`}
          </span>
        </div>
      </div>
    </div>
  );
}
