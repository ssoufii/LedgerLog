import { useCallback, useEffect, useState } from "react";

import type { Snapshot } from "./engine/protocol";
import { engineBridge, useEngineStatus } from "./engine/useEngine";

/**
 * The scaffold's page: boot status, the six engine calls, and what came back.
 *
 * Deliberately plain. This story (M11.2) is about the engine really running in
 * the browser, and the design arrives in M11.3, so everything here is a control
 * the next stories move rather than a layout they keep. What is already true,
 * and has to stay true, is that every number on the page was read from the
 * engine through the bridge: nothing is computed here.
 */
export function App() {
  const bridge = engineBridge();
  const status = useEngineStatus();
  const [key, setKey] = useState("user:1042:login");
  const [value, setValue] = useState("2026-11-16T05:00:00Z");
  const [action, setAction] = useState("");
  const [found, setFound] = useState<string>("");
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

  return (
    <main>
      <h1>ledgerlog</h1>
      <p>
        A log-structured key-value store written from scratch in Python. This page runs the
        engine itself, in a Web Worker, through Pyodide.
      </p>

      <p data-testid="boot-status">{status.message}</p>
      {status.info ? (
        <p data-testid="engine-info">
          python {status.info.pythonVersion} on pyodide {status.info.pyodideVersion},{" "}
          {status.info.modules.length} engine modules imported
        </p>
      ) : null}
      {status.error ? <p data-testid="boot-error">{status.error}</p> : null}

      <section>
        <h2>write</h2>
        <p>
          <label htmlFor="key">key</label>{" "}
          <input
            id="key"
            name="key"
            value={key}
            onChange={(event) => setKey(event.target.value)}
          />
        </p>
        <p>
          <label htmlFor="value">value</label>{" "}
          <input
            id="value"
            name="value"
            value={value}
            onChange={(event) => setValue(event.target.value)}
          />
        </p>
        <p>
          <button type="button" onClick={onPut} disabled={busy}>
            put
          </button>{" "}
          <button type="button" onClick={onGet} disabled={busy}>
            get
          </button>{" "}
          <button type="button" onClick={onDelete} disabled={busy}>
            delete
          </button>{" "}
          <button type="button" onClick={onFlush} disabled={busy}>
            flush
          </button>{" "}
          <button type="button" onClick={onCompact} disabled={busy}>
            compact
          </button>{" "}
          <button type="button" onClick={onReset} disabled={busy}>
            reset
          </button>
        </p>
        <p data-testid="last-action">{action}</p>
        <p data-testid="get-result">{found}</p>
      </section>

      <section>
        <h2>memtable</h2>
        <ul data-testid="memtable-entries">
          {(snapshot?.memtable.entries ?? []).map((entry) => (
            <li key={entry.key} data-key={entry.key} data-level={entry.level}>
              {entry.key} is {entry.tombstone ? "a tombstone" : (entry.value ?? "")} at level{" "}
              {entry.level}
            </li>
          ))}
        </ul>
      </section>

      <section>
        <h2>log</h2>
        <ul data-testid="wal-records">
          {(snapshot?.wal.records ?? []).slice(-6).map((record) => (
            <li key={record.offset}>
              offset {record.offset}, {record.kind} {record.key}, {record.length} bytes, crc32{" "}
              {record.checksum}
            </li>
          ))}
        </ul>
      </section>

      <section>
        <h2>tables</h2>
        <ul data-testid="sstables">
          {(snapshot?.sstables ?? []).map((table) => (
            <li key={table.sequence}>
              {table.name} holds {table.record_count} records in {table.size_bytes} bytes, keys{" "}
              {table.min_key ?? "none"} to {table.max_key ?? "none"}
            </li>
          ))}
        </ul>
        <p data-testid="compaction-state">
          {snapshot === null
            ? ""
            : `${snapshot.compaction.tiers.length} tiers, ${
                snapshot.compaction.needs_compaction ? "one is ready to merge" : "none ready"
              }`}
        </p>
      </section>
    </main>
  );
}
