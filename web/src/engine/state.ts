/**
 * The engine's state as the page holds it, in one place.
 *
 * Why a store rather than state inside the console: from story M11.5 there are
 * two views of the same engine on the page, the hero console and the four
 * stage strip, and they have to agree. Two components each taking their own
 * snapshot would drift apart the moment one of them refreshed and the other
 * did not, and the page would be showing two different engines. One store, one
 * refresh, one set of numbers.
 *
 * Nothing here decides anything about storage. Every field is either something
 * the engine returned or the key the page is currently following; the actions
 * are thin wrappers that call the bridge and then re-read the engine.
 *
 * A module-level store rather than a React context, for the reason the bridge
 * is module-level (see useEngine.ts): what it fronts is one Web Worker holding
 * one database, which lives as long as the page.
 */

import { useSyncExternalStore } from "react";

import type { CompactionResult, LocateResult, Snapshot } from "./protocol";
import { engineBridge } from "./useEngine";

/**
 * The key the session writes once at boot, mirrored from `session.py`.
 *
 * Mirrored rather than fetched because it is the page's default subject, not a
 * fact about the engine: the strip follows it until the visitor writes
 * something of their own. What the cards show about it is read from the engine
 * either way, so a drift between these two constants shows up as a strip whose
 * first card says the key was never written, not as invented data.
 */
export const DEMO_KEY = "user:1042:login";

export interface EngineState {
  /** The whole engine, as of the last refresh. */
  snapshot: Snapshot | null;
  /** Where `trackedKey` is, layer by layer, as of the last refresh. */
  located: LocateResult | null;
  /** The key the strip is following: the visitor's most recent, else the demo key. */
  trackedKey: string;
  /** The last merge run in this session, which is what card four reports. */
  lastCompaction: CompactionResult | null;
  /** What the last call did, in the console's words. */
  lastAction: string;
  /** What the last get found, kept apart so an empty value is not "not found". */
  lastGet: string;
  /** True while a call is in flight, so a control cannot be pressed twice. */
  pending: boolean;
}

let state: EngineState = {
  snapshot: null,
  located: null,
  trackedKey: DEMO_KEY,
  lastCompaction: null,
  lastAction: "",
  lastGet: "",
  pending: false,
};

const listeners = new Set<() => void>();

function publish(patch: Partial<EngineState>): void {
  state = { ...state, ...patch };
  for (const listener of listeners) {
    listener();
  }
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function snapshotState(): EngineState {
  return state;
}

export function useEngineState(): EngineState {
  return useSyncExternalStore(subscribe, snapshotState);
}

/**
 * Re-read the engine: the whole snapshot, and where the tracked key is.
 *
 * Both in one round trip so that the console and the strip are never showing
 * an engine from two different moments.
 */
export async function refresh(key: string = state.trackedKey): Promise<void> {
  const bridge = engineBridge();
  const [snapshot, located] = await Promise.all([bridge.snapshot(), bridge.locate(key)]);
  publish({ snapshot, located, trackedKey: key });
}

/**
 * Run one call, record what it said, and re-read the engine afterwards.
 *
 * The label and the sentence are the console's readout. A failure is reported
 * in the same place rather than thrown away, because a visitor who pressed a
 * button is owed an answer either way.
 */
async function run(
  label: string,
  key: string,
  call: () => Promise<string>,
): Promise<void> {
  publish({ pending: true });
  try {
    const said = await call();
    publish({ lastAction: `${label}: ${said}` });
    await refresh(key);
  } catch (error) {
    publish({
      lastAction: `${label} failed: ${error instanceof Error ? error.message : String(error)}`,
    });
  } finally {
    publish({ pending: false });
  }
}

export const actions = {
  async put(key: string, value: string): Promise<void> {
    const bridge = engineBridge();
    await run("put", key, async () => {
      const result = await bridge.put(key, value);
      return `${result.key} logged at offset ${String(result.wal_offset)}`;
    });
  },

  async get(key: string): Promise<void> {
    const bridge = engineBridge();
    await run("get", key, async () => {
      const result = await bridge.get(key);
      publish({ lastGet: result.found ? (result.value ?? "") : "not found" });
      return result.found ? `${result.key} is ${String(result.value_length)} bytes` : "not found";
    });
  },

  async delete(key: string): Promise<void> {
    const bridge = engineBridge();
    await run("delete", key, async () => {
      const result = await bridge.delete(key);
      return `${result.key} tombstoned at offset ${String(result.wal_offset)}`;
    });
  },

  async flush(): Promise<void> {
    const bridge = engineBridge();
    await run("flush", state.trackedKey, async () => {
      const result = await bridge.flush();
      if (result.tables.length === 0) {
        return "nothing in memory to flush";
      }
      return result.tables
        .map((table) => `${table.name} holds ${String(table.record_count)} records`)
        .join(", ");
    });
  },

  async compact(): Promise<void> {
    const bridge = engineBridge();
    await run("compact", state.trackedKey, async () => {
      const result = await bridge.compactOnce();
      publish({ lastCompaction: result });
      if (result === null) {
        return "no tier is ready to merge";
      }
      return `merged ${result.source_sequences.join(", ")} into ${String(
        result.output_sequence,
      )}, dropping ${String(result.records_dropped)} records`;
    });
  },

  async reset(): Promise<void> {
    const bridge = engineBridge();
    await run("reset", DEMO_KEY, async () => {
      const result = await bridge.reset();
      // A new database has none of the old one's history, so the page must not
      // keep reporting a merge that happened in a directory that is gone.
      publish({ lastGet: "", lastCompaction: null });
      return `new database at ${result.directory}`;
    });
  },
};
