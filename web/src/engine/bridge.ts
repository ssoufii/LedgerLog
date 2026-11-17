/**
 * The page's only way to reach the engine: one typed method per call.
 *
 * Everything the demo shows comes through here, and nothing here decides
 * anything about storage. A method sends a message, waits for the answer with
 * the matching id, and hands back the typed result; the worker runs the Python.
 * The rule this enforces by being the only door is the one in `CLAUDE.md`: the
 * page never reimplements engine behaviour, because it has no way to.
 *
 * Calls made before the engine has finished booting are not refused, they are
 * answered when it is ready (see `worker.ts`), so the page can be written
 * without a "not yet" branch at every call site.
 */

import EngineWorker from "./worker?worker";
import {
  BOOT_MESSAGES,
  type BootPhase,
  type CompactionResult,
  type DeleteResult,
  type EngineCallName,
  type EngineCalls,
  type EngineInfo,
  type FlushResult,
  type GetResult,
  type LocateResult,
  type PutResult,
  type ResetResult,
  type Snapshot,
  type WorkerResponse,
} from "./protocol";

export interface BridgeStatus {
  phase: BootPhase;
  message: string;
  info?: EngineInfo;
  error?: string;
}

export interface EngineBridge {
  /** The boot state, as the worker last reported it. */
  readonly status: BridgeStatus;
  /** Watch the boot state. Returns the function that stops watching. */
  subscribe(listener: (status: BridgeStatus) => void): () => void;
  put(key: string, value: string, ttl?: number): Promise<PutResult>;
  get(key: string): Promise<GetResult>;
  delete(key: string): Promise<DeleteResult>;
  flush(): Promise<FlushResult>;
  compactOnce(): Promise<CompactionResult | null>;
  snapshot(): Promise<Snapshot>;
  /** Where one key is, layer by layer. Costlier than a snapshot: see session.py. */
  locate(key: string): Promise<LocateResult>;
  reset(): Promise<ResetResult>;
  /** Stop the worker. The bridge cannot be used afterwards. */
  dispose(): void;
}

interface Pending {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

/**
 * Where the worker fetches the interpreter and the engine source from.
 *
 * Resolved on the page rather than in the worker because the worker's own URL
 * is a bundled file under `assets/`, so a path relative to it would point at
 * the wrong place. `BASE_URL` is what Vite was built with, so the same code
 * works at the root of a domain and under a sub-path.
 */
function assetBases(): { pyodideBase: string; engineBase: string } {
  const base = new URL(import.meta.env.BASE_URL, location.href);
  return {
    pyodideBase: new URL("pyodide/", base).href,
    engineBase: new URL("engine/", base).href,
  };
}

export function createBridge(): EngineBridge {
  const worker = new EngineWorker();
  const pending = new Map<number, Pending>();
  const listeners = new Set<(status: BridgeStatus) => void>();
  let status: BridgeStatus = { phase: "idle", message: BOOT_MESSAGES.idle };
  let nextId = 1;
  let disposed = false;

  function publish(next: BridgeStatus): void {
    status = next;
    for (const listener of listeners) {
      listener(next);
    }
  }

  worker.addEventListener("message", (event: MessageEvent<WorkerResponse>) => {
    const message = event.data;
    if (message.kind === "status") {
      publish({
        phase: message.phase,
        message: message.message,
        ...(message.info ? { info: message.info } : {}),
      });
      return;
    }
    if (message.kind === "result") {
      pending.get(message.id)?.resolve(message.value);
      pending.delete(message.id);
      return;
    }
    if (message.id === null) {
      // A failure with no call behind it: the boot. Every waiting call fails
      // with it, because none of them can ever be answered now.
      publish({ phase: "failed", message: BOOT_MESSAGES.failed, error: message.message });
      for (const [id, waiter] of pending) {
        waiter.reject(new Error(message.message));
        pending.delete(id);
      }
      return;
    }
    pending.get(message.id)?.reject(new Error(message.message));
    pending.delete(message.id);
  });

  worker.addEventListener("error", (event: ErrorEvent) => {
    publish({
      phase: "failed",
      message: BOOT_MESSAGES.failed,
      error: event.message || "the engine worker stopped",
    });
  });

  const bases = assetBases();
  worker.postMessage({ kind: "boot", ...bases });

  function call<Name extends EngineCallName>(
    name: Name,
    payload: EngineCalls[Name]["request"],
  ): Promise<EngineCalls[Name]["response"]> {
    if (disposed) {
      return Promise.reject(new Error("the engine bridge has been disposed"));
    }
    const id = nextId++;
    return new Promise<EngineCalls[Name]["response"]>((resolve, reject) => {
      pending.set(id, { resolve: resolve as (value: unknown) => void, reject });
      worker.postMessage({ kind: "call", id, name, payload });
    });
  }

  return {
    get status() {
      return status;
    },
    subscribe(listener) {
      listeners.add(listener);
      listener(status);
      return () => listeners.delete(listener);
    },
    put(key, value, ttl) {
      return call("put", ttl === undefined ? { key, value } : { key, value, ttl });
    },
    get(key) {
      return call("get", { key });
    },
    delete(key) {
      return call("delete", { key });
    },
    flush() {
      return call("flush", {});
    },
    compactOnce() {
      return call("compactOnce", {});
    },
    snapshot() {
      return call("snapshot", {});
    },
    locate(key) {
      return call("locate", { key });
    },
    reset() {
      return call("reset", {});
    },
    dispose() {
      disposed = true;
      for (const [id, waiter] of pending) {
        waiter.reject(new Error("the engine bridge has been disposed"));
        pending.delete(id);
      }
      listeners.clear();
      worker.terminate();
    },
  };
}
