/// <reference lib="webworker" />

/**
 * The worker that hosts Python.
 *
 * It boots Pyodide, writes the repository's engine source into Pyodide's
 * filesystem, runs `session.py` to open a database, and from then on does one
 * thing per message: hand the call's name and payload to the Python entry point
 * and post back the JSON string it returns.
 *
 * It runs in a worker and not on the page because booting CPython and running a
 * compaction are both long enough to be felt: on the main thread they would
 * freeze the page mid-animation. Nothing about the engine needs a worker, which
 * is the point of putting the whole boundary in one file.
 *
 * No engine behaviour lives here. The worker does not know what a tombstone is
 * or when a tier is ready; it moves strings.
 */

import type { PyodideAPI } from "pyodide";

import sessionSource from "./session.py?raw";
import {
  BOOT_MESSAGES,
  type BootPhase,
  type EngineInfo,
  type WorkerRequest,
  type WorkerResponse,
} from "./protocol";

const scope = self as unknown as DedicatedWorkerGlobalScope;

interface EngineManifest {
  package: string;
  pyodideVersion: string;
  modules: { name: string; bytes: number; sha256: string }[];
}

/** Where the engine source is written inside Pyodide, and what goes on sys.path. */
const PYTHON_ROOT = "/engine";

/**
 * The shapes this file borrows from Pyodide, named locally.
 *
 * Pyodide's own types describe the filesystem and a Python callable loosely, as
 * an Emscripten module and a proxy that returns `any`, which is honest for a
 * general API and useless at a boundary: an untyped call here would let a
 * misspelled method through to runtime. These say exactly what the worker uses,
 * and the cast is where the borrowing happens, in one place.
 */
interface PyodideFS {
  mkdirTree(path: string): void;
  writeFile(path: string, data: string): void;
}

interface PyodideGlobals {
  get(name: string): unknown;
}

type HandleCall = (name: string, payloadJson: string) => string;

let handleCall: HandleCall | null = null;
let booting: Promise<void> | null = null;

function post(message: WorkerResponse): void {
  scope.postMessage(message);
}

function status(phase: BootPhase, info?: EngineInfo): void {
  post({ kind: "status", phase, message: BOOT_MESSAGES[phase], ...(info ? { info } : {}) });
}

function describe(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

async function fetchText(url: string): Promise<string> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${url} answered ${response.status}`);
  }
  return response.text();
}

async function boot(pyodideBase: string, engineBase: string): Promise<void> {
  status("loading-python");

  // Imported at runtime from the copy in the build output, not bundled: the
  // Pyodide runtime is a 9 MB wasm module plus a stdlib archive that it fetches
  // by URL relative to indexURL, so it has to be served as files. The
  // @vite-ignore comment is what stops the bundler from trying to resolve a URL
  // that only exists once sync-assets.mjs has run.
  const pyodideModule = (await import(/* @vite-ignore */ `${pyodideBase}pyodide.mjs`)) as {
    loadPyodide: (options: { indexURL: string }) => Promise<PyodideAPI>;
  };
  const pyodide = await pyodideModule.loadPyodide({ indexURL: pyodideBase });

  status("loading-engine");
  const manifest = JSON.parse(await fetchText(`${engineBase}manifest.json`)) as EngineManifest;
  const packageDir = `${PYTHON_ROOT}/${manifest.package}`;
  const fs = pyodide.FS as unknown as PyodideFS;
  fs.mkdirTree(packageDir);
  // Fetched in parallel and written in order: nine small files, and the import
  // below is the only thing that reads them, so the writes just have to be
  // finished before it runs.
  const sources = await Promise.all(
    manifest.modules.map(async (module) => ({
      name: module.name,
      body: await fetchText(`${engineBase}${manifest.package}/${module.name}`),
    })),
  );
  for (const source of sources) {
    fs.writeFile(`${packageDir}/${source.name}`, source.body);
  }

  // sys.path first, then the session module, so the import inside session.py
  // resolves to the files just written and to nothing else.
  pyodide.runPython(`import sys\nsys.path.insert(0, ${JSON.stringify(PYTHON_ROOT)})`);
  pyodide.runPython(sessionSource);
  const globals = pyodide.globals as unknown as PyodideGlobals;
  const handle = globals.get("handle") as HandleCall | undefined;
  if (handle === undefined) {
    throw new Error("session.py defined no handle function");
  }
  handleCall = handle;

  const info = JSON.parse(handle("info", "")) as {
    pythonVersion: string;
    modules: string[];
  };
  status("ready", {
    pyodideVersion: manifest.pyodideVersion,
    pythonVersion: info.pythonVersion,
    modules: info.modules,
  });
}

function call(id: number, name: string, payload: unknown): void {
  if (handleCall === null) {
    post({ kind: "error", id, message: "the engine is not ready yet" });
    return;
  }
  try {
    const json = handleCall(name, JSON.stringify(payload ?? {}));
    post({ kind: "result", id, value: JSON.parse(json) });
  } catch (error) {
    // Caught per call, not per worker: a Python exception is an answer to one
    // call (an empty key, a closed engine), and taking the worker down with it
    // would mean a visitor had to reload the page to carry on.
    post({ kind: "error", id, message: describe(error) });
  }
}

scope.addEventListener("message", (event: MessageEvent<WorkerRequest>) => {
  const message = event.data;
  if (message.kind === "boot") {
    booting ??= boot(message.pyodideBase, message.engineBase).catch((error: unknown) => {
      status("failed");
      post({ kind: "error", id: null, message: describe(error) });
    });
    return;
  }
  if (message.kind === "call") {
    // Awaited rather than queued: the boot is one promise, and a call that
    // arrives mid-boot should run when it finishes rather than be refused.
    void (booting ?? Promise.resolve()).then(() => {
      call(message.id, message.name, message.payload);
    });
  }
});

status("idle");
