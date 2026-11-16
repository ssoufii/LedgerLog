// Copies the two things the demo has to serve itself into web/public/:
// Pyodide's runtime, from the pinned npm package, and the engine's own Python
// source from src/ledgerlog.
//
// Why copy rather than load either from a CDN: the page claims to run this
// repository's engine, and a CDN is a promise that someone else will keep
// serving the same bytes. The demo serves the interpreter and the source out of
// its own build output, so what a visitor runs is the commit they are looking
// at (and the Playwright run, which may have no network, has everything it
// needs on disk).
//
// Why copy rather than import the .py files through the bundler: the engine is
// Python source that a Python interpreter reads at runtime, not a module the
// bundler should touch. Copying keeps one directory of plain files, with a
// manifest naming them, which the worker writes into Pyodide's filesystem
// unchanged.
//
// Run by the predev and prebuild npm scripts, so a dev server and a production
// build both see the current source. Writing into public/ rather than dist/
// means `vite dev` serves the files too, and the copies are generated, so they
// are not committed (see web/.gitignore).

import { createHash } from "node:crypto";
import { cp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const here = dirname(fileURLToPath(import.meta.url));
const webRoot = resolve(here, "..");
const repoRoot = resolve(webRoot, "..");
const publicDir = join(webRoot, "public");

// Everything loadPyodide fetches at runtime. Listed by name rather than copied
// wholesale so that a change in the package's contents is a change to this
// list, visible in a diff, instead of a silent change in what the demo ships.
const PYODIDE_FILES = [
  "pyodide.mjs",
  "pyodide.asm.mjs",
  "pyodide.asm.wasm",
  "python_stdlib.zip",
  "pyodide-lock.json",
];

async function syncPyodide() {
  const packageJson = require.resolve("pyodide/package.json");
  const source = dirname(packageJson);
  const { version } = JSON.parse(await readFile(packageJson, "utf8"));
  const target = join(publicDir, "pyodide");

  await rm(target, { recursive: true, force: true });
  await mkdir(target, { recursive: true });
  for (const name of PYODIDE_FILES) {
    await cp(join(source, name), join(target, name));
  }
  return version;
}

async function syncEngine() {
  const source = join(repoRoot, "src", "ledgerlog");
  const target = join(publicDir, "engine", "ledgerlog");
  const names = (await readdir(source)).filter((name) => name.endsWith(".py")).sort();
  if (names.length === 0) {
    throw new Error(`no Python source found in ${source}`);
  }

  await rm(join(publicDir, "engine"), { recursive: true, force: true });
  await mkdir(target, { recursive: true });
  const modules = [];
  for (const name of names) {
    const body = await readFile(join(source, name));
    await writeFile(join(target, name), body);
    // The digest is here so a reader of the manifest can tell which source the
    // page is running without diffing files; the worker does not check it.
    modules.push({ name, bytes: body.byteLength, sha256: createHash("sha256").update(body).digest("hex") });
  }
  return modules;
}

const pyodideVersion = await syncPyodide();
const modules = await syncEngine();

// One manifest the worker reads, so the list of modules to write into Pyodide's
// filesystem comes from the files that were actually copied rather than from a
// list kept in the TypeScript by hand.
await writeFile(
  join(publicDir, "engine", "manifest.json"),
  `${JSON.stringify({ package: "ledgerlog", pyodideVersion, modules }, null, 2)}\n`,
);

const bytes = modules.reduce((total, module) => total + module.bytes, 0);
console.log(
  `synced pyodide ${pyodideVersion} and ${modules.length} engine modules (${bytes} bytes)`,
);
