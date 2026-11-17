/**
 * The shapes that cross the worker boundary, and the shape of a snapshot.
 *
 * These types are a transcription of `ledgerlog.introspect`'s output, which is
 * the only thing the page is allowed to read the engine through. They are
 * hand-written rather than generated because there is one producer and one
 * consumer and a generator would be a build step to maintain; the Playwright
 * test that boots the engine and reads a real snapshot is what keeps them
 * honest, since a field that moved shows up as a type error or a failed
 * assertion rather than as `undefined` on the page.
 *
 * Keys and values are strings here. The engine stores bytes, and the session
 * glue encodes what it is given as UTF-8 and decodes what it reports the same
 * way (see `session.py`), so the demo can only drive the engine with text. That
 * is what a visitor can type, and the byte lengths beside each field are the
 * real ones.
 */

/** One record in the write-ahead log, as the snapshot reports it. */
export interface WalRecordView {
  offset: number;
  length: number;
  checksum: number;
  kind: "put" | "delete";
  key: string;
  value_length: number;
  expires_at_ms: number | null;
}

/** The log, with what a replay of it would find. */
export interface WalView {
  path: string;
  record_count: number;
  end_offset: number;
  intact: boolean;
  stopped_at: number | null;
  reason: string | null;
  records: WalRecordView[];
}

/** One record held in memory, with the height of its skip list tower. */
export interface MemtableEntryView {
  key: string;
  value: string | null;
  value_length: number | null;
  tombstone: boolean;
  expires_at_ms: number | null;
  level: number;
}

/** One memtable: the active one, or one frozen behind it waiting for a flush. */
export interface MemtableView {
  entry_count: number;
  nbytes: number;
  frozen: boolean;
  entries: MemtableEntryView[];
}

/** One committed SSTable on disk. */
export interface SSTableView {
  sequence: number;
  name: string;
  format_version: number;
  record_count: number;
  size_bytes: number;
  min_key: string | null;
  max_key: string | null;
  index_entry_count: number;
  bloom_bit_count: number;
  bloom_bits_set: number;
  bloom_hash_count: number;
}

/** One group of comparably sized tables, and whether it is worth merging. */
export interface CompactionTierView {
  sequences: number[];
  table_count: number;
  total_size_bytes: number;
  ready: boolean;
}

/** How the tables on disk are currently grouped for compaction. */
export interface CompactionView {
  policy: { size_ratio: number; min_tier_tables: number };
  needs_compaction: boolean;
  tiers: CompactionTierView[];
}

/** Everything the engine holds, in the order a read consults it. */
export interface Snapshot {
  snapshot_version: number;
  directory: string;
  closed: boolean;
  fsync_policy: string;
  memtable_threshold_bytes: number;
  flush_count: number;
  wal: WalView;
  memtable: MemtableView;
  frozen_memtables: MemtableView[];
  sstables: SSTableView[];
  compaction: CompactionView;
}

/** What one table on disk has to say about one key, in the order a read asks it. */
export interface LocatedTable {
  sequence: number;
  name: string;
  bloom_bit_count: number;
  bloom_bit_positions: number[];
  bloom_bits_set_for_key: boolean[];
  bloom_might_contain: boolean;
  searched: boolean;
  indexed: boolean;
  index_offset: number | null;
  scan_from_key: string | null;
  present: boolean;
  record_offset: number | null;
  record_end_offset: number | null;
  tombstone: boolean;
}

/** Where one key is, layer by layer, in the order a `get` consults them. */
export interface LocateResult {
  locate_version: number;
  key: string;
  key_bytes: number;
  wal: WalRecordView | null;
  memtable: (MemtableEntryView & { frozen: boolean }) | null;
  sstables: LocatedTable[];
  answered_by: "memtable" | "frozen_memtable" | "sstable" | null;
  answering_sequence: number | null;
}

/** What one `put` did. */
export interface PutResult {
  key: string;
  value_length: number;
  wal_offset: number;
  expires_at_ms: number | null;
}

/** What one `get` found, keeping "deleted or absent" distinct from "empty". */
export interface GetResult {
  key: string;
  found: boolean;
  value: string | null;
  value_length: number | null;
}

/** What one `delete` did. */
export interface DeleteResult {
  key: string;
  wal_offset: number;
}

/** One table a flush wrote. */
export interface FlushedTable {
  sequence: number;
  name: string;
  record_count: number;
  size_bytes: number;
  min_key: string | null;
  max_key: string | null;
}

/** What one `flush` wrote, oldest table first. */
export interface FlushResult {
  tables: FlushedTable[];
}

/** What one compaction step merged, and what it reclaimed. */
export interface CompactionResult {
  source_sequences: number[];
  output_sequence: number;
  source_record_count: number;
  output_record_count: number;
  records_dropped: number;
}

/** The engine as it is after a `reset`: a fresh database on an empty directory. */
export interface ResetResult {
  directory: string;
}

/** Which Python and which engine source the page is running. */
export interface EngineInfo {
  pyodideVersion: string;
  pythonVersion: string;
  modules: string[];
}

/** The calls the bridge can make, and what each one answers with. */
export interface EngineCalls {
  put: { request: { key: string; value: string; ttl?: number }; response: PutResult };
  get: { request: { key: string }; response: GetResult };
  delete: { request: { key: string }; response: DeleteResult };
  flush: { request: Record<string, never>; response: FlushResult };
  compactOnce: { request: Record<string, never>; response: CompactionResult | null };
  snapshot: { request: Record<string, never>; response: Snapshot };
  locate: { request: { key: string }; response: LocateResult };
  reset: { request: Record<string, never>; response: ResetResult };
}

export type EngineCallName = keyof EngineCalls;

/** A call on its way to the worker. */
export interface CallMessage<Name extends EngineCallName = EngineCallName> {
  kind: "call";
  id: number;
  name: Name;
  payload: EngineCalls[Name]["request"];
}

/** The one message that starts the worker, carrying the URLs it fetches from. */
export interface BootMessage {
  kind: "boot";
  pyodideBase: string;
  engineBase: string;
}

export type WorkerRequest = BootMessage | CallMessage;

/** How far the boot has got. The page shows this; it is not an error channel. */
export type BootPhase = "idle" | "loading-python" | "loading-engine" | "ready" | "failed";

export interface StatusMessage {
  kind: "status";
  phase: BootPhase;
  message: string;
  info?: EngineInfo;
}

export interface ResultMessage {
  kind: "result";
  id: number;
  value: unknown;
}

export interface ErrorMessage {
  kind: "error";
  id: number | null;
  message: string;
}

export type WorkerResponse = StatusMessage | ResultMessage | ErrorMessage;

/**
 * What the page shows about the boot.
 *
 * The message is written here rather than on the page so that the worker and
 * the page cannot disagree about what "ready" means: the worker says it is
 * ready when Python has imported the engine and opened a database, not when the
 * interpreter has loaded.
 */
export const BOOT_MESSAGES: Record<BootPhase, string> = {
  idle: "waiting",
  "loading-python": "starting python",
  "loading-engine": "loading the engine",
  ready: "ready",
  failed: "failed to start",
};
