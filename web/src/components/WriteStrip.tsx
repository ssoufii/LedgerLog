import { actions, useEngineState } from "../engine/state";
import { CONSOLE_KEY_INPUT_ID } from "./Console";
import { RecordCard, type CardRow } from "./RecordCard";
import { Tower } from "./Tower";

/**
 * How a write travels: four colour fields, one record card in each.
 *
 * The card follows one key, the visitor's most recent or the demo record the
 * session writes at boot, and every row on it is read from
 * `ledgerlog.introspect.locate`, which walks the engine's own read path. The
 * page works nothing out: which table answers for the key, whether the sparse
 * index points straight at it or sends a reader scanning, and which bloom bits
 * it lands on are all the engine's answers. That is the difference between a
 * diagram of an LSM tree and this.
 *
 * A stage the record has not reached says so and offers the control that would
 * get it there. It never fills the gap with a plausible number, which is the
 * rule in section 9 of docs/FRONTEND_DESIGN.md and the reason the session
 * writes a real record at boot rather than the page inventing one.
 */
const CLAUSES = [
  "First it lands in the log.",
  "Then it is sorted in memory.",
  "Then it is flushed to disk.",
  "Then compaction merges it.",
];

function bytes(count: number): string {
  return `${String(count)} bytes`;
}

export function WriteStrip() {
  const { located, snapshot, trackedKey, lastCompaction, pending } = useEngineState();

  const focusConsole = () => {
    document.getElementById(CONSOLE_KEY_INPUT_ID)?.focus();
  };

  const writeAction = (
    <button type="button" className="button--secondary" onClick={focusConsole}>
      type a key
    </button>
  );

  // Field one: the log the write landed in first.
  const wal = located?.wal ?? null;
  const logRows: CardRow[] = wal
    ? [
        { label: "offset", value: String(wal.offset) },
        { label: "length", value: bytes(wal.length) },
        { label: "crc32", value: String(wal.checksum) },
        { label: "kind", value: wal.kind },
      ]
    : [];

  // Field two: the memtable node, drawn at the height the skip list gave it.
  const node = located?.memtable ?? null;
  const memoryRows: CardRow[] = node
    ? [
        { label: "level", value: String(node.level) },
        { label: "value", value: node.tombstone ? "deleted" : (node.value ?? "") },
        { label: "held in", value: node.frozen ? "a frozen table" : "the active table" },
      ]
    : [];

  // Field three: the first table on disk that holds the key, asked the way a
  // read asks it.
  const table = located?.sstables.find((candidate) => candidate.present) ?? null;
  const diskRows: CardRow[] = table
    ? [
        { label: "table", value: String(table.sequence) },
        {
          label: "sparse index",
          value: table.indexed
            ? "points at it"
            : `scans from ${table.scan_from_key ?? "the start"}`,
        },
        {
          label: "bloom bits",
          value: `${String(table.bloom_bits_set_for_key.filter(Boolean).length)} of ${String(
            table.bloom_bit_positions.length,
          )} set`,
        },
        { label: "record at", value: String(table.record_offset ?? 0) },
      ]
    : [];

  // Field four: the merge, if one has run in this session.
  const mergeRows: CardRow[] = lastCompaction
    ? [
        { label: "merged", value: lastCompaction.source_sequences.join(", ") },
        { label: "into", value: String(lastCompaction.output_sequence) },
        { label: "dropped", value: `${String(lastCompaction.records_dropped)} records` },
        {
          label: "key now in",
          value:
            located?.answering_sequence === null || located?.answering_sequence === undefined
              ? "memory"
              : String(located.answering_sequence),
        },
      ]
    : [];

  /**
   * Whether the engine has heard of this key at all. A card for a stage the
   * record has not reached still names the key it is waiting for; only a key
   * that was never written anywhere has no name to show.
   */
  const everWritten = wal !== null || node !== null || table !== null;
  const followed = everWritten ? (located?.key ?? trackedKey) : null;

  const canCompact = snapshot?.compaction.needs_compaction ?? false;
  const tierSize = snapshot?.compaction.policy.min_tier_tables ?? null;
  const tableCount = snapshot?.sstables.length ?? 0;

  return (
    <section className="strip" id="how-it-works" aria-labelledby="strip-heading">
      <h2 className="strip__heading" id="strip-heading" data-reveal="0">
        how a write travels
      </h2>

      <div className="strip__field" data-field="log">
        <RecordCard
          stage="in the log"
          recordKey={followed}
          rows={logRows}
          {...(wal
            ? {}
            : { missing: "no record for this key in the log", action: writeAction })}
        />
        <p className="strip__clause" data-reveal="1">{CLAUSES[0]}</p>
      </div>

      <div className="strip__field" data-field="memory">
        <RecordCard
          stage="in memory"
          recordKey={followed}
          rows={memoryRows}
          {...(node
            ? { figure: <Tower level={node.level} tombstone={node.tombstone} /> }
            : {
                missing: table
                  ? "flushed out of memory and onto disk"
                  : "not in memory",
                ...(table ? {} : { action: writeAction }),
              })}
        />
        <p className="strip__clause" data-reveal="1">{CLAUSES[1]}</p>
      </div>

      <div className="strip__field" data-field="disk">
        <RecordCard
          stage="on disk"
          recordKey={followed}
          rows={diskRows}
          {...(table
            ? {}
            : {
                missing: "not flushed yet",
                action: (
                  <button
                    type="button"
                    className="button--secondary"
                    disabled={pending}
                    onClick={() => void actions.flush()}
                  >
                    flush it
                  </button>
                ),
              })}
        />
        <p className="strip__clause" data-reveal="1">{CLAUSES[2]}</p>
      </div>

      <div className="strip__field" data-field="merge">
        <RecordCard
          stage="after a merge"
          recordKey={followed}
          rows={mergeRows}
          {...(lastCompaction
            ? {}
            : {
                missing: canCompact
                  ? "a tier is ready to merge"
                  : `${String(tableCount)} tables on disk, and a tier merges at ${String(
                      tierSize ?? 0,
                    )}`,
                ...(canCompact
                  ? {
                      action: (
                        <button
                          type="button"
                          className="button--secondary"
                          disabled={pending}
                          onClick={() => void actions.compact()}
                        >
                          merge it
                        </button>
                      ),
                    }
                  : {}),
              })}
        />
        <p className="strip__clause" data-reveal="1">{CLAUSES[3]}</p>
      </div>
    </section>
  );
}
