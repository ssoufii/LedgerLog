/**
 * The motif at true size: one key's skip list tower.
 *
 * The height is the node's level from the engine's snapshot, not a value this
 * component chooses, which is the whole point of drawing it: a row of these in
 * key order is the memtable's skip list seen from the side, with the real coin
 * flips the engine made when each key arrived (section 1 of
 * docs/FRONTEND_DESIGN.md).
 *
 * Decorative in the accessibility tree. The level is already announced in the
 * text beside the tower, and a screen reader reading four empty blocks would
 * be reading the drawing rather than the data.
 */
import type { CSSProperties } from "react";

import { useEntrance } from "../motion";

export function Tower({ level, tombstone }: { level: number; tombstone: boolean }) {
  // Section 8: a new key's tower builds block by block, bottom first. The
  // index staggers the blocks; the entrance is fixed when the tower mounts,
  // so a keyboard put fades in rather than building (rule 8).
  const entrance = useEntrance();
  return (
    <span
      className="tower__stack"
      data-tombstone={tombstone ? "true" : "false"}
      data-enter={entrance}
      aria-hidden="true"
    >
      {Array.from({ length: level }, (_, index) => (
        <span
          className="tower__block"
          key={index}
          style={{ "--block-index": index } as CSSProperties}
        />
      ))}
    </span>
  );
}
