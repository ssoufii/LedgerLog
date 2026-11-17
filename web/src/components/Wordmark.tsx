import type { CSSProperties } from "react";

/**
 * "ledgerlog" built from blocks: one solid rectangle per letter, the letter
 * knocked out of it in the page ground, bottoms on one line, 4px gaps.
 *
 * The heights are a real skip list's levels, not a shape that looked right.
 * They came from the engine's own memtable, drawn with the default level
 * probability of 0.25:
 *
 *     import random
 *     from ledgerlog.memtable import SkipList
 *     sl = SkipList(rng=random.Random(8))
 *     for i, c in enumerate("ledgerlog"):
 *         sl.insert(f"{i:02d}:{c}".encode(), b"x")
 *     [level for _k, _v, level in sl.items_with_levels()]
 *     # [2, 2, 3, 2, 1, 1, 1, 2, 4]
 *
 * That is why most of the mark is short and one block is tall: a geometric
 * draw at p=0.25 gives a row of mostly level one nodes with the occasional
 * tower over them, which is what the hero console draws from live data too.
 * The two tallest letters take the maroon; the rest take the dark grey, per
 * section 6.
 *
 * The mark is an image of the product name, so it is announced once rather
 * than letter by letter.
 */
const LEVELS = [2, 2, 3, 2, 1, 1, 1, 2, 4];
const WORD = "ledgerlog";

export function Wordmark() {
  return (
    <div className="wordmark" role="img" aria-label={WORD}>
      {[...WORD].map((letter, index) => {
        const level = LEVELS[index] ?? 1;
        return (
          <span
            // The word has repeated letters, so the position is the identity.
            key={`${letter}-${String(index)}`}
            className="wordmark__block"
            style={{ "--level": level } as CSSProperties}
            data-tall={level >= 3 ? "true" : "false"}
            aria-hidden="true"
          >
            {letter}
          </span>
        );
      })}
    </div>
  );
}
