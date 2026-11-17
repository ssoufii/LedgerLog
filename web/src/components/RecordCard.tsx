import type { ReactNode } from "react";

/**
 * The one card that appears in all four fields of the write strip.
 *
 * The same component in every field, as section 6 asks, because the claim the
 * strip makes is that this is one record moving, not four diagrams of four
 * ideas. What changes field to field is which rows it is given.
 *
 * A fragment at true size, so it carries `data-ui` and may hold the rules and
 * the dense labels a product surface holds.
 */
export interface CardRow {
  label: string;
  value: string;
}

export function RecordCard({
  stage,
  recordKey,
  rows,
  figure,
  missing,
  action,
}: {
  /** Which stage of the write path this card is standing in. */
  stage: string;
  /** The key being followed, or null before anything has been written. */
  recordKey: string | null;
  /** What the engine says about the key here. Empty when it is not here. */
  rows: CardRow[];
  /** Anything drawn rather than listed, such as the memtable tower. */
  figure?: ReactNode;
  /** Why there is nothing to show, when there is nothing to show. */
  missing?: string;
  /** The control that would get the record to this stage. */
  action?: ReactNode;
}) {
  return (
    <div className="record-card" data-ui="record-card" data-stage={stage} data-present={rows.length > 0}>
      <span className="record-card__stage">{stage}</span>
      <span className="record-card__key" title={recordKey ?? ""}>
        {recordKey ?? "no key yet"}
      </span>
      {figure}
      {rows.length > 0 ? (
        <dl className="record-card__rows" data-numeric>
          {rows.map((row) => (
            <div className="record-card__row" key={row.label} data-row={row.label}>
              <dt>{row.label}</dt>
              <dd title={row.value}>{row.value}</dd>
            </div>
          ))}
        </dl>
      ) : null}
      {missing === undefined ? null : <span className="record-card__missing">{missing}</span>}
      {action}
    </div>
  );
}
