import { useId, useState } from "react";

import { BENCH, figuresFor } from "../data/bench";
import { Invocation } from "./Invocation";

/**
 * You can work out what it costs for your workload.
 *
 * One slider and one figure: how long it takes to write N records at the
 * measured `fsync=never` rate. The rate is the README's, the arithmetic is one
 * division, and both are shown in words beside the figure so the number is
 * traceable rather than impressive.
 *
 * The slider is logarithmic because the interesting range spans four orders of
 * magnitude (ten thousand to a hundred million), and a linear slider over that
 * range spends nine tenths of its travel above ten million. The native range
 * input carries the position, and the position is mapped to a record count,
 * so arrow keys, Home, End and a drag all work without a key handler here.
 */
const MIN_RECORDS = 10_000;
const MAX_RECORDS = 100_000_000;
const STEPS = 1000;

const MIN_LOG = Math.log10(MIN_RECORDS);
const MAX_LOG = Math.log10(MAX_RECORDS);

function positionToRecords(position: number): number {
  const exponent = MIN_LOG + (position / STEPS) * (MAX_LOG - MIN_LOG);
  const raw = Math.pow(10, exponent);
  // Rounded to three significant figures, so the number under the slider is
  // one a person would say out loud rather than 1,004,117.
  const magnitude = Math.pow(10, Math.floor(Math.log10(raw)) - 2);
  return Math.round(raw / magnitude) * magnitude;
}

function recordsToPosition(records: number): number {
  return Math.round(((Math.log10(records) - MIN_LOG) / (MAX_LOG - MIN_LOG)) * STEPS);
}

/**
 * Seconds, at a precision that suits the size of the number.
 *
 * Three decimals under a second, one under a hundred, none above: enough
 * figures to be useful and not so many as to claim a precision the
 * measurement does not have.
 */
export function formatSeconds(seconds: number): string {
  if (seconds < 1) {
    return seconds.toFixed(3);
  }
  if (seconds < 100) {
    return seconds.toFixed(1);
  }
  return Math.round(seconds).toLocaleString("en-GB");
}

export function NumbersSlider() {
  const sliderId = useId();
  const [position, setPosition] = useState(recordsToPosition(1_000_000));
  const records = positionToRecords(position);
  const rate = figuresFor("never").records_per_second;
  const seconds = records / rate;

  return (
    <section className="numbers" id="numbers" aria-labelledby="numbers-heading">
      <h2 className="numbers__heading" id="numbers-heading" data-reveal="0">
        how many records?
      </h2>

      <div className="numbers__control" data-ui="records-slider" data-reveal="2">
        <label className="numbers__label" htmlFor={sliderId}>
          records to write
        </label>
        <input
          id={sliderId}
          className="numbers__slider"
          type="range"
          min={0}
          max={STEPS}
          step={1}
          value={position}
          onChange={(event) => setPosition(Number(event.target.value))}
          aria-valuetext={`${records.toLocaleString("en-GB")} records`}
          data-testid="records-slider"
        />
        <output className="numbers__records" htmlFor={sliderId} data-numeric data-testid="records-count">
          {records.toLocaleString("en-GB")}
        </output>
      </div>

      <div className="numbers__result" data-reveal="1">
        <span className="numbers__figure" data-numeric data-testid="records-seconds">
          {formatSeconds(seconds)}
          <span className="numbers__suffix">s</span>
        </span>
      </div>

      <div className="numbers__note" data-reveal="1">
        <p>
          {records.toLocaleString("en-GB")} records divided by {rate.toLocaleString("en-GB")}{" "}
          records per second, the sequential write throughput measured at{" "}
          <code>fsync=never</code> on {BENCH.machine}
        </p>
        <Invocation command={BENCH.invocation} label="the bench invocation behind this rate" />
      </div>
    </section>
  );
}
