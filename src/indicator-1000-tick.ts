import axios from 'axios';
import * as readline from 'node:readline';
import { Readable } from 'stream';

// ── Configuration ─────────────────────────────────────────────────────────────
const PAGE_MS = 5 * 60 * 1000; // paging window for API calls
const TICK_SIZE = 1000; // number of trades per bar
const WINDOW_SIZE = 5; // number of tick‐bars to look back
const TOL_PCT = 0.001; // breakout tolerance

// Databento parameters
const DATASET = 'GLBX.MDP3';
const SCHEMA = 'trades';
const SYMBOL = 'MESM5';

interface Trade {
  px: number;
  size: number;
  side: 'B' | 'S';
  ts_ms: number;
}

interface Bar {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  delta: number;
  cvd_running: number;
  cvd?: number;
  cvd_color?: string;
}

// ── Helper Streams ─────────────────────────────────────────────────────────────
async function* streamTradesPaged(
  key: string,
  dataset: string,
  schema: string,
  startUtc: string,
  endUtc: string,
  symbol: string
): AsyncGenerator<Trade> {
  let curStart = Date.parse(startUtc);
  const endMs = Date.parse(endUtc);

  while (curStart < endMs) {
    const curEnd = Math.min(curStart + PAGE_MS, endMs);
    const resp = await axios.get<Readable>(
      'https://hist.databento.com/v0/timeseries.get_range',
      {
        params: {
          dataset,
          schema,
          symbols: symbol,
          start: new Date(curStart).toISOString(),
          end: new Date(curEnd).toISOString(),
          encoding: 'json',
        },
        auth: { username: key, password: '' },
        responseType: 'stream',
      }
    );

    let lastTs = curStart;
    const rl = readline.createInterface({ input: resp.data });
    for await (const line of rl) {
      if (!line) continue;
      const rec = JSON.parse(line) as any;
      const tsEvent = Number(rec.hd.ts_event) / 1_000_000;
      lastTs = Math.floor(tsEvent);

      yield {
        px: Number(rec.price) / 1e9,
        size: rec.size,
        side: rec.side,
        ts_ms: lastTs,
      };
    }

    curStart = lastTs + 1;
  }
}

// ── CVD color logic ────────────────────────────────────────────────────────────
function getCvdColor(
  close: number,
  open: number,
  prevHigh: number | null,
  prevLow: number | null,
  strong: boolean
): string {
  if (!strong || prevHigh === null || prevLow === null) {
    if (close > open) return 'green';
    if (close < open) return 'red';
    return 'gray';
  }
  if (close > prevHigh) return 'green';
  if (close < prevLow) return 'red';
  return 'gray';
}

// ── Tick‐bar aggregation ───────────────────────────────────────────────────────
async function* streamTickBars(
  key: string,
  dataset: string,
  schema: string,
  startUtc: string,
  endUtc: string,
  symbol: string,
  strongUpdown = true
): AsyncGenerator<Bar> {
  let current: Bar | null = null;
  let runningCvd = 0;
  let prevHigh: number | null = null;
  let prevLow: number | null = null;
  let tickCount = 0;

  for await (const t of streamTradesPaged(
    key,
    dataset,
    schema,
    startUtc,
    endUtc,
    symbol
  )) {
    const { px, size, side, ts_ms } = t;
    const iso = new Date(ts_ms).toISOString();
    const delta = side === 'B' ? size : -size;
    runningCvd += delta;
    tickCount++;

    if (!current) {
      // start new bar
      current = {
        timestamp: iso,
        open: px,
        high: px,
        low: px,
        close: px,
        volume: size,
        delta,
        cvd_running: runningCvd,
      };
    } else {
      // extend bar
      current.high = Math.max(current.high, px);
      current.low = Math.min(current.low, px);
      current.close = px;
      current.volume += size;
      current.delta += delta;
      current.cvd_running = runningCvd;
    }

    if (tickCount >= TICK_SIZE) {
      // finalize and yield
      current.cvd = current.cvd_running;
      current.cvd_color = getCvdColor(
        current.close,
        current.open,
        prevHigh,
        prevLow,
        strongUpdown
      );
      yield current;

      prevHigh = current.high;
      prevLow = current.low;
      current = null;
      tickCount = 0;
    }
  }

  // yield last partial bar if needed
  if (current) {
    current.cvd = current.cvd_running;
    current.cvd_color = getCvdColor(
      current.close,
      current.open,
      prevHigh,
      prevLow,
      strongUpdown
    );
    yield current;
  }
}

// ── Trendline Fitting ──────────────────────────────────────────────────────────
function checkTrendLine(
  support: boolean,
  pivot: number,
  slope: number,
  y: number[]
): number {
  const intercept = -slope * pivot + y[pivot];
  const diffs = y.map((yi, i) => slope * i + intercept - yi);
  if (support && Math.max(...diffs) > 1e-5) return -1;
  if (!support && Math.min(...diffs) < -1e-5) return -1;
  return diffs.reduce((sum, d) => sum + d * d, 0);
}

function optimizeSlope(
  support: boolean,
  pivot: number,
  initSlope: number,
  y: number[]
): [number, number] {
  const slopeUnit = (Math.max(...y) - Math.min(...y)) / y.length;
  let optStep = 1;
  const minStep = 1e-4;
  let bestSlope = initSlope;
  let bestErr = checkTrendLine(support, pivot, bestSlope, y);
  let derivative = 0;
  let getDerivative = true;

  // V3 Dynamic Bailout
  const maxIters = y.length * 20;
  const maxNoImprove = y.length * 5;
  let iters = 0;
  let noImprove = 0;

  while (optStep > minStep) {
    iters++;
    if (iters >= maxIters || noImprove >= maxNoImprove) {
      console.warn(
        `[optimizeSlope] V3 bail-out: iters=${iters}, noImprove=${noImprove}, window=${y.length}`
      );
      break;
    }
    if (getDerivative) {
      let testSlope = bestSlope + slopeUnit * minStep;
      let errTest = checkTrendLine(support, pivot, testSlope, y);
      if (errTest < 0) {
        testSlope = bestSlope - slopeUnit * minStep;
        errTest = checkTrendLine(support, pivot, testSlope, y);
      }
      derivative = errTest - bestErr;
      getDerivative = false;
    }

    const trial =
      derivative > 0
        ? bestSlope - slopeUnit * optStep
        : bestSlope + slopeUnit * optStep;
    const errTest = checkTrendLine(support, pivot, trial, y);
    if (errTest < 0 || errTest >= bestErr) {
      optStep *= 0.5;
      noImprove++;
    } else {
      bestSlope = trial;
      bestErr = errTest;
      getDerivative = true;
      noImprove = 0;
    }
  }

  const bestIntercept = -bestSlope * pivot + y[pivot];
  return [bestSlope, bestIntercept];
}

function fitTrendlinesWindow(y: number[]): {
  supportLine: number[];
  resistLine: number[];
  supSlope: number;
  resSlope: number;
  breakout: 'bullish' | 'bearish' | 'none';
} {
  const N = y.length;
  const x = Array.from({ length: N }, (_, i) => i);
  const meanX = x.reduce((a, b) => a + b, 0) / N;
  const meanY = y.reduce((a, b) => a + b, 0) / N;
  const covXY = x.reduce(
    (sum, xi, i) => sum + (xi - meanX) * (y[i] - meanY),
    0
  );
  const varX = x.reduce((sum, xi) => sum + (xi - meanX) * (xi - meanX), 0);
  const slope = covXY / varX;
  const residuals = y.map((yi, i) => yi - slope * i);

  const upperPivot = residuals.indexOf(Math.max(...residuals));
  const lowerPivot = residuals.indexOf(Math.min(...residuals));

  const [supSlope, supInt] = optimizeSlope(true, lowerPivot, slope, y);
  const [resSlope, resInt] = optimizeSlope(false, upperPivot, slope, y);

  const supportLine = x.map((i) => supSlope * i + supInt);
  const resistLine = x.map((i) => resSlope * i + resInt);
  const last = y[N - 1];
  const tol = Math.abs(resistLine[N - 1]) * TOL_PCT;

  const breakout =
    last >= resistLine[N - 1] - tol
      ? 'bullish'
      : last <= supportLine[N - 1] + tol
      ? 'bearish'
      : 'none';

  return { supportLine, resistLine, supSlope, resSlope, breakout };
}

// ── Formatter ─────────────────────────────────────────────────────────────────
function fmtPdt(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour12: true,
    timeZone: 'America/Los_Angeles',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

// ── Main Logic (entry‐only) ────────────────────────────────────────────────────
async function main() {
  const key = 'db-FCxYPt94JEum59MjiTfpWxPgLfQ8E';
  if (!key) {
    console.error('❌ Please set DATABENTO_API_KEY');
    process.exit(1);
  }
  const start = '2025-05-12T09:30:00-04:00'; // market open (9:30 AM EDT)
  const end = '2025-05-12T13:30:00-04:00'; // four hours later (1:30 PM EDT)

  const cvdWindow: number[] = [];
  const priceWindow: number[] = [];
  const volumeWindow: number[] = [];
  let lastSignal: 'bullish' | 'bearish' | null = null;

  console.log(
    '📊 Streaming 1000-tick MESM5 bars with CVD + Trendlines + Filters'
  );
  let i = 0;

  for await (const bar of streamTickBars(
    key,
    DATASET,
    SCHEMA,
    start,
    end,
    SYMBOL,
    true
  )) {
    i++;
    const time = fmtPdt(bar.timestamp);
    console.log(
      `#${i} ${time} | O:${bar.open.toFixed(2)} H:${bar.high.toFixed(2)} ` +
        `L:${bar.low.toFixed(2)} C:${bar.close.toFixed(2)} ` +
        `Vol:${bar.volume} CVD:${bar.cvd} Color:${bar.cvd_color}`
    );

    // build sliding windows
    cvdWindow.push(bar.cvd!);
    priceWindow.push(bar.close);
    volumeWindow.push(bar.volume);
    if (cvdWindow.length > WINDOW_SIZE) cvdWindow.shift();
    if (priceWindow.length > WINDOW_SIZE) priceWindow.shift();
    if (volumeWindow.length > WINDOW_SIZE) volumeWindow.shift();
    if (cvdWindow.length < WINDOW_SIZE) continue;

    // fit trendlines & detect breakout
    const { supSlope, resSlope, breakout } = fitTrendlinesWindow(cvdWindow);
    let signal = breakout;

    // reversal filter
    if (signal !== 'none' && signal === lastSignal) {
      console.log(`    → filtered: waiting for reversal from ${lastSignal}`);
      signal = 'none';
    }

    // slope filters
    if (signal === 'bullish' && resSlope <= 0) {
      console.log('    → filtered: resistance slope not positive');
      signal = 'none';
    }
    if (signal === 'bearish' && supSlope >= 0) {
      console.log('    → filtered: support slope not negative');
      signal = 'none';
    }

    // price & volume confirmations
    const prevPrices = priceWindow.slice(0, -1);
    if (signal === 'bullish' && bar.close <= Math.max(...prevPrices)) {
      console.log('    → filtered: price did not exceed recent highs');
      signal = 'none';
    }
    if (signal === 'bearish' && bar.close >= Math.min(...prevPrices)) {
      console.log('    → filtered: price did not drop below recent lows');
      signal = 'none';
    }
    const avgVol =
      volumeWindow.slice(0, -1).reduce((a, b) => a + b, 0) /
      (volumeWindow.length - 1);
    if (
      (signal === 'bullish' || signal === 'bearish') &&
      bar.volume <= avgVol
    ) {
      console.log('    → filtered: volume below recent average');
      signal = 'none';
    }

    // ENTRY only
    if (signal !== 'none') {
      console.log(`    → ENTRY SIGNAL: ${signal.toUpperCase()} @ ${time}`);
      lastSignal = signal;
    }
  }
}

main().catch((err) => console.error(err));
