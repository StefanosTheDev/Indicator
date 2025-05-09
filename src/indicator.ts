import axios from 'axios';
import * as readline from 'node:readline';
import { Readable } from 'stream';

// ── Configuration ─────────────────────────────────────────────────────────────
const PAGE_MS = 5 * 60 * 1000; // 5-minute paging window
const WINDOW_SIZE = 5; // Number of bars in each trendline window
const TOL_PCT = 0.001; // 0.1% dynamic tolerance for breakout
const R_MULTIPLE = 2; // Reward:risk multiple for target

// Databento parameters
const DATASET = 'GLBX.MDP3';
const SCHEMA = 'trades';
const SYMBOL = 'MESM5';

interface Trade {
  px: number;
  size: number;
  // https://databento.com/docs/standards-and-conventions/common-fields-enums-types#side?historical=http&live=python&reference=http
  side: 'B' | 'S';  // 'A' | 'B' | 'N'
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

// ── CVD bar aggregation ────────────────────────────────────────────────────────
function getCvdColor(
  close: number,
  open: number,
  prevHigh: number | null,
  prevLow: number | null,
  strong: boolean
): string {
  if (!strong) {
    if (close > open) return 'green';
    if (close < open) return 'red';
    return 'gray';
  }
  if (prevHigh === null || prevLow === null) {
    if (close > open) return 'green';
    if (close < open) return 'red';
    return 'gray';
  }
  if (close > prevHigh) return 'green';
  if (close < prevLow) return 'red';
  return 'gray';
}

async function* streamOneMinuteBars(
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

  for await (const t of streamTradesPaged(
    key,
    dataset,
    schema,
    startUtc,
    endUtc,
    symbol
  )) {
    const { px, size, side, ts_ms } = t;
    const dt = new Date(ts_ms);
    dt.setSeconds(0, 0);
    const iso = dt.toISOString();
    // consider neutral bars; should not impact delta
    const delta = side === 'B' ? size : -size;
    runningCvd += delta;

    if (!current || current.timestamp !== iso) {
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
        prevHigh = current.high;
        prevLow = current.low;
      }

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
      current.high = Math.max(current.high, px);
      current.low = Math.min(current.low, px);
      current.close = px;
      current.volume += size;
      current.delta += delta;
      current.cvd_running = runningCvd;
    }
  }

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

  while (optStep > minStep) {
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
    } else {
      bestSlope = trial;
      bestErr = errTest;
      getDerivative = true;
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
  const dt = new Date(iso);
  return dt.toLocaleTimeString('en-US', {
    hour12: true,
    timeZone: 'America/Los_Angeles',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

// ── Main Logic ───────────────────────────────────────────────────────────────
async function main() {
  const key = 'db-QnnN73X5bwsvBSV74f6GFFuHRKSAV';
  if (!key) {
    console.error('❌ Please set DATABENTO_API_KEY');
    process.exit(1);
  }

  const start = '2025-05-05T06:30:00-07:00'; // 6:30 AM PDT
  const end = '2025-05-05T12:45:00-07:00'; // 12:45 PM PDT

  const cvdWindow: number[] = [];
  const priceWindow: number[] = [];
  const volumeWindow: number[] = [];

  let lastSignal: 'bullish' | 'bearish' | null = null;
  let position: 'bullish' | 'bearish' | null = null;
  let stopPrice = 0;
  let targetPrice = 0;

  console.log(
    '📊 Streaming 1-min MESM5 bars with CVD + Trendlines + Filters + Reversal + Stop-Loss & TP'
  );
  let i = 0;

  for await (const bar of streamOneMinuteBars(
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

    // Stop-loss / Take-profit
    if (position === 'bullish') {
      if (bar.low <= stopPrice) {
        console.log(
          `  → 🚫 STOP-LOSS LONG @ ${time} | stop was ${stopPrice.toFixed(2)}`
        );
        position = null;
        lastSignal = null;
        continue;
      }
      if (bar.high >= targetPrice) {
        console.log(
          `  → 🎯 TAKE-PROFIT LONG @ ${time} | target was ${targetPrice.toFixed(
            2
          )}`
        );
        position = null;
        lastSignal = null;
        continue;
      }
    } else if (position === 'bearish') {
      if (bar.high >= stopPrice) {
        console.log(
          `  → 🚫 STOP-LOSS SHORT @ ${time} | stop was ${stopPrice.toFixed(2)}`
        );
        position = null;
        lastSignal = null;
        continue;
      }
      if (bar.low <= targetPrice) {
        console.log(
          `  → 🎯 TAKE-PROFIT SHORT @ ${time} | target was ${targetPrice.toFixed(
            2
          )}`
        );
        position = null;
        lastSignal = null;
        continue;
      }
    }

    // Accumulate windows
    cvdWindow.push(bar.cvd!);
    priceWindow.push(bar.close);
    volumeWindow.push(bar.volume);
    if (cvdWindow.length < WINDOW_SIZE) continue;
    if (cvdWindow.length > WINDOW_SIZE) cvdWindow.shift();
    if (priceWindow.length > WINDOW_SIZE) priceWindow.shift();
    if (volumeWindow.length > WINDOW_SIZE) volumeWindow.shift();

    // Skip entries if in position
    if (position) {
      console.log(`    → in position (${position}), waiting for exit`);
      continue;
    }

    // Fit trendlines & detect breakout
    const { supSlope, resSlope, breakout } = fitTrendlinesWindow(cvdWindow);
    let signal = breakout;

    // Reversal filter
    if (signal !== 'none' && signal === lastSignal) {
      console.log(`    → filtered: waiting for reversal from ${lastSignal}`);
      signal = 'none';
    }

    // Slope filters
    if (signal === 'bullish' && resSlope <= 0) {
      console.log('    → filtered: resistance slope not positive');
      signal = 'none';
    }
    if (signal === 'bearish' && supSlope >= 0) {
      console.log('    → filtered: support slope not negative');
      signal = 'none';
    }

    // Price & volume confirmations
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

    // Entry logic
    if (signal !== 'none') {
      const entry = bar.close;
      if (signal === 'bullish') {
        stopPrice = Math.min(...priceWindow);
        const R = entry - stopPrice;
        targetPrice = entry + R * R_MULTIPLE;
      } else {
        stopPrice = Math.max(...priceWindow);
        const R = stopPrice - entry;
        targetPrice = entry - R * R_MULTIPLE;
      }
      console.log(`    → ENTRY SIGNAL: ${signal.toUpperCase()}`);
      console.log(`       Stop price:   ${stopPrice.toFixed(2)}`);
      console.log(`       Target price: ${targetPrice.toFixed(2)}`);
      console.log('');
      position = signal;
      lastSignal = signal;
    }
  }
}

main().catch((err) => console.error(err));
