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
  side: 'A' | 'B' | 'N'; // Ask-hit, Bid-hit, or Neutral
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

// ── Map aggressor side → signed delta ─────────────────────────────────────────
function deltaFromSide(side: 'A' | 'B' | 'N', size: number): number {
  if (side === 'B') return +size; // bid-hit = buy pressure
  if (side === 'A') return -size; // ask-hit = sell pressure
  return 0; // neutral
}

// ── Page-through raw trades from Databento ────────────────────────────────────
async function* streamTradesPaged(
  key: string,
  dataset: string,
  schema: string,
  symbol: string,
  startUtc: string,
  endUtc: string
): AsyncGenerator<Trade> {
  let cur = Date.parse(startUtc);
  const endMs = Date.parse(endUtc);

  while (cur < endMs) {
    const nxt = Math.min(cur + PAGE_MS, endMs);
    const resp = await axios.get<Readable>(
      'https://hist.databento.com/v0/timeseries.get_range',
      {
        params: {
          dataset,
          schema,
          symbols: symbol,
          start: new Date(cur).toISOString(),
          end: new Date(nxt).toISOString(),
          encoding: 'json',
        },
        auth: { username: key, password: '' },
        responseType: 'stream',
      }
    );

    let lastTs = cur;
    const rl = readline.createInterface({ input: resp.data });
    for await (const line of rl) {
      if (!line) continue;
      const rec: any = JSON.parse(line);
      const tsEv = Number(rec.hd.ts_event) / 1_000_000;
      lastTs = Math.floor(tsEv);
      yield {
        px: Number(rec.price) / 1e9,
        size: rec.size,
        side: rec.side,
        ts_ms: lastTs,
      };
    }
    cur = lastTs + 1;
  }
}

// ── Build 1-min bars and true cumulative delta ────────────────────────────────
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
  symbol: string,
  startUtc: string,
  endUtc: string,
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
    symbol,
    startUtc,
    endUtc
  )) {
    const { px, size, side, ts_ms } = t;
    const dt = new Date(ts_ms);
    dt.setSeconds(0, 0);
    const iso = dt.toISOString();

    const delta = deltaFromSide(side, size);
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

// ── Trendline fitting & breakout detection ───────────────────────────────────
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
  let optStep = 1,
    bestSlope = initSlope;
  let bestErr = checkTrendLine(support, pivot, initSlope, y);
  let derivative = 0,
    getDerivative = true;
  const minStep = 1e-4;

  while (optStep > minStep) {
    if (getDerivative) {
      let test = bestSlope + slopeUnit * minStep;
      let err = checkTrendLine(support, pivot, test, y);
      if (err < 0) {
        test = bestSlope - slopeUnit * minStep;
        err = checkTrendLine(support, pivot, test, y);
      }
      derivative = err - bestErr;
      getDerivative = false;
    }
    const trial =
      derivative > 0
        ? bestSlope - slopeUnit * optStep
        : bestSlope + slopeUnit * optStep;
    const errTrial = checkTrendLine(support, pivot, trial, y);
    if (errTrial < 0 || errTrial >= bestErr) {
      optStep *= 0.5;
    } else {
      bestSlope = trial;
      bestErr = errTrial;
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
  const covXY = x.reduce((s, xi, i) => s + (xi - meanX) * (y[i] - meanY), 0);
  const varX = x.reduce((s, xi) => s + (xi - meanX) ** 2, 0);
  const slope = covXY / varX;
  const residuals = y.map((yi, i) => yi - slope * i);

  const upPiv = residuals.indexOf(Math.max(...residuals));
  const loPiv = residuals.indexOf(Math.min(...residuals));

  const [supSlope, supInt] = optimizeSlope(true, loPiv, slope, y);
  const [resSlope, resInt] = optimizeSlope(false, upPiv, slope, y);

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

// ── Print times in PDT ────────────────────────────────────────────────────────
// ── Format Eastern Time ───────────────────────────────────────────────────────
function fmtEst(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour12: true,
    timeZone: 'America/New_York',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

// ── Main execution ───────────────────────────────────────────────────────────
async function main() {
  const key = process.env.DATABENTO_API_KEY ?? '';
  if (!key) {
    console.error('❌ Please set DATABENTO_API_KEY');
    process.exit(1);
  }

  const start = '2025-05-07T18:00:00-04:00'; // 6:00 PM EDT
  const end = '2025-05-07T21:00:00-04:00'; // 9:00 PM EDT

  const cvdWin: number[] = [];
  const pxWin: number[] = [];
  const volWin: number[] = [];

  let lastSig: 'bullish' | 'bearish' | null = null;
  let pos: 'bullish' | 'bearish' | null = null;
  let stopP = 0,
    targP = 0;

  console.log(
    '📊 Streaming 1-min MESM5 bars w/ true CVD + trendlines + entries'
  );
  let i = 0;

  for await (const bar of streamOneMinuteBars(
    key,
    DATASET,
    SCHEMA,
    SYMBOL,
    start,
    end,
    true
  )) {
    i++;
    const t = fmtEst(bar.timestamp);
    console.log(
      `#${i} ${t} | O:${bar.open.toFixed(2)} H:${bar.high.toFixed(2)} ` +
        `L:${bar.low.toFixed(2)} C:${bar.close.toFixed(2)} Vol:${bar.volume} ` +
        `CVD:${bar.cvd} Color:${bar.cvd_color}`
    );

    // Stop-loss / Take-profit
    if (pos === 'bullish') {
      if (bar.low <= stopP) {
        console.log(
          `  → 🚫 STOP-LOSS LONG @ ${t} | stop was ${stopP.toFixed(2)}`
        );
        pos = lastSig = null;
        continue;
      }
      if (bar.high >= targP) {
        console.log(
          `  → 🎯 TAKE-PROFIT LONG @ ${t} | target was ${targP.toFixed(2)}`
        );
        pos = lastSig = null;
        continue;
      }
    } else if (pos === 'bearish') {
      if (bar.high >= stopP) {
        console.log(
          `  → 🚫 STOP-LOSS SHORT @ ${t} | stop was ${stopP.toFixed(2)}`
        );
        pos = lastSig = null;
        continue;
      }
      if (bar.low <= targP) {
        console.log(
          `  → 🎯 TAKE-PROFIT SHORT @ ${t} | target was ${targP.toFixed(2)}`
        );
        pos = lastSig = null;
        continue;
      }
    }

    // Build windows
    cvdWin.push(bar.cvd!);
    pxWin.push(bar.close);
    volWin.push(bar.volume);
    if (cvdWin.length < WINDOW_SIZE) continue;
    if (cvdWin.length > WINDOW_SIZE) cvdWin.shift();
    if (pxWin.length > WINDOW_SIZE) pxWin.shift();
    if (volWin.length > WINDOW_SIZE) volWin.shift();

    // Skip entries if in position
    if (pos) {
      console.log(`    → in position (${pos}), waiting for exit`);
      continue;
    }

    // Fit trendlines & detect breakout
    const { supSlope, resSlope, breakout } = fitTrendlinesWindow(cvdWin);
    let signal = breakout;

    // Reversal filter
    if (signal !== 'none' && signal === lastSig) {
      console.log(`    → filtered: waiting for reversal from ${lastSig}`);
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
    const prevPrices = pxWin.slice(0, -1);
    if (signal === 'bullish' && bar.close <= Math.max(...prevPrices)) {
      console.log('    → filtered: price did not exceed recent highs');
      signal = 'none';
    }
    if (signal === 'bearish' && bar.close >= Math.min(...prevPrices)) {
      console.log('    → filtered: price did not drop below recent lows');
      signal = 'none';
    }
    const avgVol =
      volWin.slice(0, -1).reduce((a, b) => a + b, 0) / (volWin.length - 1);
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
        stopP = Math.min(...pxWin);
        const R = entry - stopP;
        targP = entry + R * R_MULTIPLE;
      } else {
        stopP = Math.max(...pxWin);
        const R = stopP - entry;
        targP = entry - R * R_MULTIPLE;
      }
      console.log(`    → ENTRY SIGNAL: ${signal.toUpperCase()}`);
      console.log(`       Stop price:   ${stopP.toFixed(2)}`);
      console.log(`       Target price: ${targP.toFixed(2)}\n`);
      pos = signal;
      lastSig = signal;
    }
  }
}

main().catch(console.error);
