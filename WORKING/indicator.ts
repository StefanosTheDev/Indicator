// CVD-Trendline Breakout Scalper with Hard-coded 21-EMA Bias Filter
// ------------------------------------------------------------------
// Run with: ts-node cvd_breakout_ema.ts
// Requires: axios, readline, ts-node
import axios from 'axios';
import * as readline from 'node:readline';
import { Readable } from 'stream';

// ── Config ───────────────────────────────────────────────────────────────
const PAGE_MS = 5 * 60 * 1000;
const WINDOW_SIZE = 5;
const TOL_PCT = 0.001;
const R_MULTIPLE = 2;

const DATASET = 'GLBX.MDP3';
const SCHEMA = 'trades';
const SYMBOL = 'MESM5';

// ── Types ─────────────────────────────────────────────────────────────────
interface Trade {
  px: number;
  size: number;
  side: 'A' | 'B' | 'N';
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
  cvd: number;
  cvdColor: string;
  ema21?: number;
}

// ── Helpers ───────────────────────────────────────────────────────────────
const deltaFromSide = (side: 'A' | 'B' | 'N', size: number): number =>
  side === 'B' ? size : side === 'A' ? -size : 0;

function getCvdColor(
  close: number,
  open: number,
  prevHigh: number | null,
  prevLow: number | null
): string {
  if (prevHigh === null || prevLow === null) {
    return close > open ? 'green' : close < open ? 'red' : 'gray';
  }
  if (close > prevHigh) return 'green';
  if (close < prevLow) return 'red';
  return close > open ? 'green' : close < open ? 'red' : 'gray';
}

const updateEma21 = (price: number, prev: number | null): number =>
  prev === null ? price : (2 / 22) * price + (20 / 22) * prev;

// ── Tick stream w/ paging ──────────────────────────────────────────────────
async function* streamTradesPaged(
  key: string,
  dataset: string,
  schema: string,
  symbol: string,
  startUtc: string,
  endUtc: string
): AsyncGenerator<Trade> {
  let cur = Date.parse(startUtc);
  const end = Date.parse(endUtc);

  while (cur < end) {
    const nxt = Math.min(cur + PAGE_MS, end);
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
      lastTs = Math.floor(Number(rec.hd.ts_event) / 1_000_000);
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

// ── Build 1-min bars with CVD & 21-EMA ─────────────────────────────────────
async function* streamOneMinuteBars(
  key: string,
  dataset: string,
  schema: string,
  symbol: string,
  startUtc: string,
  endUtc: string
): AsyncGenerator<Bar> {
  let bar: Bar | null = null;
  let runningCvd = 0;
  let prevHigh: number | null = null;
  let prevLow: number | null = null;
  let ema21: number | null = null;

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

    if (!bar || bar.timestamp !== iso) {
      if (bar) {
        bar.cvd = runningCvd;
        bar.cvdColor = getCvdColor(bar.close, bar.open, prevHigh, prevLow);
        bar.ema21 = ema21 = updateEma21(bar.close, ema21);
        yield bar;
        prevHigh = bar.high;
        prevLow = bar.low;
      }
      bar = {
        timestamp: iso,
        open: px,
        high: px,
        low: px,
        close: px,
        volume: size,
        delta,
        cvd: 0,
        cvdColor: 'gray',
      };
    } else {
      bar.high = Math.max(bar.high, px);
      bar.low = Math.min(bar.low, px);
      bar.close = px;
      bar.volume += size;
      bar.delta += delta;
    }
  }

  if (bar) {
    bar.cvd = runningCvd;
    bar.cvdColor = getCvdColor(bar.close, bar.open, prevHigh, prevLow);
    bar.ema21 = ema21 = updateEma21(bar.close, ema21);
    yield bar;
  }
}

// ── Trend-line fitting ─────────────────────────────────────────────────────
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
  let step = 1;
  let bestSlope = initSlope;
  let bestErr = checkTrendLine(support, pivot, initSlope, y);
  const minStep = 1e-4;

  while (step > minStep) {
    const trial =
      bestErr > 0 ? bestSlope - slopeUnit * step : bestSlope + slopeUnit * step;
    const err = checkTrendLine(support, pivot, trial, y);
    if (err < 0 || err >= bestErr) step *= 0.5;
    else {
      bestSlope = trial;
      bestErr = err;
    }
  }
  return [bestSlope, -bestSlope * pivot + y[pivot]];
}

function fitTrendlinesWindow(y: number[]): {
  supportLine: number[];
  resistLine: number[];
  supSlope: number;
  resSlope: number;
  breakout: 'bullish' | 'bearish' | 'none';
} {
  const N = y.length;
  const x = [...Array(N).keys()];
  const meanX = (N - 1) / 2;
  const meanY = y.reduce((a, b) => a + b, 0) / N;
  const covXY = x.reduce((s, xi, i) => s + (xi - meanX) * (y[i] - meanY), 0);
  const varX = x.reduce((s, xi) => s + (xi - meanX) * (xi - meanX), 0);
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

  const breakout: 'bullish' | 'bearish' | 'none' =
    last >= resistLine[N - 1] - tol
      ? 'bullish'
      : last <= supportLine[N - 1] + tol
      ? 'bearish'
      : 'none';

  return { supportLine, resistLine, supSlope, resSlope, breakout };
}

// ── ET clock formatting ─────────────────────────────────────────────────────
function fmtEt(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour12: true,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZone: 'America/New_York',
  });
}

// ── Main execution ─────────────────────────────────────────────────────────
async function main() {
  const key =
    process.env.DATABENTO_API_KEY ?? 'db-n6Vh58u3yPLaYNAvvLTSnabf9xQ8R';
  if (!key) throw new Error('🔑 Set DATABENTO_API_KEY');

  const start = '2025-05-07T18:00:00-04:00';
  const end = '2025-05-07T21:00:00-04:00';

  const cvdWin: number[] = [];
  const priceWin: number[] = [];
  const volWin: number[] = [];
  let lastSignal: 'bullish' | 'bearish' | null = null;
  let position: 'bullish' | 'bearish' | null = null;
  let stopP = 0;
  let targetP = 0;
  let prevBar: Bar | null = null;

  console.log('📡 Streaming MES 1-min bars w/ 21‑EMA bias filter…');

  for await (const bar of streamOneMinuteBars(
    key,
    DATASET,
    SCHEMA,
    SYMBOL,
    start,
    end
  )) {
    const t = fmtEt(bar.timestamp);
    console.log(
      `# ${t} | C:${bar.close.toFixed(2)} Vol:${bar.volume} CVD:${bar.cvd}`
    );

    // position management
    if (position === 'bullish') {
      if (bar.low <= stopP) {
        console.log(`❌ Long stopped @ ${stopP}`);
        position = lastSignal = null;
      } else if (bar.high >= targetP) {
        console.log(`✅ Long target hit @ ${targetP}`);
        position = lastSignal = null;
      }
    } else if (position === 'bearish') {
      if (bar.high >= stopP) {
        console.log(`❌ Short stopped @ ${stopP}`);
        position = lastSignal = null;
      } else if (bar.low <= targetP) {
        console.log(`✅ Short target hit @ ${targetP}`);
        position = lastSignal = null;
      }
    }

    if (position) {
      console.log('   ↳ in trade – skipping new signals');
      prevBar = bar;
      continue;
    }

    // rolling windows
    cvdWin.push(bar.cvd);
    priceWin.push(bar.close);
    volWin.push(bar.volume);
    if (cvdWin.length < WINDOW_SIZE) {
      prevBar = bar;
      continue;
    }
    if (cvdWin.length > WINDOW_SIZE) {
      cvdWin.shift();
      priceWin.shift();
      volWin.shift();
    }

    // trendline breakout + filters
    let { supSlope, resSlope, breakout } = fitTrendlinesWindow(cvdWin);
    let signal = breakout;
    if (signal === lastSignal) signal = 'none';
    if (signal === 'bullish' && resSlope <= 0) signal = 'none';
    if (signal === 'bearish' && supSlope >= 0) signal = 'none';

    const prevPrices = priceWin.slice(0, -1);
    if (signal === 'bullish' && bar.close <= Math.max(...prevPrices))
      signal = 'none';
    if (signal === 'bearish' && bar.close >= Math.min(...prevPrices))
      signal = 'none';

    const avgVol =
      volWin.slice(0, -1).reduce((a, b) => a + b, 0) / (volWin.length - 1);
    if ((signal === 'bullish' || signal === 'bearish') && bar.volume <= avgVol)
      signal = 'none';

    // 21‑EMA bias + fresh momentum filter
    if (signal !== 'none' && prevBar?.ema21 !== undefined) {
      if (signal === 'bullish') {
        if (!(prevBar.low >= prevBar.ema21 && bar.close > prevBar.close))
          signal = 'none';
      } else {
        if (!(prevBar.high <= prevBar.ema21 && bar.close < prevBar.close))
          signal = 'none';
      }
    }

    if (signal !== 'none') {
      const entry = bar.close;
      if (signal === 'bullish') {
        stopP = Math.min(...priceWin);
        targetP = entry + (entry - stopP) * R_MULTIPLE;
      } else {
        stopP = Math.max(...priceWin);
        targetP = entry - (stopP - entry) * R_MULTIPLE;
      }
      console.log(
        `🔔 ${signal.toUpperCase()} @${entry.toFixed(2)} SL:${stopP.toFixed(
          2
        )} TP:${targetP.toFixed(2)}`
      );
      position = signal;
      lastSignal = signal;
    }

    prevBar = bar;
  }
}

main().catch(console.error);
