import axios from 'axios';
import * as readline from 'node:readline';
import { Readable } from 'stream';

// ── Configuration ─────────────────────────────────────────────────────────────
const PAGE_MS = 5 * 60 * 1000; // 5-minute paging window
const WINDOW_SIZE = 5; // bars per trendline window
const TOL_PCT = 0.001; // 0.1% breakout tolerance
const R_MULTIPLE = 2; // reward:risk for TP

// Databento parameters
const DATASET = 'GLBX.MDP3';
const SCHEMA = 'trades';
const SYMBOL = 'MESM5';

interface Trade {
  px: number;
  size: number;
  side: 'A' | 'B' | 'N'; // Ask-hit, Bid-hit, Neutral
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
  if (side === 'B') return +size; // Bid-hit → buy pressure
  if (side === 'A') return -size; // Ask-hit → sell pressure
  return 0; // neutral
}

// ── Stream trades for SYMBOL in 5-min pages ────────────────────────────────────
async function* streamTradesPaged(
  key: string,
  startUtc: string,
  endUtc: string
): AsyncGenerator<Trade> {
  let cur = Date.parse(startUtc);
  const endMs = Date.parse(endUtc);

  while (cur < endMs) {
    const next = Math.min(cur + PAGE_MS, endMs);
    const resp = await axios.get<Readable>(
      'https://hist.databento.com/v0/timeseries.get_range',
      {
        params: {
          dataset: DATASET,
          schema: SCHEMA,
          symbols: SYMBOL,
          start: new Date(cur).toISOString(),
          end: new Date(next).toISOString(),
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

// ── 1-min bar aggregation with CVD ─────────────────────────────────────────────
async function* streamOneMinuteBars(
  key: string,
  startUtc: string,
  endUtc: string,
  strongUpdown = true
): AsyncGenerator<Bar> {
  let current: Bar | null = null;
  let runningCvd = 0;
  let prevHigh: number | null = null;
  let prevLow: number | null = null;

  for await (const t of streamTradesPaged(key, startUtc, endUtc)) {
    const dt = new Date(t.ts_ms);
    dt.setSeconds(0, 0);
    const iso = dt.toISOString();

    const delta = deltaFromSide(t.side, t.size);
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
        open: t.px,
        high: t.px,
        low: t.px,
        close: t.px,
        volume: t.size,
        delta,
        cvd_running: runningCvd,
      };
    } else {
      current.high = Math.max(current.high, t.px);
      current.low = Math.min(current.low, t.px);
      current.close = t.px;
      current.volume += t.size;
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

// ── CVD color helper ───────────────────────────────────────────────────────────
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

// ── Trendline helpers omitted ──────────────────────────────────────────────────
// copy your existing checkTrendLine, optimizeSlope, fitTrendlinesWindow

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

// ── Main logic ────────────────────────────────────────────────────────────────
async function main() {
  const key =
    process.env.DATABENTO_API_KEY ?? 'db-FpKGVJkGx3f9r8XRgmWKgfwbdh4uV';
  if (!key) {
    console.error('❌ Please set DATABENTO_API_KEY');
    process.exit(1);
  }

  const start = '2025-05-07T18:00:00-04:00'; // 6:00 PM EDT
  const end = '2025-05-07T21:00:00-04:00'; // 9:00 PM EDT

  console.log('📊 Streaming 1-min MESM5 bars (EDT)');

  let i = 0;
  for await (const bar of streamOneMinuteBars(key, start, end, true)) {
    const time = fmtEst(bar.timestamp);
    console.log(
      `#${++i} ${time} | ` +
        `O:${bar.open.toFixed(2)} H:${bar.high.toFixed(2)} ` +
        `L:${bar.low.toFixed(2)} C:${bar.close.toFixed(2)} ` +
        `Vol:${bar.volume} CVD:${bar.cvd} Color:${bar.cvd_color}`
    );
  }
}

main().catch(console.error);
