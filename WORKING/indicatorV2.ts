import { AStrategy } from '../strategy/a.strategy';
import { WorldCandle } from '../world.candle';
import { AutoTrade, CandleAI } from '../../stock/polygon/options.model';

const WINDOW_SIZE = 5; // Number of bars in each trendline window
const TOL_PCT = 0.001; // 0.1% dynamic tolerance for breakout
const R_MULTIPLE = 2; // Reward:risk multiple for target

export class CdStrategy extends AStrategy {
  cvdWindow: number[] = [];
  priceWindow: number[] = [];
  volumeWindow: number[] = [];
  lastSignal: 'bullish' | 'bearish' | null = null;
  position: 'bullish' | 'bearish' | null = null;
  stopPrice = 0;
  targetPrice = 0;

  constructor(name: string, bt: AutoTrade) {
    console.log('*** CD init', name, bt.sym);
    super(name, bt);
  }

  onCandle(wc: WorldCandle, bar: CandleAI, prev: CandleAI) {
    const me = this;

    // ── Build rolling windows ────────────────────────────────────────────────
    me.cvdWindow.push(bar.cvd);
    me.priceWindow.push(bar.c);
    me.volumeWindow.push(bar.vol);

    if (me.cvdWindow.length < WINDOW_SIZE) return;

    if (me.cvdWindow.length > WINDOW_SIZE) me.cvdWindow.shift();
    if (me.priceWindow.length > WINDOW_SIZE) me.priceWindow.shift();
    if (me.volumeWindow.length > WINDOW_SIZE) me.volumeWindow.shift();

    // ── Trendline breakout & filters ────────────────────────────────────────
    const { supSlope, resSlope, breakout } = fitTrendlinesWindow(me.cvdWindow);
    let signal = breakout;

    // Reversal filter
    if (signal !== 'none' && signal === me.lastSignal) {
      signal = 'none';
    }

    // Slope filters
    if (signal === 'bullish' && resSlope <= 0) {
      signal = 'none';
    }
    if (signal === 'bearish' && supSlope >= 0) {
      signal = 'none';
    }

    // Price & volume confirmations
    const prevPrices = me.priceWindow.slice(0, -1);
    if (signal === 'bullish' && bar.c <= Math.max(...prevPrices)) {
      signal = 'none';
    }
    if (signal === 'bearish' && bar.c >= Math.min(...prevPrices)) {
      signal = 'none';
    }
    const avgVol =
      me.volumeWindow.slice(0, -1).reduce((sum, v) => sum + v, 0) /
      (me.volumeWindow.length - 1);
    if ((signal === 'bullish' || signal === 'bearish') && bar.vol <= avgVol) {
      signal = 'none';
    }

    // ── Entry logic with hard-coded 21-EMA filter ───────────────────────────
    if (signal !== 'none') {
      const entry = bar.c;

      // compute stops & targets
      if (signal === 'bullish') {
        me.stopPrice = Math.min(...me.priceWindow);
        const R = entry - me.stopPrice;
        me.targetPrice = entry + R * R_MULTIPLE;
      } else {
        me.stopPrice = Math.max(...me.priceWindow);
        const R = me.stopPrice - entry;
        me.targetPrice = entry - R * R_MULTIPLE;
      }

      const dir = signal === 'bullish';
      const p = wc.lastPrice;

      // Frik’s 21-EMA filter:
      if (dir) {
        // long only if prev.low ≥ prev.e21 AND fresh momentum (bar.c > prev.c)
        if (prev.l >= prev.e21 && bar.c > prev.c) {
          // V4 21 EMA
          wc.tradeMan.tradeManTakeTrade(me.bt, me.name, p, dir);
        }
      } else {
        // short only if prev.high ≤ prev.e21 AND fresh momentum (bar.c < prev.c)
        if (prev.h <= prev.e21 && bar.c < prev.c) {
          // V4 21 EMA
          wc.tradeMan.tradeManTakeTrade(me.bt, me.name, p, dir);
        }
      }

      me.position = signal;
      me.lastSignal = signal;
    }
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
