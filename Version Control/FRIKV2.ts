import { AStrategy } from '../strategy/a.strategy';
import { WorldCandle } from '../world.candle';
import { AutoTrade, CandleAI } from '../../stock/polygon/options.model';

const name = 'CD';
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

  constructor(bt: AutoTrade) {
    super(name, bt);
  }

  onCandle(wc: WorldCandle, bar: CandleAI) {
    const me = this;
    // console.info(x.t, x.cdv);

    let cvdWindow = me.cvdWindow;
    let priceWindow = me.priceWindow;
    let volumeWindow = me.volumeWindow;

    let close = bar.c;
    let vol = bar.vol;

    cvdWindow.push(bar.cdv);
    priceWindow.push(close);
    volumeWindow.push(vol);
    if (cvdWindow.length < WINDOW_SIZE) return;
    if (cvdWindow.length > WINDOW_SIZE) me.cvdWindow.shift();
    if (priceWindow.length > WINDOW_SIZE) me.priceWindow.shift();
    if (volumeWindow.length > WINDOW_SIZE) me.volumeWindow.shift();


    // Fit trendlines & detect breakout
    const { supSlope, resSlope, breakout } = fitTrendlinesWindow(cvdWindow);
    let signal = breakout;

    // Reversal filter
    if (signal !== 'none' && signal === me.lastSignal) {
      // console.info(`    → filtered: waiting for reversal from ${me.lastSignal}`);
      signal = 'none';
    }

    // Slope filters
    if (signal === 'bullish' && resSlope <= 0) {
      // console.info('    → filtered: resistance slope not positive');
      signal = 'none';
    }
    if (signal === 'bearish' && supSlope >= 0) {
      // console.info('    → filtered: support slope not negative');
      signal = 'none';
    }

    // Price & volume confirmations
    const prevPrices = priceWindow.slice(0, -1);
    if (signal === 'bullish' && close <= Math.max(...prevPrices)) {
      // console.info('    → filtered: price did not exceed recent highs');
      signal = 'none';
    }
    if (signal === 'bearish' && close >= Math.min(...prevPrices)) {
      // console.info('    → filtered: price did not drop below recent lows');
      signal = 'none';
    }
    const avgVol =
      volumeWindow.slice(0, -1).reduce((a, b) => a + b, 0) /
      (volumeWindow.length - 1);
    if (
      (signal === 'bullish' || signal === 'bearish') &&
      vol <= avgVol
    ) {
      // console.info('    → filtered: volume below recent average');
      signal = 'none';
    }

    // Entry logic
    if (signal !== 'none') {
      const entry = close;
      if (signal === 'bullish') {
        me.stopPrice = Math.min(...priceWindow);
        const R = entry - me.stopPrice;
        me.targetPrice = entry + R * R_MULTIPLE;
      } else {
        me.stopPrice = Math.max(...priceWindow);
        const R = me.stopPrice - entry;
        me.targetPrice = entry - R * R_MULTIPLE;
      }
      console.info(`    → ENTRY SIGNAL: ${signal.toUpperCase()}`);
      // console.info(`       Stop price:   ${me.stopPrice.toFixed(2)}`);
      // console.info(`       Target price: ${me.targetPrice.toFixed(2)}`);
      // console.info('');
      
      wc.tradeMan.tradeManTakeTrade(me.bt, name, 0, signal === 'bullish');

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
