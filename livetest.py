#!/usr/bin/env python3
"""
Stream live 1-min MESM5 bars with cumulative delta (CVD),
compute rolling 5-bar support/resistance trendlines on the CVD series,
apply real-world trading filters, wait for each trend reversal,
handle stop-loss & take-profit events, and detect cleaner breakouts with re-entry.
"""

import databento as db
import logging
import os
import sys
import numpy as np
from collections import deque
from datetime import datetime, timezone, timedelta
import queue
import threading
import time

# ── Configuration ─────────────────────────────────────────────────────────────
WINDOW_SIZE      = 5      # Number of bars in each trendline window
TOL_PCT          = 0.001  # 0.1% dynamic tolerance for breakout
R_MULTIPLE       = 2      # Reward:risk multiple for target
NO_DATA_TIMEOUT  = 60     # Seconds to wait for data before warning

DATASET = 'GLBX.MDP3'     # Databento parameters (MESM5, CME Globex MDP-3.0, trade prints)
SCHEMA  = 'trades'
SYMBOL  = 'MESM5'

# ── Logging Configuration ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,   # only INFO+ messages
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ── Helper Functions ──────────────────────────────────────────────────────────
def get_cvd_color(close, open_, prev_high, prev_low, strong):
    """Determine CVD color based on price movement."""
    if not strong or prev_high is None or prev_low is None:
        if close > open_: return 'green'
        if close < open_: return 'red'
        return 'gray'
    if close > prev_high: return 'green'
    if close < prev_low:  return 'red'
    return 'gray'

def stream_one_minute_bars(client, strong_updown=True):
    """Aggregate live trade data into 1-minute OHLCV bars with CVD."""
    bar_queue = queue.Queue()
    current = None
    running_cvd = 0
    prev_high = prev_low = None
    last_trade_time = time.time()

    def process_trade(record):
        nonlocal current, running_cvd, prev_high, prev_low, last_trade_time
        # Skip non-trade messages
        if not isinstance(record, db.TradeMsg):
            return

        px = float(record.price) / 1e9
        size = record.size
        side = record.side
        ts_ms = int(record.ts_event // 1_000_000)
        dt = datetime.fromtimestamp(ts_ms / 1000, timezone.utc).replace(second=0, microsecond=0)
        iso = dt.isoformat()

        # Update CVD
        delta = size if side == 'B' else -size
        running_cvd += delta
        last_trade_time = time.time()

        # New minute bar?
        if current is None or current['timestamp'] != iso:
            if current:
                current['cvd'] = current['cvd_running']
                current['cvd_color'] = get_cvd_color(
                    current['close'], current['open'], prev_high, prev_low, strong_updown
                )
                bar_queue.put(current)
                prev_high = current['high']
                prev_low = current['low']

            current = {
                'timestamp': iso,
                'open': px,
                'high': px,
                'low': px,
                'close': px,
                'volume': size,
                'delta': delta,
                'cvd_running': running_cvd,
            }
        else:
            # Update existing bar
            current['high'] = max(current['high'], px)
            current['low'] = min(current['low'], px)
            current['close'] = px
            current['volume'] += size
            current['delta'] += delta
            current['cvd_running'] = running_cvd

    def run_client():
        try:
            client.subscribe(
                dataset=DATASET,
                schema=SCHEMA,
                symbols=[SYMBOL],
                stype_in="raw_symbol",
                snapshot=False
            )
            client.add_callback(process_trade)
            client.start()
            client.block_for_close()
        except Exception as e:
            logging.error(f"Live client error: {e}", exc_info=True)
            bar_queue.put(None)

    threading.Thread(target=run_client, daemon=True).start()

    # Yield only completed bars (block until each minute closes)
    while True:
        bar = bar_queue.get()
        if bar is None:
            logging.error("Live client terminated unexpectedly")
            return
        yield bar

# ── Trendline Fitting ──────────────────────────────────────────────────────────
def check_trend_line(support, pivot, slope, y):
    intercept = -slope * pivot + y[pivot]
    x = np.arange(len(y))
    diffs = (slope * x + intercept) - y
    if support and diffs.max() > 1e-5: return -1.0
    if not support and diffs.min() < -1e-5: return -1.0
    return (diffs ** 2).sum()

def optimize_slope(support, pivot, init_slope, y):
    slope_unit = (y.max() - y.min()) / len(y)
    opt_step, min_step = 1.0, 1e-4
    best_slope = init_slope
    best_err = check_trend_line(support, pivot, best_slope, y)
    get_derivative = True
    derivative = 0.0

    while opt_step > min_step:
        if get_derivative:
            test_slope = best_slope + slope_unit * min_step
            err_test = check_trend_line(support, pivot, test_slope, y)
            if err_test < 0:
                test_slope = best_slope - slope_unit * min_step
                err_test = check_trend_line(support, pivot, test_slope, y)
            derivative = err_test - best_err
            get_derivative = False

        if derivative > 0:
            trial = best_slope - slope_unit * opt_step
        else:
            trial = best_slope + slope_unit * opt_step
        err_test = check_trend_line(support, pivot, trial, y)
        if err_test < 0 or err_test >= best_err:
            opt_step *= 0.5
        else:
            best_slope, best_err = trial, err_test
            get_derivative = True

    intercept = -best_slope * pivot + y[pivot]
    return best_slope, intercept

def fit_trendlines_window(y):
    N = len(y)
    x = np.arange(N)
    slope, _ = np.polyfit(x, y, 1)
    residuals = y - (slope * x)

    upper_pivot = int(np.argmax(residuals))
    lower_pivot = int(np.argmin(residuals))

    sup_slope, sup_int = optimize_slope(True, lower_pivot, slope, y)
    res_slope, res_int = optimize_slope(False, upper_pivot, slope, y)

    support_line = sup_slope * x + sup_int
    resist_line  = res_slope * x + res_int

    last = y[-1]
    tol  = abs(resist_line[-1]) * TOL_PCT
    if last >= resist_line[-1] - tol:
        breakout = 'bullish'
    elif last <= support_line[-1] + tol:
        breakout = 'bearish'
    else:
        breakout = 'none'

    return support_line, resist_line, sup_slope, res_slope, breakout

# ── Formatter ─────────────────────────────────────────────────────────────────
def fmt_pdt(iso_str):
    dt = datetime.fromisoformat(iso_str)
    pdt = dt.astimezone(timezone(timedelta(hours=-7)))
    return pdt.strftime('%I:%M:%S %p')

# ── Main Logic with Reversal Filter, Stop-Loss & Take-Profit ─────────────────
def main():
    # Read API key
    key = ""
    if not key:
        key = input("Enter your Databento API key: ").strip()
    if not key.startswith("db-") or len(key) != 32:
        logging.error("Invalid API key format. Must start with 'db-' and be 32 chars.")
        sys.exit(1)

    # Initialize Databento Live client
    try:
        client = db.Live(key=key)
    except Exception as e:
        logging.error(f"Failed to initialize Live client: {e}")
        sys.exit(1)

    # Initialize state
    cvd_window    = deque(maxlen=WINDOW_SIZE)
    price_window  = deque(maxlen=WINDOW_SIZE)
    volume_window = deque(maxlen=WINDOW_SIZE)
    last_signal   = None
    position      = None
    stop_price    = None
    target_price  = None

    print("📊 Streaming live 1-min MESM5 bars with CVD + Trendlines + Filters + Reversal + Stop-Loss & TP")
    try:
        for i, bar in enumerate(stream_one_minute_bars(client, strong_updown=True), start=1):
            print(
                f"#{i:03d} {fmt_pdt(bar['timestamp'])}  |  "
                f"O:{bar['open']:.2f}  H:{bar['high']:.2f}  "
                f"L:{bar['low']:.2f}  C:{bar['close']:.2f}  "
                f"Vol:{bar['volume']}  CVD:{bar['cvd']}  "
                f"{bar['cvd_color'].upper()}"
            )

            # Stop-loss / take-profit checks
            if position == 'bullish':
                if bar['low'] <= stop_price:
                    print(f"  → 🚫 STOP-LOSS LONG @ {fmt_pdt(bar['timestamp'])} | stop was {stop_price:.2f}\n")
                    position = last_signal = None
                    continue
                if bar['high'] >= target_price:
                    print(f"  → 🎯 TAKE-PROFIT LONG @ {fmt_pdt(bar['timestamp'])} | target was {target_price:.2f}\n")
                    position = last_signal = None
                    continue
            elif position == 'bearish':
                if bar['high'] >= stop_price:
                    print(f"  → 🚫 STOP-LOSS SHORT @ {fmt_pdt(bar['timestamp'])} | stop was {stop_price:.2f}\n")
                    position = last_signal = None
                    continue
                if bar['low'] <= target_price:
                    print(f"  → 🎯 TAKE-PROFIT SHORT @ {fmt_pdt(bar['timestamp'])} | target was {target_price:.2f}\n")
                    position = last_signal = None
                    continue

            # Build rolling windows
            cvd_window.append(bar['cvd'])
            price_window.append(bar['close'])
            volume_window.append(bar['volume'])
            if len(cvd_window) < WINDOW_SIZE or position is not None:
                continue

            # Trendline & breakout detection
            y = np.array(cvd_window)
            sup_line, res_line, sup_slope, res_slope, breakout = fit_trendlines_window(y)

            # Reversal, slope, price, and volume filters
            if breakout == last_signal:
                breakout = 'none'
            if breakout == 'bullish' and res_slope <= 0:
                breakout = 'none'
            if breakout == 'bearish' and sup_slope >= 0:
                breakout = 'none'
            if breakout == 'bullish' and bar['close'] <= max(list(price_window)[:-1]):
                breakout = 'none'
            if breakout == 'bearish' and bar['close'] >= min(list(price_window)[:-1]):
                breakout = 'none'
            avg_vol = sum(list(volume_window)[:-1])/(len(volume_window)-1)
            if breakout in ('bullish','bearish') and bar['volume'] <= avg_vol:
                breakout = 'none'

            # Emit entry
            if breakout in ('bullish', 'bearish'):
                entry = bar['close']
                if breakout == 'bullish':
                    stop_price = min(price_window)
                    R = entry - stop_price
                    target_price = entry + R*R_MULTIPLE
                else:
                    stop_price = max(price_window)
                    R = stop_price - entry
                    target_price = entry - R*R_MULTIPLE

                print(f"    → ENTRY SIGNAL: {breakout.upper()}")
                print(f"       Stop price:   {stop_price:.2f}")
                print(f"       Target price: {target_price:.2f}\n")

                position = last_signal = breakout

    except KeyboardInterrupt:
        logging.info("Interrupted by user; stopping client.")
        client.stop()
        sys.exit(0)
    except db.BentoError as e:
        logging.error(f"Databento error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
