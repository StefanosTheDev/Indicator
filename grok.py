#!/usr/bin/env python3
"""
process_1min_cvd_with_trendlines_with_reversal_filter_and_stop.py

Stream 1-min MESM5 bars with cumulative delta (CVD),
compute rolling 5-bar support/resistance trendlines on the CVD series,
apply real-world trading filters including user-defined market strategies,
wait for each trend reversal, handle stop-loss & take-profit events,
and detect cleaner breakouts with re-entry.
"""

import os
import sys
import json
import requests
import numpy as np
from collections import deque
from datetime import datetime, timezone, timedelta

# ── Configuration ─────────────────────────────────────────────────────────────
PAGE_MS      = 5 * 60 * 1000  # 5-minute paging window
WINDOW_SIZE  = 5              # Number of bars in each trendline window
TOL_PCT      = 0.001          # 0.1% dynamic tolerance for breakout
R_MULTIPLE   = 2              # Reward:risk multiple for target

# Databento parameters (MESM5, CME Globex MDP-3.0, trade prints)
DATASET = 'GLBX.MDP3'
SCHEMA  = 'trades'
SYMBOL  = 'MESM5'

# ── Helper Streams ─────────────────────────────────────────────────────────────
def stream_trades_paged(key, dataset, schema, start_utc, end_utc, symbol):
    cur_start = int(datetime.fromisoformat(start_utc).timestamp() * 1000)
    end_ms    = int(datetime.fromisoformat(end_utc).timestamp() * 1000)
    while cur_start < end_ms:
        cur_end = min(cur_start + PAGE_MS, end_ms)
        resp = requests.get(
            'https://hist.databento.com/v0/timeseries.get_range',
            params={
                'dataset': dataset,
                'schema':  schema,
                'symbols': symbol,
                'start':   datetime.fromtimestamp(cur_start/1000, timezone.utc).isoformat(),
                'end':     datetime.fromtimestamp(cur_end/1000,   timezone.utc).isoformat(),
                'encoding':'json',
            },
            auth=(key, ''),
            stream=True
        )
        resp.raise_for_status()
        last_ts = cur_start
        for line in resp.iter_lines():
            if not line: continue
            trade = json.loads(line)
            ts_ms  = int(int(trade['hd']['ts_event']) // 1_000_000)
            last_ts = ts_ms
            yield {'px': float(trade['price'])/1e9,
                   'size': trade['size'],
                   'side': trade['side'],
                   'ts_ms': ts_ms}
        cur_start = last_ts + 1

# ── CVD Bar Aggregation ────────────────────────────────────────────────────────
def get_cvd_color(close, open_, prev_high, prev_low, strong):
    if not strong:
        if close > open_: return 'green'
        if close < open_: return 'red'
        return 'gray'
    if prev_high is None or prev_low is None:
        if close > open_: return 'green'
        if close < open_: return 'red'
        return 'gray'
    if close > prev_high: return 'green'
    if close < prev_low:  return 'red'
    return 'gray'

def stream_one_minute_bars(key, dataset, schema, start_utc, end_utc, symbol, strong_updown=True):
    current     = None
    running_cvd = 0
    prev_high   = prev_low = None

    for t in stream_trades_paged(key, dataset, schema, start_utc, end_utc, symbol):
        px, size, side, ts_ms = t['px'], t['size'], t['side'], t['ts_ms']
        dt = datetime.fromtimestamp(ts_ms/1000, timezone.utc).replace(second=0, microsecond=0)
        iso = dt.isoformat()

        delta = size if side == 'B' else -size
        running_cvd += delta

        if current is None or current['timestamp'] != iso:
            if current:
                current['cvd']       = current['cvd_running']
                current['cvd_color'] = get_cvd_color(
                    current['close'], current['open'], prev_high, prev_low, strong_updown
                )
                yield current
                prev_high = current['high']
                prev_low  = current['low']

            current = {
                'timestamp':   iso,
                'open':        px,
                'high':        px,
                'low':         px,
                'close':       px,
                'volume':      size,
                'delta':       delta,
                'cvd_running': running_cvd,
            }
        else:
            current['high']        = max(current['high'], px)
            current['low']         = min(current['low'], px)
            current['close']       = px
            current['volume']     += size
            current['delta']      += delta
            current['cvd_running'] = running_cvd

    if current:
        current['cvd']       = current['cvd_running']
        current['cvd_color'] = get_cvd_color(
            current['close'], current['open'], prev_high, prev_low, strong_updown
        )
        yield current

# ── Trendline Fitting ──────────────────────────────────────────────────────────
def check_trend_line(support, pivot, slope, y):
    intercept = -slope * pivot + y[pivot]
    x         = np.arange(len(y))
    diffs     = (slope * x + intercept) - y
    if support and diffs.max() > 1e-5: return -1.0
    if not support and diffs.min() < -1e-5: return -1.0
    return (diffs ** 2).sum()

def optimize_slope(support, pivot, init_slope, y):
    slope_unit     = (y.max() - y.min()) / len(y)
    opt_step, min_step = 1.0, 1e-4
    best_slope     = init_slope
    best_err       = check_trend_line(support, pivot, best_slope, y)
    get_derivative = True
    derivative     = 0.0

    while opt_step > min_step:
        if get_derivative:
            test_slope = best_slope + slope_unit * min_step
            err_test   = check_trend_line(support, pivot, test_slope, y)
            if err_test < 0:
                test_slope = best_slope - slope_unit * min_step
                err_test   = check_trend_line(support, pivot, test_slope, y)
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

    best_intercept = -best_slope * pivot + y[pivot]
    return best_slope, best_intercept

def fit_trendlines_window(y):
    N         = len(y)
    x         = np.arange(N)
    slope, _  = np.polyfit(x, y, 1)
    residuals = y - (slope * x)

    upper_pivot = int(np.argmax(residuals))
    lower_pivot = int(np.argmin(residuals))

    sup_slope, sup_int = optimize_slope(True,  lower_pivot, slope, y)
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
    dt  = datetime.fromisoformat(iso_str)
    pdt = dt.astimezone(timezone(timedelta(hours=-7)))
    return pdt.strftime('%I:%M:%S %p')

# ── Market Strategies Integration ─────────────────────────────────────────────
def apply_market_strategies(bar, cvd_window, price_window, volume_window, breakout):
    """
    Apply user-defined market strategies to confirm or filter breakout signals.
    Parameters:
        bar: Current bar data (dict with 'close', 'volume', 'cvd', etc.)
        cvd_window: Deque of recent CVD values
        price_window: Deque of recent close prices
        volume_window: Deque of recent volume values
        breakout: Initial breakout signal ('bullish', 'bearish', or 'none')
    Returns:
        str: Confirmed or adjusted breakout signal ('bullish', 'bearish', or 'none')
    """
    # Example placeholder strategy: Confirm breakout only if CVD is above its 5-period average
    if len(cvd_window) >= WINDOW_SIZE:
        cvd_avg = sum(cvd_window) / WINDOW_SIZE
        if breakout == 'bullish' and bar['cvd'] <= cvd_avg:
            print("    → filtered by strategy: CVD not above average")
            return 'none'
        if breakout == 'bearish' and bar['cvd'] >= cvd_avg:
            print("    → filtered by strategy: CVD not below average")
            return 'none'

    # Add your custom strategies here, e.g.:
    # - Check additional indicators (e.g., moving averages, RSI)
    # - Custom price action rules
    # - Volume thresholds
    # Example:
    # if breakout == 'bullish' and bar['close'] < some_moving_average:
    #     return 'none'

    return breakout

# ── Main Logic with Strategies, Reversal Filter, Stop-Loss & Take-Profit ──────
def main():
    # Read API key from environment
    key = ""
    if not key:
        print("❌ Please set the DATABENTO_API_KEY environment variable", file=sys.stderr)
        sys.exit(1)

    start         = '2025-05-05T06:30:00-07:00'
    end           = '2025-05-05T12:45:00-07:00'
    cvd_window    = deque(maxlen=WINDOW_SIZE)
    price_window  = deque(maxlen=WINDOW_SIZE)
    volume_window = deque(maxlen=WINDOW_SIZE)
    last_signal   = None  # Track last entry direction
    position      = None  # 'bullish' or 'bearish'
    stop_price    = None
    target_price  = None

    print("📊 Streaming 1-min MESM5 bars with CVD + Trendlines + Strategies + Filters + Reversal + Stop-Loss & TP")
    i = 0
    for bar in stream_one_minute_bars(key, DATASET, SCHEMA, start, end, SYMBOL, True):
        i += 1
        time = fmt_pdt(bar['timestamp'])
        print(f"#{i:<3} {time} | "
              f"O:{bar['open']:.2f} H:{bar['high']:.2f} "
              f"L:{bar['low']:.2f} C:{bar['close']:.2f} "
              f"Vol:{bar['volume']} CVD:{bar['cvd']} "
              f"Color:{bar['cvd_color']}")

        # Stop-loss or take-profit handling
        if position == 'bullish':
            if bar['low'] <= stop_price:
                print(f"  → 🚫 STOP-LOSS LONG @ {time} | stop was {stop_price:.2f}\n")
                position = None; last_signal = None
                continue
            if bar['high'] >= target_price:
                print(f"  → 🎯 TAKE-PROFIT LONG @ {time} | target was {target_price:.2f}\n")
                position = None; last_signal = None
                continue
        elif position == 'bearish':
            if bar['high'] >= stop_price:
                print(f"  → 🚫 STOP-LOSS SHORT @ {time} | stop was {stop_price:.2f}\n")
                position = None; last_signal = None
                continue
            if bar['low'] <= target_price:
                print(f"  → 🎯 TAKE-PROFIT SHORT @ {time} | target was {target_price:.2f}\n")
                position = None; last_signal = None
                continue

        # Accumulate windows
        cvd_window.append(bar['cvd'])
        price_window.append(bar['close'])
        volume_window.append(bar['volume'])
        if len(cvd_window) < WINDOW_SIZE:
            continue

        # Skip new entries while in position
        if position is not None:
            print(f"    → in position ({position}), waiting for exit\n")
            continue

        # Fit trendlines & breakout
        y = np.array(cvd_window)
        sup_line, res_line, sup_slope, res_slope, breakout = fit_trendlines_window(y)

        # Apply market strategies
        breakout = apply_market_strategies(bar, cvd_window, price_window, volume_window, breakout)

        # Reversal filter
        if breakout in ('bullish','bearish') and breakout == last_signal:
            print(f"    → filtered: waiting for reversal from {last_signal}")
            breakout = 'none'

        # Slope filters
        if breakout == 'bullish' and res_slope <= 0:
            print("    → filtered: resistance slope not positive")
            breakout = 'none'
        if breakout == 'bearish' and sup_slope >= 0:
            print("    → filtered: support slope not negative")
            breakout = 'none'

        # Price & volume confirmations
        if breakout == 'bullish' and bar['close'] <= max(list(price_window)[:-1]):
            print("    → filtered: price did not exceed recent highs")
            breakout = 'none'
        if breakout == 'bearish' and bar['close'] >= min(list(price_window)[:-1]):
            print("    → filtered: price did not drop below recent lows")
            breakout = 'none'

        avg_vol = sum(list(volume_window)[:-1]) / (len(volume_window)-1)
        if breakout in ('bullish','bearish') and bar['volume'] <= avg_vol:
            print("    → filtered: volume below recent average")
            breakout = 'none'

        # Emit entry
        if breakout in ('bullish','bearish'):
            entry_price = bar['close']
            if breakout == 'bullish':
                stop_price   = min(list(price_window))
                R            = entry_price - stop_price
                target_price = entry_price + R * R_MULTIPLE
            else:
                stop_price   = max(list(price_window))
                R            = stop_price - entry_price
                target_price = entry_price - R * R_MULTIPLE

            print(f"    → ENTRY SIGNAL: {breakout.upper()}")
            print(f"       Stop price:   {stop_price:.2f}")
            print(f"       Target price: {target_price:.2f}\n")

            position    = breakout
            last_signal = breakout

if __name__ == '__main__':
    main()