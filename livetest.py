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
WINDOW_SIZE  = 5              # Number of bars in each trendline window
TOL_PCT      = 0.001          # 0.1% dynamic tolerance for breakout
R_MULTIPLE   = 2              # Reward:risk multiple for target
NO_DATA_TIMEOUT = 60          # Seconds to wait for data before warning

# Databento parameters (MESM5, CME Globex MDP-3.0, trade prints)
DATASET = 'GLBX.MDP3'
SCHEMA  = 'trades'
SYMBOL  = 'MESM5'

# ── Logging Configuration ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,  # Detailed logging for troubleshooting
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ── Helper Functions ──────────────────────────────────────────────────────────
def get_cvd_color(close, open_, prev_high, prev_low, strong):
    """Determine CVD color based on price movement."""
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

def stream_one_minute_bars(client, strong_updown=True):
    """Aggregate live trade data into 1-minute OHLCV bars with CVD."""
    bar_queue = queue.Queue()
    current = None
    running_cvd = 0
    prev_high = prev_low = None
    last_trade_time = time.time()

    def process_trade(record):
        nonlocal current, running_cvd, prev_high, prev_low, last_trade_time
        try:
            # Skip non-trade messages
            if not isinstance(record, db.TradeMsg):
                logging.debug(f"Skipping non-trade message: {type(record).__name__}")
                return

            px = float(record.price) / 1e9
            size = record.size
            side = record.side
            ts_ms = int(record.ts_event // 1_000_000)
            dt = datetime.fromtimestamp(ts_ms / 1000, timezone.utc).replace(second=0, microsecond=0)
            iso = dt.isoformat()

            # Log trade details
            logging.debug(f"Received trade: price={px}, size={size}, side={side}, ts_ms={ts_ms}")

            # Handle side for CVD ('B' = Bid/buy, 'A' = Ask/sell)
            delta = size if side == 'B' else -size
            running_cvd += delta
            last_trade_time = time.time()

            if current is None or current['timestamp'] != iso:
                if current:
                    current['cvd'] = current['cvd_running']
                    current['cvd_color'] = get_cvd_color(
                        current['close'], current['open'], prev_high, prev_low, strong_updown
                    )
                    logging.debug(f"Adding bar to queue: {current}")
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
                logging.debug(f"Started new bar: {current}")
            else:
                current['high'] = max(current['high'], px)
                current['low'] = min(current['low'], px)
                current['close'] = px
                current['volume'] += size
                current['delta'] += delta
                current['cvd_running'] = running_cvd
                logging.debug(f"Updated bar: {current}")

        except Exception as e:
            logging.error(f"Error in process_trade: {e}", exc_info=True)

    # Start the live client in a separate thread
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
            bar_queue.put(None)  # Signal termination

    threading.Thread(target=run_client, daemon=True).start()

    # Yield bars from the queue
    while True:
        try:
            bar = bar_queue.get(timeout=NO_DATA_TIMEOUT)
            if bar is None:
                logging.error("Live client terminated unexpectedly")
                break
            logging.debug(f"Yielding bar: {bar}")
            yield bar
        except queue.Empty:
            if time.time() - last_trade_time > NO_DATA_TIMEOUT:
                logging.warning(f"No trades received for {NO_DATA_TIMEOUT} seconds. Verify MESM5 is active and market is open.")
            if current:
                current['cvd'] = current['cvd_running']
                current['cvd_color'] = get_cvd_color(
                    current['close'], current['open'], prev_high, prev_low, strong_updown
                )
                logging.debug(f"Yielding timed-out bar: {current}")
                yield current
                current = None
            continue

# ── Trendline Fitting ──────────────────────────────────────────────────────────
def check_trend_line(support, pivot, slope, y):
    """Check if a trendline is valid and compute error."""
    intercept = -slope * pivot + y[pivot]
    x = np.arange(len(y))
    diffs = (slope * x + intercept) - y
    if support and diffs.max() > 1e-5: return -1.0
    if not support and diffs.min() < -1e-5: return -1.0
    return (diffs ** 2).sum()

def optimize_slope(support, pivot, init_slope, y):
    """Optimize the slope for a trendline."""
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

    best_intercept = -best_slope * pivot + y[pivot]
    return best_slope, best_intercept

def fit_trendlines_window(y):
    """Fit support and resistance trendlines to a window of CVD values."""
    N = len(y)
    x = np.arange(N)
    slope, _ = np.polyfit(x, y, 1)
    residuals = y - (slope * x)

    upper_pivot = int(np.argmax(residuals))
    lower_pivot = int(np.argmin(residuals))

    sup_slope, sup_int = optimize_slope(True, lower_pivot, slope, y)
    res_slope, res_int = optimize_slope(False, upper_pivot, slope, y)

    support_line = sup_slope * x + sup_int
    resist_line = res_slope * x + res_int

    last = y[-1]
    tol = abs(resist_line[-1]) * TOL_PCT
    if last >= resist_line[-1] - tol:
        breakout = 'bullish'
    elif last <= support_line[-1] + tol:
        breakout = 'bearish'
    else:
        breakout = 'none'

    return support_line, resist_line, sup_slope, res_slope, breakout

# ── Formatter ─────────────────────────────────────────────────────────────────
def fmt_pdt(iso_str):
    """Format UTC timestamp to PDT."""
    dt = datetime.fromisoformat(iso_str)
    pdt = dt.astimezone(timezone(timedelta(hours=-7)))
    return pdt.strftime('%I:%M:%S %p')

# ── Main Logic with Reversal Filter, Stop-Loss & Take-Profit ─────────────────
def main():
    # Read API key
    key_suffix = key[-5:] if key else "None"
    logging.info(f"Using API key with suffix: ...{key_suffix}")

    if not key:
        key = os.environ.get("DATABENTO_API_KEY")
        key_suffix = key[-5:] if key else "None"
        logging.info(f"Falling back to environment variable, API key suffix: ...{key_suffix}")
        if not key:
            logging.error("DATABENTO_API_KEY environment variable not set.")
            key = input("Enter your Databento API key: ")
            key_suffix = key[-5:] if key else "None"
            logging.info(f"Using manually entered API key with suffix: ...{key_suffix}")
        if not key.startswith("db-") or len(key) != 32:
            logging.error("Invalid API key format. Must be a 32-character string starting with 'db-'.")
            sys.exit(1)

    # Initialize Databento Live client
    logging.debug("Initializing Live client")
    try:
        client = db.Live(key=key)
    except Exception as e:
        logging.error(f"Failed to initialize Live client: {e}")
        sys.exit(1)

    # Initialize state
    cvd_window = deque(maxlen=WINDOW_SIZE)
    price_window = deque(maxlen=WINDOW_SIZE)
    volume_window = deque(maxlen=WINDOW_SIZE)
    last_signal = None  # Track last entry direction
    position = None     # 'bullish' or 'bearish'
    stop_price = None
    target_price = None

    print("📊 Streaming live 1-min MESM5 bars with CVD + Trendlines + Filters + Reversal + Stop-Loss & TP")
    i = 0
    try:
        for bar in stream_one_minute_bars(client, strong_updown=True):
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
                    position = None
                    last_signal = None
                    continue
                if bar['high'] >= target_price:
                    print(f"  → 🎯 TAKE-PROFIT LONG @ {time} | target was {target_price:.2f}\n")
                    position = None
                    last_signal = None
                    continue
            elif position == 'bearish':
                if bar['high'] >= stop_price:
                    print(f"  → 🚫 STOP-LOSS SHORT @ {time} | stop was {stop_price:.2f}\n")
                    position = None
                    last_signal = None
                    continue
                if bar['low'] <= target_price:
                    print(f"  → 🎯 TAKE-PROFIT SHORT @ {time} | target was {target_price:.2f}\n")
                    position = None
                    last_signal = None
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

            # Fit trendlines & detect breakout
            y = np.array(cvd_window)
            sup_line, res_line, sup_slope, res_slope, breakout = fit_trendlines_window(y)

            # Reversal filter
            if breakout in ('bullish', 'bearish') and breakout == last_signal:
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

            avg_vol = sum(list(volume_window)[:-1]) / (len(volume_window) - 1)
            if breakout in ('bullish', 'bearish') and bar['volume'] <= avg_vol:
                print("    → filtered: volume below recent average")
                breakout = 'none'

            # Emit entry
            if breakout in ('bullish', 'bearish'):
                entry_price = bar['close']
                if breakout == 'bullish':
                    stop_price = min(list(price_window))
                    R = entry_price - stop_price
                    target_price = entry_price + R * R_MULTIPLE
                else:
                    stop_price = max(list(price_window))
                    R = stop_price - entry_price
                    target_price = entry_price - R * R_MULTIPLE

                print(f"    → ENTRY SIGNAL: {breakout.upper()}")
                print(f"       Stop price:   {stop_price:.2f}")
                print(f"       Target price: {target_price:.2f}\n")

                position = breakout
                last_signal = breakout

    except db.BentoError as e:
        logging.error(f"Databento error: {e}")
        logging.info("Verify API key and permissions in the Databento portal (https://databento.com/portal).")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt, stopping live client")
        client.stop()
        sys.exit(0)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()