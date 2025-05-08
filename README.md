# Indicator Cumulative Delta Breakout Trading Bot

A high-frequency trading algorithm for the Micro E-mini S&P 500 futures (MESM5) that:

- Streams 1‑minute bars with cumulative volume delta (CVD) from Databento
- Fits rolling support/resistance trendlines on CVD
- Applies multi‑stage filters (reversal, slope, price, volume)
- Executes entry/exit signals with stop‑loss and take‑profit management

---

## Table of Contents
1. [Overview](#overview)
2. [Configuration](#configuration)
3. [Installation](#installation)
4. [Data Streaming](#data-streaming)
5. [CVD Aggregation](#cvd-aggregation)
6. [Trendline Fitting](#trendline-fitting)
7. [Signal Filters](#signal-filters)
8. [Risk Management](#risk-management)
9. [Logging & Formatting](#logging--formatting)
10. [Usage](#usage)

---

## Overview
This bot identifies breakout opportunities in MESM5 by analyzing orderflow imbalance via CVD rather than price. It computes dynamic trendlines, filters false signals, and manages risk through stop-loss and take-profit rules.

## Configuration
Edit the top of the `process1minCVD.ts` file to adjust:

```ts
const PAGE_MS    = 5 * 60 * 1000;  // paging window (ms)
const WINDOW_SIZE = 5;             // bars per trendline
const TOL_PCT     = 0.001;         // breakout tolerance
const R_MULTIPLE  = 2;             // reward:risk factor

// Databento API
const DATASET = 'GLBX.MDP3';
const SCHEMA  = 'trades';
const SYMBOL  = 'MESM5';
```

To change trading hours, modify the `start`/`end` ISO strings (use UTC± offsets for PDT/PST).

## Installation

```bash
# clone repo or copy file
npm install axios @types/node ts-node typescript
export DATABENTO_API_KEY=your_key_here
```

## Data Streaming
- **`streamTradesPaged`**: pages trade prints in 5-min windows from Databento.
- **`streamOneMinuteBars`**: buckets trades into 1-min bars, calculates cumulative delta.

## CVD Aggregation
- Tracks running CVD: +size for buys, -size for sells.
- Records per-bar OHLC, volume, delta, and assigns a CVD color:
  - **Weak**: green/red/gray by open vs close
  - **Strong**: requires new highs/lows vs previous bar

## Trendline Fitting
1. **Linear regression seed** on the CVD series
2. **Pivot selection** at max/min residuals
3. **Slope optimization** under support/resistance constraints
4. **Breakout detection** using a 0.1% tolerance on final values

## Signal Filters
After raw breakout detection, apply:
- **Reversal filter**: block same-direction re-entries
- **Slope filter**: require positive resistance slope for bullish, negative support slope for bearish
- **Price filter**: bar close must exceed recent high/low
- **Volume filter**: bar volume must exceed average of prior volumes

## Risk Management
On signal pass:
1. Entry at bar close
2. Stop at recent price extreme
3. Target = entry ± (risk × `R_MULTIPLE`)
4. Monitor each bar for stop-loss or take-profit, then reset

## Logging & Formatting
- Uses `toLocaleTimeString` with `timeZone: 'America/Los_Angeles'` for correct PST/PDT timestamps
- Structured console output shows each bar, filters, and trade events

## Usage

Run the script:

```bash
npx ts-node process1minCVD.ts
```

You’ll see output like:

```
📊 Streaming 1-min MESM5 bars...
#1 06:30:00 AM | O:5664.25 H:5669.00 ... CVD:-36 Color:green
#5 06:34:00 AM | ...
    → filtered: resistance slope not positive
#10 06:39:00 AM | ... CVD:-1617 Color:red
    → ENTRY SIGNAL: BEARISH
       Stop price: 5670.00
       Target price: 5652.00
```

---

Feel free to extend this bot with live order placement, additional analytics, or integration into a larger trading framework.

---

- **Error Handling & Retries**: For production, wrap HTTP calls with retry/backoff logic (e.g., using `axios-retry`) to handle transient network errors.
- **Logging**: Integrate a logging library (e.g., `pino` or `winston`) instead of `console.log` for structured logs, log levels, and file output.
- **Testing**: Add unit tests with Jest for each module (`streamTradesPaged`, `streamOneMinuteBars`, trendline functions) and mock `axios` streams.
- **Resource Management**: Monitor memory usage when running long backfills—consider paging window size (`PAGE_MS`) as a tuning parameter.
- **Build & Deployment**: Compile with `tsc` and bundle (if needed) using `esbuild` or `webpack` for a single executable, then deploy to your VPS or serverless environment.

---

*End of README*

