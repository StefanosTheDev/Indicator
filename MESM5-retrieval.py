import databento as db
import pandas as pd
from datetime import datetime
import pytz

# ── Configuration ─────────────────────────────────────
API_KEY = "db-SsEgaXSBfWPD4exmHcjp4yDrPRF6v"  # Replace with your key
SYMBOL = "MESM5"
SCHEMA = "trades"
DATASET = "GLBX.MDP3"
START_DATE = "2025-05-07T00:00:00"
END_DATE = "2025-05-07T23:59:59"
TICK_SIZE = 1000
OUTPUT_CSV = "mes_1000tick_eastern.csv"

# ── Initialize Client ─────────────────────────────────
client = db.Historical(API_KEY)

# ── Request Tick Data ─────────────────────────────────
print("📥 Fetching tick data...")
data = client.timeseries.get_range(
    dataset=DATASET,
    schema=SCHEMA,
    symbols=SYMBOL,
    start=START_DATE,
    end=END_DATE,
)

df = data.to_df()
df = df.sort_values(by="ts_event")  # Ensure correct ordering

print(f"✅ Downloaded {len(df)} ticks")

# ── Build 1000-Tick Bars ──────────────────────────────
bars = []
eastern = pytz.timezone("US/Eastern")

for i in range(0, len(df), TICK_SIZE):
    chunk = df.iloc[i:i+TICK_SIZE]
    if len(chunk) < TICK_SIZE:
        break  # Skip incomplete bar

    open_price = chunk.iloc[0]["price"]
    high_price = chunk["price"].max()
    low_price = chunk["price"].min()
    close_price = chunk.iloc[-1]["price"]
    volume = chunk["size"].sum()

    cd = chunk["size"].where(chunk["side"] == "B", -chunk["size"]).sum()
    cdv = chunk["size"].where(chunk["side"] == "A", -chunk["size"]).cumsum().iloc[-1]
    vwap = (chunk["price"] * chunk["size"]).sum() / volume

    # Convert timestamp to 12-hour Eastern time
    utc_dt = pd.to_datetime(chunk.iloc[-1]["ts_event"], utc=True)
    est_str = utc_dt.tz_convert(eastern).strftime("%b %d, %Y %I:%M:%S %p")

    bars.append({
        "timestamp_eastern": est_str,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
        "cd": cd,
        "cdv": cdv,
        "vwap": vwap
    })

bars_df = pd.DataFrame(bars)

# ── Save to CSV ───────────────────────────────────────
bars_df.to_csv(OUTPUT_CSV, index=False)
print(f"📄 Saved to {OUTPUT_CSV}")
