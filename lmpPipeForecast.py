import gridstatus
import pandas as pd
from datetime import datetime, timedelta
import pytz

iso = gridstatus.Ercot()
houston_tz = pytz.FixedOffset(-300)

def fetch_lmp_prices(days_back=1):
    now = datetime.now(houston_tz)
    now = now.replace(minute=0, second=0, microsecond=0)  # Round down to the hour
    
    past_data = []

    # --- Fetch REAL-TIME data for previous days ---
    for i in range(days_back, -1, -1):  # e.g., 7 days ago up to today
        date = (now - timedelta(days=i)).date()
        try:
            df_day = iso.get_spp(
                date=date,
                market="REAL_TIME_15_MIN",
                location_type="Trading Hub"
            )
            df_day = df_day[df_day["Location"] == "HB_HOUSTON"]
            df_day["Time"] = pd.to_datetime(df_day["Time"], errors="coerce", utc=True).dt.tz_convert(houston_tz).dt.floor('h')
            df_day = (
                df_day.groupby("Time")["SPP"]
                .mean()
                .reset_index()
                .rename(columns={"Time": "date", "SPP": "lmp"})
            )
            df_day["location"] = "HB_HOUSTON"
            past_data.append(df_day)
        except Exception as e:
            print(f"Failed to fetch {date}: {e}")

    df_past = pd.concat(past_data, ignore_index=True)

    # --- DAY-AHEAD TODAY ---
    df2 = iso.get_spp(date=now, market="DAY_AHEAD_HOURLY", location_type="Trading Hub")
    df2 = df2[df2["Location"] == "HB_HOUSTON"]
    df2["Time"] = pd.to_datetime(df2["Time"], errors="coerce", utc=True).dt.tz_convert(houston_tz).dt.floor('h')
    df2 = df2.rename(columns={"Time": "date", "Location": "location", "SPP": "lmp"})
    df2 = df2[["date", "location", "lmp"]]

    # --- FORECAST ---
    df3 = iso.get_spp(date="latest", market="DAY_AHEAD_HOURLY", location_type="Trading Hub")
    df3 = df3[df3["Location"] == "HB_HOUSTON"]
    df3["Time"] = pd.to_datetime(df3["Time"],errors="coerce", utc=True).dt.tz_convert(houston_tz).dt.floor('h')
    df3 = df3.rename(columns={"Time": "date", "Location": "location", "SPP": "lmp"})
    df3 = df3[["date", "location", "lmp"]]

    # --- COMBINE ---
    df_combined = pd.concat([df_past, df2, df3], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=["date"], keep="first")
    df_combined = df_combined.sort_values(by="date").reset_index(drop=True)

    # Split
    df1 = df_combined[df_combined["date"] <= now].copy()
    df2 = df_combined[df_combined["date"] > now].copy()

    return df1, df2

# --- RUN ---
df1, df2 = fetch_lmp_prices(days_back=1)
print("Past/Current LMP Prices (last 2 days + today):")
print(df1.head(24))
print(df1.tail(24))
print("\nFuture LMP Prices:")
print(df2.head(24))
print(df2.tail(24))
print("Data fetched successfully.")




