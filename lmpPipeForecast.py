import gridstatus
import pandas as pd
from datetime import datetime, date, timedelta
import pytz  # Import for timezone handling


iso = gridstatus.Ercot()
houston_tz = pytz.timezone('America/Chicago')

def fetch_lmp_prices():
    # Get current time in Central Time (ERCOT's timezone)
    #central = pytz.timezone('US/Central')
    #now = pd.Timestamp.now(tz=central).floor('h')
    now = datetime.now(houston_tz)
    now = now.replace(minute=0, second=0, microsecond=0)  # Round down to the hour
    # --- REAL-TIME DATA ---
    df = iso.get_spp(date=now, market="REAL_TIME_15_MIN", location_type="Trading Hub")
    df = df[df["Location"] == "HB_HOUSTON"]
    df["Time"] = pd.to_datetime(df["Time"],utc=True).dt.tz_convert(houston_tz).dt.floor('h') # Convert to Houston local time
    df["hour"] = df["Time"]  # Remove timezone for comparison
    
    df = (
        df.groupby("hour")["SPP"]
        .mean()
        .reset_index()
        .rename(columns={"hour": "date", "SPP": "LMP"})
    )
    df["location"] = "HB_HOUSTON"
    df = df[["date", "location", "LMP"]]
    
    # --- DAY-AHEAD TODAY ---
    df2 = iso.get_spp(date=now, market="DAY_AHEAD_HOURLY", location_type="Trading Hub")
    df2 = df2[df2["Location"] == "HB_HOUSTON"]
    df2["Time"] = pd.to_datetime(df2["Time"],utc=True).dt.tz_convert(houston_tz).dt.floor('h')   # Convert to Houston local time
    #df2["Time"] = df2["Time"].dt.tz_localize(None)  # Remove timezone
    df2 = df2.rename(columns={"Time": "date", "Location": "location", "SPP": "LMP"})
    df2 = df2[["date", "location", "LMP"]]
    
    # --- FORECAST ---
    df3 = iso.get_spp(date="latest", market="DAY_AHEAD_HOURLY", location_type="Trading Hub")
    df3 = df3[df3["Location"] == "HB_HOUSTON"]
    df3["Time"] = pd.to_datetime(df3["Time"],utc=True).dt.tz_convert(houston_tz).dt.floor('h')   # Convert to Houston local time
    #df3["Time"] = df3["Time"].dt.tz_localize(None)  # Remove timezone
    df3 = df3.rename(columns={"Time": "date", "Location": "location", "SPP": "LMP"})
    df3 = df3[["date", "location", "LMP"]]
    
    # --- COMBINE AND SPLIT ---
    df_combined = pd.concat([df, df2, df3], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=["date"], keep="first")
    df_combined = df_combined.sort_values(by="date").reset_index(drop=True)
    
    # Ensure now is timezone-naive for comparison
    #now_naive = now.tz_localize(None)
    
    # Split data
    df1 = df_combined[df_combined["date"] <= now].copy()
    df2 = df_combined[df_combined["date"] > now].copy()
    
    # Format dates as strings
    #df1["date"] = df1["date"].dt.strftime('%Y-%m-%d %H:%M:%S')
    #df2["date"] = df2["date"].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df1, df2

# --- RUN ---
df1, df2 = fetch_lmp_prices()
print("Past/Current LMP Prices:")
print(df1.tail(48))
print("\nFuture LMP Prices:")
print(df2.head(24))
print("tail \n")
print(df2.tail(24))
