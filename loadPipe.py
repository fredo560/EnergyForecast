from gridstatusio import GridStatusClient
from datetime import datetime, date, timedelta
import pandas as pd
import time
import pytz  # Import for timezone handling

# meant for version 0.5.6 now so it will work with gridstatus library
# removed timezone which was bottlenecking the code
# this one is done and gets 6 days advanced as well

def getErcotLoadForecast():
    ''' Fetches ERCOT load forecast data for a week starting from the given date.
    '''

    # Use today's date
    houston_tz = pytz.timezone('America/Chicago')
    now = datetime.now(houston_tz)
    today = now.date()
    start_date = today - timedelta(days=1)
    end_date = start_date + timedelta(days=6)

    # Initialize the client
    time.sleep(1)
    client = GridStatusClient("8d79158749f44daba78482b30626f617")

    # Fetch data as pandas DataFrame
    df = client.get_dataset(
        dataset="ercot_load_forecast_by_weather_zone",
        start=start_date,#.strftime("%Y-%m-%d"),
        end=end_date,#.strftime("%Y-%m-%d"),
        publish_time="latest",
        #timezone="market",
    )

    df = df[["interval_start_utc", "interval_end_utc", "coast", "system_total"]]
    #df["date"] = pd.to_datetime(df["interval_start_utc"]).dt.tz_localize(None)
    df["date"] = (
        pd.to_datetime(df["interval_start_utc"], utc=True)
        .dt.tz_convert("America/Chicago")   # Houston local time
        #.dt.tz_localize(None)               # remove tzinfo for cleaner output
    )
    df = df.drop(columns=["interval_start_utc", "interval_end_utc"])
    df = df.dropna()

    # Get current datetime
    #now = datetime.now()

    # Split the data
    df1 = df[df["date"] <= now].copy()
    df2 = df[df["date"] > now].copy()

    #df1["date"] = df1["date"].dt.strftime('%Y-%m-%d %H:%M:%S')
    #df2["date"] = df2["date"].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df1, df2



df1, df2 = getErcotLoadForecast()

# Output
print("Printing df1 (past/current load data):")
print(df1.tail(48))
print("\nPrinting df2 (future load forecast data):")
print(df2.head(24))
print(df2.tail(24))
