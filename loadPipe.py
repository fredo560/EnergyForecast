from gridstatusio import GridStatusClient
from datetime import datetime, date, timedelta
import pandas as pd


# this one is done and gets 6 days advanced as well

def getErcotLoadForecast():
    ''' Fetches ERCOT load forecast data for a week starting from the given date.
    '''

    # Use today's date
    start_date = date.today() - timedelta(days=1)
    end_date = start_date + timedelta(days=6)

    # Initialize the client
    client = GridStatusClient("8d79158749f44daba78482b30626f617")

    # Fetch data as pandas DataFrame
    df = client.get_dataset(
        dataset="ercot_load_forecast_by_weather_zone",
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        publish_time="latest",
        timezone="market",
    )

    df = df[["interval_start_utc", "interval_end_utc", "coast", "system_total"]]
    df["date"] = pd.to_datetime(df["interval_start_utc"]).dt.tz_localize(None)
    df = df.drop(columns=["interval_start_utc", "interval_end_utc"])
    df = df.dropna()

    # Get current datetime
    now = datetime.now()

    # Split the data
    df1 = df[df["date"] <= now].copy()
    df2 = df[df["date"] > now].copy()

    df1["date"] = df1["date"].dt.strftime('%Y-%m-%d %H:%M:%S')
    df2["date"] = df2["date"].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df1, df2



df1, df2 = getErcotLoadForecast()

# Output
print("Printing df1 (past/current load data):")
print(df1.tail(48))
print("\nPrinting df2 (future load forecast data):")
print(df2.tail(48))