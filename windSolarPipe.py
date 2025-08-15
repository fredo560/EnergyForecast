from gridstatusio import GridStatusClient
import pandas as pd
from datetime import date, timedelta

# this one works and gets the day prior and up to the recent hour of today
# and the next 7 days forecast


def fetch_wind_solar():
    start_date = str(date.today() - timedelta(days=1))
    end_date = str(date.today() + timedelta(days=1))


    ''' Fetches SPP wind and solar forecast data for the given date range.
    '''
    # Recommended: set GRIDSTATUS_API_KEY as an
    # environment variable instead of hardcoding
    client = GridStatusClient("8d79158749f44daba78482b30626f617")
    # Fetch data as pandas DataFrame
    df = client.get_dataset(
        dataset="spp_solar_and_wind_forecast_short_term",
        start=start_date,
        end=end_date,
        publish_time="latest",
        timezone="market",
    )

    df['datetime'] = pd.to_datetime(df['interval_start_local'])
    df["date"] = df["datetime"].dt.floor("h")

    # Group by hour and calculate averages
    df_hourly = (
        df.groupby("date")[["actual_wind_mw", "actual_solar_mw"]]
        .mean()
        .reset_index()
    )

    # Format the date nicely
    df_hourly["date"] = df_hourly["date"].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_hourly = df_hourly.dropna()

    #df = df[['date', 'actual_wind_mw', 'actual_solar_mw']]

    return df_hourly


def fetch_wind_solar_forecast(days_ahead=8):
    ''' Fetches SPP wind and solar forecast data for the next 8 days.
    '''
    client = GridStatusClient("8d79158749f44daba78482b30626f617")
    endEndDate = date.today() + timedelta(days=days_ahead)

    df = client.get_dataset(
        dataset="spp_solar_and_wind_forecast_mid_term",
        start=date.today(),
        end=endEndDate,
        publish_time="latest",
        timezone="market",
        )

    df['date'] = pd.to_datetime(df['interval_start_local']).dt.floor("h").dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df[['date', 'wind_forecast_mw', 'solar_forecast_mw']]
    df = df.dropna()
    return df

def fetch_windSolar_real_and_forecast():
    ''' Fetches both real and forecasted wind and solar data.
    '''
    df1 = fetch_wind_solar()
    df2 = fetch_wind_solar_forecast()


    return df1,df2


df1, df2 = fetch_windSolar_real_and_forecast()
print("Priting real data: \n")
print(df1.tail(48))

print("Printing forecast data: \n")
print(df2.head(24))
print(df2.tail(24))