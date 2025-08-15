import warnings
warnings.filterwarnings("ignore")

import gridstatus
import pandas as pd
from datetime import datetime, date, timedelta
import pytz  # Import for timezone handling
from gridstatusio import GridStatusClient
import yfinance as yf
import json
import requests

from loadPipe import getErcotLoadForecast
from lmpPipeForecast import fetch_lmp_prices
from windSolarPipe import fetch_windSolar_real_and_forecast
from weatherPipe import get_weather_data 

df1, df2 = getErcotLoadForecast()
df3, df4 = fetch_lmp_prices()
df5, df6 = fetch_windSolar_real_and_forecast()
df7, df8 = get_weather_data()


def standardize_date_column(df):
    df["date"] = pd.to_datetime(df["date"], errors="coerce")  # convert to datetime, drop errors if any
    df["date"] = df["date"].dt.tz_localize(None)              # remove timezone
    df["date"] = df["date"].dt.floor("h")                     # round to hour
    return df

for df in [df1, df2, df3, df4, df5, df6, df7, df8]:
    standardize_date_column(df)

# Merge past data
df_past = df1.merge(df3, on="date", how="inner")
df_past = df_past.merge(df5, on="date", how="inner")
df_past = df_past.merge(df7, on="date", how="inner")

# Merge forecast data
df_forecast = df2.merge(df4, on="date", how="inner")
df_forecast = df_forecast.merge(df6, on="date", how="inner")
df_forecast = df_forecast.merge(df8, on="date", how="inner")

# Sort by date
df_past = df_past.sort_values("date").reset_index(drop=True)
df_forecast = df_forecast.sort_values("date").reset_index(drop=True)

print("Past DataFrame:")
print(df_past.head(24))
print(df_past.tail(24))
print("Forecast DataFrame:")
print(df_forecast.head(24))
print(df_forecast.tail(24))

def print_date_range(df, name="DataFrame"):
    # Convert to datetime if not already
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    earliest = df["date"].min()
    latest = df["date"].max()
    print(f"{name} - Earliest date: {earliest}, Latest date: {latest}")

# Example usage for your dataframes
print_date_range(df1, "df1")
print_date_range(df3, "df3")
print_date_range(df5, "df5")
print_date_range(df7, "df7")

print_date_range(df2, "df2")
print_date_range(df4, "df4")
print_date_range(df6, "df6")
print_date_range(df8, "df8")

print_date_range(df_past, "df_past")
print_date_range(df_forecast, "df_forecast")

print("df_past columns:",df_past.columns)
print("df_forecast columns:",df_forecast.columns)

# Save the dataframes to CSV files
df_past.to_csv("df_past.csv", index=False)
df_forecast.to_csv("df_forecast.csv", index=False)

print("Data saved to df_past.csv and df_forecast.csv")
