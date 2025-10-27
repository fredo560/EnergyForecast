import requests
import pandas as pd
import json 
from datetime import datetime, timedelta
import pytz
import time

print("Fetching weather data...")

def get_weather_data():
    # --- API URLs ---
    forecast_url = (
        "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        "houston/next7days?unitGroup=us&elements=datetime%2CdatetimeEpoch%2Ctemp%2Cdew%2C"
        "humidity%2Cprecip%2Csnow%2Cwindspeed%2Cpressure%2Ccloudcover%2Cvisibility%2C"
        "solarradiation%2Csolarenergy&key=B8HMZFWVSYTM4U5Q7AVQ7UL83&contentType=json"
    )

    yesterday_url = (
        "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        "houston/yesterday?unitGroup=us&elements=datetime%2Ctemp%2Cdew%2Chumidity%2Cprecip%2C"
        "snow%2Cwindspeed%2Cpressure%2Ccloudcover%2Cvisibility%2Csolarradiation%2Csolarenergy"
        "&include=hours&key=B8HMZFWVSYTM4U5Q7AVQ7UL83&contentType=json"
    )

    # --- Houston timezone ---
    houston_tz = pytz.timezone("America/Chicago")

    # --- Fetch yesterday ---
    response_yesterday = requests.get(yesterday_url)
    data_yesterday = response_yesterday.json()

    hourly_yesterday = []
    for day in data_yesterday.get("days", []):
        for hour in day.get("hours", []):
            hour["date"] = day["datetime"]
            hourly_yesterday.append(hour)

    df_yesterday = pd.DataFrame(hourly_yesterday)

    time.sleep(2)  # avoid hitting rate limits

    # --- Fetch forecast ---
    response = requests.get(forecast_url)
    data = response.json()

    hourly_data = []
    for day in data.get("days", []):
        for hour in day.get("hours", []):
            hour["date"] = day["datetime"]
            hourly_data.append(hour)

    df_forecast = pd.DataFrame(hourly_data)

    # --- Combine yesterday + forecast ---
    df = pd.concat([df_yesterday, df_forecast], ignore_index=True)

    # --- Build combined timestamp safely ---
    df["naive_dt"] = pd.to_datetime(df["date"] + " " + df["datetime"], errors="coerce")

    # Localize safely: use `ambiguous='NaT'` to handle DST transitions
    df["full_datetime"] = (
        df["naive_dt"]
        .dt.tz_localize(houston_tz, ambiguous="NaT", nonexistent="shift_forward")
    )

    # --- Split into past and forecast ---
    now = datetime.now(houston_tz)

    df_past = df[df["full_datetime"] <= now].copy()
    df_forecast = df[df["full_datetime"] > now].copy()

    # --- Clean up columns ---
    drop_cols = ["datetime", "datetimeEpoch", "date", "naive_dt"]
    df_past.drop(columns=drop_cols, inplace=True, errors="ignore")
    df_forecast.drop(columns=drop_cols, inplace=True, errors="ignore")

    df_past.rename(columns={"full_datetime": "date"}, inplace=True)
    df_forecast.rename(columns={"full_datetime": "date"}, inplace=True)

    return df_past, df_forecast


# --- Run ---
df1, df2 = get_weather_data()

print("\nPast Data (last 48 hours):")
print(df1.tail(48))
print("\nForecast Data (next 48 hours):")
print(df2.head(48))
print("\nColumns:", df1.columns)
print("Finished weather data fetch")
