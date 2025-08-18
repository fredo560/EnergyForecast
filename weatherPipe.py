import requests
import pandas as pd
import json 
from datetime import datetime, date, timedelta
import pytz

print("Fetching weather data...")

# this one is done, gets todays weather and the next 7 days forecast in seperate DFs

# https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/houston/next7days?unitGroup=us&elements=datetime%2CdatetimeEpoch%2Ctemp%2Cdew%2Chumidity%2Cprecip%2Csnow%2Cwindspeedmean%2Cpressure%2Ccloudcover%2Cvisibility%2Csolarradiation&include=hours&key=B8HMZFWVSYTM4U5Q7AVQ7UL83&contentType=json

# next 7 days
# https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/houston/next7days?unitGroup=us&elements=datetime%2CdatetimeEpoch%2Ctemp%2Cdew%2Chumidity%2Cprecip%2Csnow%2Cwindspeed%2Cpressure%2Ccloudcover%2Cvisibility%2Csolarradiation%2Csolarenergy&key=B8HMZFWVSYTM4U5Q7AVQ7UL83&contentType=json

def get_weather_data():
    url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/houston/next7days?unitGroup=us&elements=datetime%2CdatetimeEpoch%2Ctemp%2Cdew%2Chumidity%2Cprecip%2Csnow%2Cwindspeed%2Cpressure%2Ccloudcover%2Cvisibility%2Csolarradiation%2Csolarenergy&key=B8HMZFWVSYTM4U5Q7AVQ7UL83&contentType=json"

    response = requests.get(url)
    data = response.json()

    hourly_data = []
    for day in data.get("days", []):
        for hour in day.get("hours", []):
            # Add date info to each hourly record
            hour["date"] = day["datetime"]
            hourly_data.append(hour)

    df = pd.DataFrame(hourly_data)

    houston_tz = pytz.timezone('America/Chicago')

    df["full_datetime"] = pd.to_datetime(df["date"] + " " + df["datetime"]).dt.tz_localize(houston_tz)
    # Get current time (in local time zone)
    now = datetime.now(houston_tz)

    # Split into past/current and forecast
    df_past = df[df["full_datetime"] <= now].copy()
    df_forecast = df[df["full_datetime"] > now].copy()

     # Drop unnecessary columns and rename full_datetime to date
    columns_to_drop = ["datetime", "datetimeEpoch", "date"]
    df_past.drop(columns=columns_to_drop, inplace=True, errors="ignore")
    df_forecast.drop(columns=columns_to_drop, inplace=True, errors="ignore")

    df_past.rename(columns={"full_datetime": "date"}, inplace=True)
    df_forecast.rename(columns={"full_datetime": "date"}, inplace=True)

    return df_past, df_forecast

df1,df2 = get_weather_data()

print(df1.head(48))
print(df2.tail(48))

print("Finished weather data fetch")
