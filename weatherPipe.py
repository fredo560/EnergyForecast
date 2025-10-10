import requests
import pandas as pd
import json 
from datetime import datetime, date, timedelta
import pytz
import time

print("Fetching weather data...")

def get_weather_data():
    # --- Existing forecast URL  ---
    url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/houston/next7days?unitGroup=us&elements=datetime%2CdatetimeEpoch%2Ctemp%2Cdew%2Chumidity%2Cprecip%2Csnow%2Cwindspeed%2Cpressure%2Ccloudcover%2Cvisibility%2Csolarradiation%2Csolarenergy&key=B8HMZFWVSYTM4U5Q7AVQ7UL83&contentType=json"

    # --- Added yesterday's data URL ---  
    url_yesterday = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/houston/yesterday?unitGroup=us&elements=datetime%2Ctemp%2Cdew%2Chumidity%2Cprecip%2Csnow%2Cwindspeed%2Cpressure%2Ccloudcover%2Cvisibility%2Csolarradiation%2Csolarenergy&include=hours&key=B8HMZFWVSYTM4U5Q7AVQ7UL83&contentType=json"  # <<< ADDED

    # --- Fetch yesterday's data ---  
    response_yesterday = requests.get(url_yesterday)  
    data_yesterday = response_yesterday.json()        

    hourly_yesterday = []                             
    for day in data_yesterday.get("days", []):        
        for hour in day.get("hours", []):             
            hour["date"] = day["datetime"]            
            hourly_yesterday.append(hour)             

    df_yesterday = pd.DataFrame(hourly_yesterday)     

    time.sleep(2) # To avoid hitting API rate limits

    # --- Fetch forecast data  ---
    response = requests.get(url)
    data = response.json()

    hourly_data = []
    for day in data.get("days", []):
        for hour in day.get("hours", []):
            hour["date"] = day["datetime"]
            hourly_data.append(hour)

    df = pd.DataFrame(hourly_data)

    # --- Combine yesterday + forecast data ---  
    df = pd.concat([df_yesterday, df], ignore_index=True)  

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

# --- Run  ---
df1, df2 = get_weather_data()

print(df1.head(48))
print(df2.tail(48))
print(df1.columns)

print("Finished weather data fetch")
