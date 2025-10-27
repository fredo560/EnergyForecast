import warnings
warnings.filterwarnings("ignore")

import gridstatus
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pytz  # Import for timezone handling
from gridstatusio import GridStatusClient
import yfinance as yf
import json
import requests
import os
import csv

# try:
#     from tensorflow.keras.models import load_model
# except Exception:
#     from keras.models import load_model
# from sklearn.preprocessing import MinMaxScaler

from loadPipe import getErcotLoadForecast
from lmpPipeForecast import fetch_lmp_prices
from windSolarPipe import fetch_windSolar_real_and_forecast
from weatherPipe import get_weather_data 

def log_message(message):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        base_dir = os.getcwd()

    log_path = os.path.join(base_dir, "pipeline_log.csv")

    # Timestamp in America/Chicago timezone
    tz = pytz.timezone("America/Chicago")
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %Z")

    # Print log entry
    print(f"[{timestamp}] {message}")

    # Write to CSV (append mode)
    file_exists = os.path.exists(log_path)
    with open(log_path, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # Add headers if the file is new
        if not file_exists:
            writer.writerow(["timestamp", "message"])
        writer.writerow([timestamp, message])

df1, df2 = getErcotLoadForecast()
df3, df4 = fetch_lmp_prices()
df5, df6 = fetch_windSolar_real_and_forecast()
df7, df8 = get_weather_data()

log_message("Data fetched successfully from all sources.")
print("Data fetched successfully from all sources.")

def standardize_date_column(df):
    houston_tz = pytz.timezone('America/Chicago')

    df["date"] = pd.to_datetime(df["date"], errors="coerce",utc=True)  # convert to datetime, drop errors if any
    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize("UTC", ambiguous="NaT", nonexistent="shift_forward")
    df["date"] = df["date"].dt.tz_convert(houston_tz).dt.floor("h")         # remove timezone                   # round to hour
    return df

for df in [df1, df2, df3, df4, df5, df6, df7, df8]:
    standardize_date_column(df)

print("Date columns standardized.")

# Merge past data
df_past = df1.merge(df3, on="date", how="inner")
df_past = df_past.merge(df5, on="date", how="inner")
df_past = df_past.merge(df7, on="date", how="inner")

# Merge forecast data
df_forecast = df2.merge(df4, on="date", how="inner")
df_forecast = df_forecast.merge(df6, on="date", how="inner")
df_forecast = df_forecast.merge(df8, on="date", how="inner")

print("Data merged successfully.")

# Sort by date
df_past = df_past.sort_values("date").reset_index(drop=True)
df_forecast = df_forecast.sort_values("date").reset_index(drop=True)

print("Data sorted by date.")

def add_time_features(df):                               
    df["year"] = df["date"].dt.year                  
    df["month"] = df["date"].dt.month               
    df["day"] = df["date"].dt.day                      
    df["dayofweek"] = df["date"].dt.dayofweek            
    df["hour"] = df["date"].dt.hour                       
    df["is_weekend"] = (df["dayofweek"] >= 5).astype(int) 
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    return df

df_past = add_time_features(df_past)                      
df_forecast = add_time_features(df_forecast) 

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

print("df_past columns:",df_past.columns.tolist())
print("df_forecast columns:",df_forecast.columns.tolist())

# --- SAVE SECTION ---

past_path = "df_past.csv"
forecast_path = "df_forecast.csv"

#  Append new past data
if os.path.exists(past_path):
    old_past = pd.read_csv(past_path)
    old_past["date"] = pd.to_datetime(old_past["date"], errors="coerce")
    combined = pd.concat([old_past, df_past], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date"]).sort_values("date")
    combined.to_csv(past_path, index=False)
    log_message(f" Appended new past data — total rows: {len(combined)} - dates changed {df_past['date'].min().date()} to {df_past['date'].max().date()} ")
else:
    df_past.to_csv(past_path, index=False)
    log_message(f" Created new {past_path} — rows: {len(df_past)}")

#  Overwrite forecast only if new data is newer
if os.path.exists(forecast_path):
    existing_forecast = pd.read_csv(forecast_path, parse_dates=["date"])

    # Determine the new forecast range
    new_start = df_forecast["date"].min()
    new_end = df_forecast["date"].max()

    # Keep only old forecast data *before* the new forecast window
    mask = existing_forecast["date"] < new_start
    combined_forecast = pd.concat(
        [existing_forecast.loc[mask], df_forecast], ignore_index=True
    )

    # Remove duplicates on 'date' (keep the newest forecast)
    combined_forecast = combined_forecast.drop_duplicates(subset=["date"], keep="last")
    combined_forecast = combined_forecast.sort_values("date").reset_index(drop=True)
    log_message(f"Existing forecast CSV found — overwrote data from {new_start.date()} to {new_end.date()}.")
else:
    combined_forecast = df_forecast.copy()
    log_message("No existing forecast CSV found — creating new file.")

combined_forecast.to_csv(forecast_path, index=False)
print(" df_forecast.csv updated.")
log_message(" df_forecast.csv saved successfully.")

# # --- LOAD MODEL AND PREDICT ---

# # Load the saved model with proper path
# model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "best_lstm_energy_model2.h5")
# if not os.path.exists(model_path):
#     raise FileNotFoundError(f"Model file not found at {model_path}. Please ensure the model has been trained and saved.")

# try:
#     model = load_model(model_path, compile=False)  # Load without compiling to avoid metric issues
#     model.compile(loss='mse', metrics=['mae'])  # Recompile with the same loss and metrics used during training
#     print(f"Model loaded successfully from {model_path}")
# except Exception as e:
#     raise Exception(f"Failed to load model: {e}")

# # Define features used during training (based on your previous script)
# features = ['temp', 'dew', 'humidity', 'precip', 'snow', 'windspeed', 'sealevelpressure', 
#             'cloudcover', 'visibility', 'solarradiation', 'solarenergy', 'lmp', 'actual_wind_mw', 
#             'actual_solar_mw', 'year', 'month', 'day', 'dayofweek', 'hour', 'is_weekend', 
#             'is_hurricane', 'hour_sin']

# df_forecast.rename(columns={"pressure":"sealevelpressure","wind_forecast_mw":"actual_wind_mw","solar_forecast_mw":"actual_solar_mw"}, inplace=True)
# missing_cols = [col for col in features if col not in df_forecast.columns]
# if missing_cols:
#     if 'is_hurricane' in missing_cols:
#         df_forecast['is_hurricane'] = 0
#         missing_cols.remove('is_hurricane')
#     if missing_cols:
#         raise ValueError(f"Missing columns in df_forecast: {missing_cols}")

# df_predict = df_forecast[features].copy()

# scaler= MinMaxScaler()
# scaler.fit(pd.read_csv("df_past.csv")[features + ['coast']])

# predict_scaled = pd.DataFrame(scaler.transform(df_predict), index=df_predict.index,columns=features)

# seq_length=24
# def create_sequences(data, seq_length=seq_length):
#     xs = []
#     for i in range(len(data) - seq_length + 1):
#         x = data.iloc[i:(i + seq_length)][features].values
#         xs.append(x)
#     return np.array(xs)

# X_predict = create_sequences(predict_scaled)

# if len(X_predict) == 0:
#     raise ValueError("Insufficient data in df_forecast to create sequences. Ensure at least 24 hours of data.")
    
# predictions_scaled = model.predict(X_predict,verbose=0).flatten()

# dummy = np.zeros((len(predictions_scaled), len(features) + 1))
# dummy[:, -1] = predictions_scaled
# predictions = scaler.inverse_transform(dummy)[:, -1]

# # Align predictions with dates (predictions correspond to the last timestep of each sequence)
# predict_dates = combined_forecast["date"].iloc[seq_length-1:].values[:len(predictions)]

# # Create a DataFrame with predictions
# df_predictions = pd.DataFrame({
#     'date': predict_dates,
#     'predicted_coast': predictions
# })

# print("Predictions for coast:")
# print(df_predictions.head())
# print(df_predictions.tail())

# predictions_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "coast_predictions.csv")
# df_predictions.to_csv(predictions_path, index=False)
# log_message(f"Coast predictions saved to {predictions_path}")



