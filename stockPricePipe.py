import datetime
import yfinance as yf
import pandas as pd

# no spy forecast but can get data from up to 30 days ago, ffill is weird with beginning hours but ffill covers those borderline cases


start_date = datetime.datetime(2025, 6, 12)
end_date = datetime.datetime(2025, 6, 20)

spy = yf.Ticker("SPY")
data = spy.history(start=start_date, end=end_date,interval="1h")

data.reset_index(inplace=True)
data.rename(columns={"Datetime":"date","Close":"SpyPrice"}, inplace=True)

df = data[["date","SpyPrice"]]

df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.floor("H")

full_hours = pd.date_range(start=start_date, end=end_date, freq="H")

df = df.set_index("date").reindex(full_hours).rename_axis("date").reset_index()
df["SpyPrice"] = df["SpyPrice"].ffill().bfill()

df["date"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

print(df.isnull().sum())
print(df.head(24))