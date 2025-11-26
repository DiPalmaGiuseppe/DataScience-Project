import pandas as pd
import requests

INPUT_FILE = "dataset/data_v2.csv"
OUTPUT_FILE = "dataset/data_v2_full.csv"

LAT = 52.408521
LON = 12.562565
COUNTRY = "DE"
REGION = None

df = pd.read_csv(INPUT_FILE, sep=";")
df["date"] = pd.to_datetime(df["date"], utc=True)
df["day_of_year"] = df["date"].dt.dayofyear
df["day_of_week"] = df["date"].dt.dayofweek

years = df["date"].dt.year.unique()
holidays = set()
for year in years:
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{COUNTRY}"
    if REGION:
        url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{COUNTRY}/{REGION}"
    response = requests.get(url)
    response.raise_for_status()
    for h in response.json():
        holidays.add(h["date"])
df["holiday"] = df["date"].dt.date.astype(str).isin(holidays).astype(int)

start_date = df["date"].dt.date.min().isoformat()
end_date = df["date"].dt.date.max().isoformat()

daily_params = [
    "weathercode",
    "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
    "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean",
    "sunrise", "sunset", "daylight_duration", "sunshine_duration",
    "rain_sum", "snowfall_sum", "precipitation_hours",
    "windspeed_10m_max", "windgusts_10m_max", "winddirection_10m_dominant",
    "shortwave_radiation_sum", "et0_fao_evapotranspiration"
]

url = (
    f"https://archive-api.open-meteo.com/v1/archive?"
    f"latitude={LAT}&longitude={LON}"
    f"&start_date={start_date}&end_date={end_date}"
    f"&daily={','.join(daily_params)}&timezone=UTC"
)

response = requests.get(url)
response.raise_for_status()
data = response.json()

daily_data = data["daily"]
cols = [c for c in daily_data.keys() if c != "time"]

daily_df = pd.DataFrame({
    "date": pd.to_datetime(daily_data["time"], utc=True),
    **{col: daily_data[col] for col in cols}
})

daily_df = daily_df.loc[:, daily_df.notna().any()]

df["date_only"] = df["date"].dt.date
daily_df["date_only"] = daily_df["date"].dt.date

for col in ["sunrise", "sunset"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], utc=True).dt.hour * 3600 \
                  + pd.to_datetime(df[col], utc=True).dt.minute * 60 \
                  + pd.to_datetime(df[col], utc=True).dt.second


df = df.merge(daily_df.drop(columns=["date"]), on="date_only", how="left")

df.drop(columns=["date_only"], inplace=True)

for col in ["sunrise", "sunset"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], utc=True).dt.hour * 3600 \
                  + pd.to_datetime(df[col], utc=True).dt.minute * 60 \
                  + pd.to_datetime(df[col], utc=True).dt.second

df.to_csv(OUTPUT_FILE, sep=";", index=False)
print("Dataset completo generato:", OUTPUT_FILE)