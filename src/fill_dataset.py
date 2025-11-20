import pandas as pd
import requests

INPUT_FILE = "dataset/data_science_project.csv"
OUTPUT_FILE = "dataset/data_science_project_full.csv"

LAT = 52.408521
LON = 12.562565
COUNTRY = "DE"
REGION = None

df = pd.read_csv(INPUT_FILE, sep=";")
df["date"] = pd.to_datetime(df["date"], utc=True)
df["day_of_year"] = df["date"].dt.dayofyear
df["day_of_week"] = df["date"].dt.dayofweek

# Holidays
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

# Open-Meteo hourly parameters (aggiunto soil_temperature_0cm)
params = [
    "temperature_2m",
    "apparent_temperature",
    "rain",
    "windspeed_10m",
    "windspeed_80m",
    "windspeed_120m",
    "temperature_80m",
    "soil_temperature_0cm"
]

start_date = df["date"].dt.date.min().isoformat()
end_date = df["date"].dt.date.max().isoformat()

url = (
    f"https://archive-api.open-meteo.com/v1/archive?"
    f"latitude={LAT}&longitude={LON}"
    f"&start_date={start_date}&end_date={end_date}"
    f"&hourly={','.join(params)}&timezone=UTC"
)

response = requests.get(url)
response.raise_for_status()
data = response.json()

# Convert hourly to daily average
hourly_df = pd.DataFrame(data["hourly"])
hourly_df["time"] = pd.to_datetime(hourly_df["time"], utc=True)
hourly_df["date_only"] = hourly_df["time"].dt.date

daily_df = hourly_df.groupby("date_only").mean().reset_index()

# Merge with original dataset
df["date_only"] = df["date"].dt.date
df = df.merge(daily_df, left_on="date_only", right_on="date_only", how="left")
df.drop(columns=["date_only"], inplace=True)

df.to_csv(OUTPUT_FILE, sep=";", index=False)
print("Dataset completo generato:", OUTPUT_FILE)
