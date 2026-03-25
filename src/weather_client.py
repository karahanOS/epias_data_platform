import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
import numpy as np
from datetime import datetime

# ── 4 ŞEHİR KONFİGÜRASYONU ───────────────────────────────────────────────────
# Her şehir farklı bir üretim/tüketim bölgesini temsil eder

CITIES = {
    "istanbul": {
        "latitude": 41.01,
        "longitude": 28.97,
        "weight_consumption": 0.40,  # Tüketim ağırlığı — TR nüfusunun %20'si
        "weight_wind": 0.10,          # Rüzgar üretimindeki payı düşük
        "weight_solar": 0.10,         # Güneş üretimindeki payı düşük
    },
    "izmir": {
        "latitude": 38.42,
        "longitude": 27.14,
        "weight_consumption": 0.20,
        "weight_wind": 0.45,          # Ege — Türkiye rüzgar üretiminin merkezi
        "weight_solar": 0.25,
    },
    "konya": {
        "latitude": 37.87,
        "longitude": 32.49,
        "weight_consumption": 0.15,
        "weight_wind": 0.25,          # İç Anadolu rüzgarı
        "weight_solar": 0.40,         # Güneydoğu'ya yakın — yüksek radyasyon
    },
    "ankara": {
        "latitude": 39.93,
        "longitude": 32.86,
        "weight_consumption": 0.25,
        "weight_wind": 0.20,
        "weight_solar": 0.25,
    },
}

HOURLY_VARIABLES = [
    "temperature_2m",
    "wind_speed_10m",
    "shortwave_radiation",
    "relative_humidity_2m",
]


class WeatherClient:
    def __init__(self):
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.client = openmeteo_requests.Client(session=retry_session)

    def get_weather_for_city(self, city_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Tek bir şehir için saatlik hava durumu verisini çeker.
        start_date / end_date: 'YYYY-MM-DD' formatında
        """
        city = CITIES[city_name]

        response = self.client.weather_api(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "hourly": HOURLY_VARIABLES,
                "timezone": "Europe/Istanbul",
                "start_date": start_date,
                "end_date": end_date,
            }
        )[0]

        hourly = response.Hourly()

        # Zaman serisi oluştur
        times = pd.date_range(
            start=pd.Timestamp(hourly.Time(), unit="s", tz="Europe/Istanbul"),
            end=pd.Timestamp(hourly.TimeEnd(), unit="s", tz="Europe/Istanbul"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )

        df = pd.DataFrame({
            "datetime": times,
            "city": city_name,
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "wind_speed_10m": hourly.Variables(1).ValuesAsNumpy(),
            "shortwave_radiation": hourly.Variables(2).ValuesAsNumpy(),
            "relative_humidity_2m": hourly.Variables(3).ValuesAsNumpy(),
        })

        return df

    def get_weighted_weather(self, start_date: str, end_date: str) -> list:
        """
        4 şehir için hava durumu çeker ve ağırlıklı Türkiye ortalamasını hesaplar.
        Airflow XCom ile uyumlu list of dict döner.
        """
        all_dfs = []

        for city_name in CITIES:
            df = self.get_weather_for_city(city_name, start_date, end_date)
            all_dfs.append(df)

        combined = pd.concat(all_dfs, ignore_index=True)

        # Ağırlıklı ortalama hesapla
        records = []
        for dt, group in combined.groupby("datetime"):
            row = {"datetime": dt.isoformat()}

            for var, weight_key in [
                ("temperature_2m", "weight_consumption"),
                ("wind_speed_10m", "weight_wind"),
                ("shortwave_radiation", "weight_solar"),
                ("relative_humidity_2m", "weight_consumption"),
            ]:
                weights = [CITIES[c][weight_key] for c in group["city"]]
                values = group[var].values
                weighted_avg = np.average(values, weights=weights)
                row[f"weighted_{var}"] = round(float(weighted_avg), 4)

            # Şehir bazında ham değerleri de sakla
            for city_name in CITIES:
                city_row = group[group["city"] == city_name]
                if not city_row.empty:
                    row[f"{city_name}_temp"] = round(float(city_row["temperature_2m"].values[0]), 2)
                    row[f"{city_name}_wind"] = round(float(city_row["wind_speed_10m"].values[0]), 2)
                    row[f"{city_name}_radiation"] = round(float(city_row["shortwave_radiation"].values[0]), 2)

            records.append(row)

        return records


if __name__ == "__main__":
    client = WeatherClient()
    data = client.get_weighted_weather("2024-01-01", "2024-01-02")
    print(f"Kayıt sayısı: {len(data)}")
    print("Örnek kayıt:")
    import json
    print(json.dumps(data[0], indent=2, default=str))