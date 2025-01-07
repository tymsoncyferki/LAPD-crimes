import os
import time
import pandas as pd
import numpy as np
from datetime import datetime
import calendar
import requests_cache
from retry_requests import retry
import openmeteo_requests

# Konfiguracja
AREAS_CSV = 'areas.csv'
OUTPUT_DIR = 'weather_data'
CACHE_DIR = '.cache'
START_YEAR = 2024
END_YEAR = 2024
OPENMETEO_HOURLY_VARIABLES = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "rain", "snowfall", "windspeed_10m", "winddirection_10m"
]

# Upewnij się, że katalogi wyjściowe istnieją
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Konfiguracja sesji z cache i retry
cache_session = requests_cache.CachedSession(CACHE_DIR, expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo_client = openmeteo_requests.Client(session=retry_session)

def fetch_open_meteo_data(lat, lon, start_date, end_date):
    """Pobiera dane z Open-Meteo Historical Weather API dla określonego zakresu dat."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "hourly": ",".join(OPENMETEO_HOURLY_VARIABLES),
        "timezone": "auto"
    }
    try:
        responses = openmeteo_client.weather_api(url, params=params)
        if not responses:
            print(f'Brak odpowiedzi dla ({lat}, {lon}) od {start_date} do {end_date}')
            return None
        return responses[0]  # Zakładamy, że mamy tylko jedną lokalizację
    except TypeError as te:
        print(f'TypeError podczas pobierania danych dla ({lat}, {lon}): {te}')
        return None
    except Exception as e:
        print(f'Błąd podczas pobierania danych dla ({lat}, {lon}): {e}')
        return None

def process_data(area, year, month, response):
    """Przetwarza dane z Open-Meteo i zwraca DataFrame z dodaną kolumną 'area'."""
    if response is None:
        print(f'Brak danych do zapisania dla {area} {year}-{month:02d}.')
        return None

    try:
        print(f'Przetwarzanie danych dla {area} {year}-{month:02d}...')

        # Metody z openmeteo_requests
        latitude = response.Latitude()
        longitude = response.Longitude()
        elevation = response.Elevation()
        timezone = response.Timezone()
        timezone_abbreviation = response.TimezoneAbbreviation()
        utc_offset_seconds = response.UtcOffsetSeconds()

        # Hourly data
        hourly = response.Hourly()

        # Generowanie zakresu czasowego
        start_timestamp = hourly.Time()
        end_timestamp = hourly.TimeEnd()
        interval_seconds = hourly.Interval()

        # Dla nowszych wersji pandas używamy 'inclusive', starsze mogą wymagać 'closed'
        try:
            times = pd.date_range(
                start=pd.to_datetime(start_timestamp, unit="s", utc=True),
                end=pd.to_datetime(end_timestamp, unit="s", utc=True),
                freq=pd.Timedelta(seconds=interval_seconds),
                inclusive='left'  # Dla pandas >=1.4.0
            )
        except TypeError:
            # Jeśli 'inclusive' nie jest obsługiwany, użyj 'closed'
            times = pd.date_range(
                start=pd.to_datetime(start_timestamp, unit="s", utc=True),
                end=pd.to_datetime(end_timestamp, unit="s", utc=True),
                freq=pd.Timedelta(seconds=interval_seconds),
                closed='left'  # Dla starszych wersji pandas
            )

        # Odczytywanie zmiennych w kolejności
        temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
        precipitation = hourly.Variables(2).ValuesAsNumpy()
        rain = hourly.Variables(3).ValuesAsNumpy()
        snowfall = hourly.Variables(4).ValuesAsNumpy()
        windspeed_10m = hourly.Variables(5).ValuesAsNumpy()
        winddirection_10m = hourly.Variables(6).ValuesAsNumpy()

        # Tworzenie DataFrame
        df = pd.DataFrame({
            "time": times,
            "temperature_2m": temperature_2m,
            "relative_humidity_2m": relative_humidity_2m,
            "precipitation": precipitation,
            "rain": rain,
            "snowfall": snowfall,
            "windspeed_10m": windspeed_10m,
            "winddirection_10m": winddirection_10m
        })

        # Dodanie kolumny 'area'
        df['area'] = area

        return df

    except Exception as e:
        print(f'Błąd podczas przetwarzania danych dla {area} {year}-{month:02d}: {e}')
        return None

def daterange_month(start_year, end_year):
    """Generator zwracający kolejne miesiące w zadanym zakresie lat."""
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            yield year, month

def get_end_date(year, month):
    """Zwraca ostatni dzień miesiąca."""
    last_day = calendar.monthrange(year, month)[1]
    return datetime(year, month, last_day)

def main():
    # Wczytaj plik CSV
    try:
        areas = pd.read_csv(AREAS_CSV)
    except Exception as e:
        print(f'Błąd podczas czytania {AREAS_CSV}: {e}')
        return

    # Iteracja po miesiącach
    for year, month in daterange_month(START_YEAR, END_YEAR):
        monthly_data = []  # Lista do przechowywania DataFrame'ów dla danego miesiąca
        print(f'\nPrzetwarzanie danych dla {year}-{month:02d}...')

        # Iteracja po dzielnicach
        for idx, row in areas.iterrows():
            area = row['area']
            lat = row['latitude']
            lon = row['longitude']
            print(f'  Pobieranie danych z Open-Meteo dla {area} {year}-{month:02d}...')
            start_date = datetime(year, month, 1)
            end_date = get_end_date(year, month)
            response = fetch_open_meteo_data(lat, lon, start_date, end_date)
            df = process_data(area, year, month, response)
            if df is not None:
                monthly_data.append(df)

            # Opcjonalnie, dodaj opóźnienie między zapytaniami
            time.sleep(1)

        # Po przetworzeniu wszystkich dzielnic dla miesiąca, zapisz dane
        if monthly_data:
            try:
                combined_df = pd.concat(monthly_data, ignore_index=True)
                # Zapisywanie do pliku CSV
                filename = f'open_meteo_{year}_{month:02d}.csv'
                filepath = os.path.join(OUTPUT_DIR, filename)
                combined_df.to_csv(filepath, index=False)
                print(f'Zapisano dane dla {year}-{month:02d} do {filepath}')
            except Exception as e:
                print(f'Błąd podczas zapisywania danych dla {year}-{month:02d}: {e}')
        else:
            print(f'Brak danych do zapisania dla {year}-{month:02d}.')

if __name__ == '__main__':
    main()