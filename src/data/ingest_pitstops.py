"""
Fetches pit stop data from the Jolpica API for seasons 2012-2025
and processes it into a Parquet file.
"""
import requests
import json
import time
import pandas as pd
from pathlib import Path
from src.config import JOLPICA_BASE_URL, RAW_DATA_DIR, PROCESSED_DATA_DIR


def fetch_page(endpoint, limit=100, offset=0, retries=5):
    url = f"{JOLPICA_BASE_URL}/{endpoint}.json?limit={limit}&offset={offset}"
    for attempt in range(retries):
        try:
            time.sleep(0.8)
            response = requests.get(url, timeout=20)
            if response.status_code == 429:
                wait_time = (attempt + 1) * 10
                print(f"[429] Rate limit. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            wait = (attempt + 1) * 2
            print(f"Attempt {attempt+1}/{retries} failed ({e}). Waiting {wait}s...")
            time.sleep(wait)
    return None


def fetch_season_pitstops(season):
    """Fetch all pit stop data for a given season, round by round."""
    # First get the number of rounds in this season
    schedule = fetch_page(f"{season}", limit=100)
    if not schedule:
        return []

    races = schedule['MRData']['RaceTable']['Races']
    all_pitstops = []

    for race in races:
        round_num = race['round']
        race_name = race['raceName']
        race_date = race.get('date', '')
        offset = 0

        while True:
            data = fetch_page(f"{season}/{round_num}/pitstops", limit=100, offset=offset)
            if not data:
                break

            race_data = data['MRData']['RaceTable']['Races']
            if not race_data or 'PitStops' not in race_data[0]:
                break

            stops = race_data[0]['PitStops']
            if not stops:
                break

            for stop in stops:
                all_pitstops.append({
                    'season': int(season),
                    'round': int(round_num),
                    'raceName': race_name,
                    'date': race_date,
                    'driverId': stop['driverId'],
                    'stop_number': int(stop['stop']),
                    'lap': int(stop['lap']),
                    'time': stop.get('time', ''),
                    'duration': stop['duration'],
                })

            total = int(data['MRData']['total'])
            offset += 100
            if offset >= total:
                break

        print(f"  {season} R{round_num} {race_name}: {len([s for s in all_pitstops if s['round'] == int(round_num)])} stops")

    return all_pitstops


def run_pitstop_ingestion():
    """Ingest pit stop data for seasons 2012-2025."""
    pitstops_dir = RAW_DATA_DIR / "pitstops"
    pitstops_dir.mkdir(parents=True, exist_ok=True)

    all_data = []

    for season in range(2012, 2026):
        cache_file = pitstops_dir / f"pitstops_{season}.json"
        if cache_file.exists():
            print(f"Season {season} already cached, loading...")
            with open(cache_file, 'r', encoding='utf-8') as f:
                season_data = json.load(f)
        else:
            print(f"\n=== Fetching pit stops for {season} ===")
            season_data = fetch_season_pitstops(season)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(season_data, f, ensure_ascii=False, indent=2)
            print(f"  Saved {len(season_data)} stops to {cache_file}")
            time.sleep(1)  # Cooldown between seasons

        all_data.extend(season_data)

    return all_data


def process_pitstops(raw_data):
    """Convert raw pit stop data to a clean Parquet file."""
    df = pd.DataFrame(raw_data)

    if df.empty:
        print("No pit stop data to process.")
        return df

    # Parse duration to seconds
    def parse_duration(d):
        try:
            parts = str(d).split(':')
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 1:
                return float(parts[0])
            else:
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        except (ValueError, IndexError):
            return None

    df['duration_seconds'] = df['duration'].apply(parse_duration)

    # Merge with driver names
    drivers_df = pd.read_parquet(PROCESSED_DATA_DIR / "drivers.parquet")
    # Use 'full_name' as it's the correct column in drivers.parquet
    driver_name_map = dict(zip(drivers_df['driverId'], drivers_df['full_name']))
    df['driver_fullname'] = df['driverId'].map(driver_name_map)

    # Save
    out_path = PROCESSED_DATA_DIR / "pitstops.parquet"
    df.to_parquet(out_path, index=False)
    print(f"\nProcessed {len(df)} pit stops -> {out_path}")
    print(f"Seasons: {df['season'].min()}-{df['season'].max()}")
    print(f"Avg duration: {df['duration_seconds'].mean():.2f}s")

    return df


if __name__ == "__main__":
    raw = run_pitstop_ingestion()
    process_pitstops(raw)
