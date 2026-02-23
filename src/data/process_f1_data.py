import pandas as pd
import json
import glob
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

def process_drivers():
    """
    Cleans and optimizes driver data for the processed layer.
    """
    print("Processing drivers...")
    with open(RAW_DATA_DIR / "drivers.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    # Transformation: Merge to full name
    df['full_name'] = df['givenName'] + " " + df['familyName']
    
    # Cleaning: Removing technical/redundant columns
    df = df.drop(columns=['url', 'givenName', 'familyName'], errors='ignore')
    
    # Typing: Date conversion
    df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'])
    
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_DATA_DIR / "drivers.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Success: {len(df)} drivers saved to {output_path}")
    return df

def process_circuits():
    """
    Flattens and cleans circuit data.
    """
    print("Processing circuits...")
    with open(RAW_DATA_DIR / "circuits.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Transformation: Flattening nested 'Location' structure
    df = pd.json_normalize(data, sep='_')
    
    # Column remapping
    rename_cols = {
        'Location_lat': 'latitude',
        'Location_long': 'longitude',
        'Location_locality': 'city',
        'Location_country': 'country'
    }
    df = df.rename(columns=rename_cols)
    
    # Nettoyage
    df = df.drop(columns=['url'], errors='ignore')
    
    output_path = PROCESSED_DATA_DIR / "circuits.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Success: {len(df)} circuits saved to {output_path}")
    return df

def process_results():
    """
    Merges all available results files and flattens entries.
    This is the core analytical table.
    """
    print("Processing results (Batch merging)...")
    all_files = glob.glob(str(RAW_DATA_DIR / "results" / "results_*.json"))
    
    all_rows = []
    
    for file_path in all_files:
        with open(file_path, "r", encoding="utf-8") as f:
            races = json.load(f)
            
        for race in races:
            # Base information for the Grand Prix
            race_info = {
                'season': race.get('season'),
                'round': race.get('round'),
                'raceName': race.get('raceName'),
                'date': race.get('date'),
                'circuitId': race.get('Circuit', {}).get('circuitId')
            }
            
            # Result entry for each driver in the Grand Prix
            results = race.get('Results', [])
            for res in results:
                # Joining GP info with driver performance
                row = {**race_info, **res}
                
                # Flattening Driver and Constructor nested structures
                if 'Driver' in row:
                    row['driverId'] = row['Driver'].get('driverId')
                    row['driver_fullname'] = f"{row['Driver'].get('givenName')} {row['Driver'].get('familyName')}"
                    del row['Driver']
                
                if 'Constructor' in row:
                    row['constructorId'] = row['Constructor'].get('constructorId')
                    row['constructor_name'] = row['Constructor'].get('name')
                    del row['Constructor']
                
                # Cleaning Time dictionaries (extracting plain strings/values)
                if 'Time' in row and isinstance(row['Time'], dict):
                    row['time_millis'] = row['Time'].get('millis')
                    row['time_text'] = row['Time'].get('time')
                    del row['Time']
                
                if 'FastestLap' in row:
                    # On pourrait extraire plus ici, mais on simplifie pour le moment
                    row['fastest_lap_rank'] = row['FastestLap'].get('rank')
                    row['fastest_lap_time'] = row['FastestLap'].get('Time', {}).get('time')
                    del row['FastestLap']
                    
                all_rows.append(row)

    df = pd.DataFrame(all_rows)
    
    # Final type casting
    df['season'] = df['season'].astype(int)
    df['round'] = df['round'].astype(int)
    df['points'] = df['points'].astype(float)
    df['grid'] = pd.to_numeric(df['grid'], errors='coerce')
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'])
    
    output_path = PROCESSED_DATA_DIR / "results.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Success: {len(df)} result rows merged into {output_path}")
    return df

def process_constructors():
    """
    Cleans constructor data.
    """
    print("Processing constructors...")
    with open(RAW_DATA_DIR / "constructors.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df = df.drop(columns=['url'], errors='ignore')
    
    output_path = PROCESSED_DATA_DIR / "constructors.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Success: {len(df)} constructors saved.")
    return df

if __name__ == "__main__":
    process_drivers()
    process_constructors()
    process_circuits()
    process_results()
