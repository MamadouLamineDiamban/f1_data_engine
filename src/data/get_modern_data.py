import fastf1
import json
import os
import pandas as pd
from pathlib import Path
from src.config import RAW_DATA_DIR, CACHE_DIR

# FastF1 Cache Configuration
CACHE_DIR.mkdir(parents=True, exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)

def get_session_results(year, round_num, session_type='R'):
    """
    Fetches results for a specific session (R=Race, Q=Qualifying, etc.)
    and saves it in a format compatible with our pipeline.
    """
    try:
        print(f"FastF1 Fetch: {year} Round {round_num} ({session_type})...")
        session = fastf1.get_session(year, round_num, session_type)
        session.load(laps=False, telemetry=False, weather=False, messages=False)
        
        # Data transformation to reflect Jolpica-like structure for downstream compatibility
        # Note: Minimal structure to avoid breaking process_historical_data.py
        race_data = {
            "season": str(year),
            "round": str(round_num),
            "raceName": session.event['EventName'],
            "date": session.date.strftime('%Y-%m-%d'),
            "Circuit": {
                "circuitId": session.event['Location'].lower().replace(' ', '_'),
                "circuitName": session.event['EventName']
            },
            "Results": []
        }
        
        for _, row in results_df.iterrows():
            result_item = {
                "number": str(row['ResultNumber']),
                "position": str(row['ClassifiedPosition']) if row['ClassifiedPosition'] else "R",
                "points": str(row['Points']),
                "Driver": {
                    "driverId": row['Abbreviation'].lower(),
                    "permanentNumber": str(row['DriverNumber']),
                    "code": row['Abbreviation'],
                    "givenName": row['FirstName'],
                    "familyName": row['LastName'],
                    "dateOfBirth": "1900-01-01", # Birthdays not readily available in session results
                    "nationality": "Unknown"
                },
                "Constructor": {
                    "constructorId": row['TeamName'].lower().replace(' ', '_'),
                    "name": row['TeamName'],
                    "nationality": "Unknown"
                },
                "status": row['Status'],
                "Time": {"millis": "0", "time": row['Time'].total_seconds() if hasattr(row['Time'], 'total_seconds') else "0"}
            }
            race_data["Results"].append(result_item)
            
        return [race_data] # Returns list for loop compatibility
        
    except Exception as e:
        print(f"FastF1 Error for {year} Rd {round_num}: {e}")
        return None

def update_modern_results(year):
    """
    Iterates through the season schedule and fetches available race results.
    """
    schedule = fastf1.get_event_schedule(year)
    # Filter for Championship Grands Prix (RoundNumber > 0)
    rounds = schedule[schedule['RoundNumber'] > 0]
    
    for _, event in rounds.iterrows():
        round_num = event['RoundNumber']
        
        # Check if race has occurred
        if pd.to_datetime(event['EventDate']).tz_localize(None) < pd.Timestamp.now():
            data = get_session_results(year, round_num)
            if data:
                # Sauvegarde au format Bronze
                output_file = RAW_DATA_DIR / "results" / f"results_{year}.json"
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Load existing if available
                existing_data = []
                if output_file.exists():
                    with open(output_file, "r") as f:
                        existing_data = json.load(f)
                
                # Update avoiding duplicate rounds
                existing_rounds = [r['round'] for r in existing_data]
                if str(round_num) not in existing_rounds:
                    existing_data.extend(data)
                    with open(output_file, "w", encoding='utf-8') as f:
                        json.dump(existing_data, f, indent=4)
                    print(f"Season {year} Rd {round_num} added.")

if __name__ == "__main__":
    # On tente de recuperer les premiers GP de 2025
    update_modern_results(2025)
