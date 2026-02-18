import fastf1
import json
import os
import pandas as pd
from pathlib import Path
from src.config import RAW_DATA_DIR

# Configuration du cache FastF1
CACHE_DIR = Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)

def get_session_results(year, round_num, session_type='R'):
    """
    Recupere les resultats d'une session specifique (R=Race, Q=Qualifying, etc.)
    et les sauvegarde au format JSON compatible avec notre pipeline.
    """
    try:
        print(f"Recuperation FastF1 : {year} Round {round_num} ({session_type})...")
        session = fastf1.get_session(year, round_num, session_type)
        session.load(laps=False, telemetry=False, weather=False, messages=False)
        
        # On recupere les resultats
        results_df = session.results
        
        # Transformation en dictionnaire compatible avec notre structure Jolpica 'light'
        # Note : On recree une structure minimale pour ne pas casser process_historical_data.py
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
                    "dateOfBirth": "1900-01-01", # Non dispo facilement dans FastF1 results
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
            
        return [race_data] # Retourne une liste pour compatibilite avec la boucle de processing
        
    except Exception as e:
        print(f"Erreur FastF1 pour {year} Rd {round_num} : {e}")
        return None

def update_modern_results(year):
    """
    Parcourt le calendrier de l'annee et recupere les resultats des courses terminees.
    """
    schedule = fastf1.get_event_schedule(year)
    # On filtre les vrais Grands Prix (ceux qui ont un RoundNumber > 0)
    rounds = schedule[schedule['RoundNumber'] > 0]
    
    for _, event in rounds.iterrows():
        round_num = event['RoundNumber']
        
        # On verifie si la course est deja passee
        if pd.to_datetime(event['EventDate']).tz_localize(None) < pd.Timestamp.now():
            data = get_session_results(year, round_num)
            if data:
                # Sauvegarde au format Bronze
                output_file = RAW_DATA_DIR / "results" / f"results_{year}.json"
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Chargement de l'existant si besoin
                existing_data = []
                if output_file.exists():
                    with open(output_file, "r") as f:
                        existing_data = json.load(f)
                
                # Mise a jour (on evite les doublons de round)
                existing_rounds = [r['round'] for r in existing_data]
                if str(round_num) not in existing_rounds:
                    existing_data.extend(data)
                    with open(output_file, "w", encoding='utf-8') as f:
                        json.dump(existing_data, f, indent=4)
                    print(f"Saison {year} Rd {round_num} ajoutee.")

if __name__ == "__main__":
    # On tente de recuperer les premiers GP de 2025
    update_modern_results(2025)
