import pandas as pd
import json
import glob
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

def process_drivers():
    """
    Nettoie et optimise les donnees des pilotes pour la couche Silver.
    """
    print("Traitement des pilotes...")
    with open(RAW_DATA_DIR / "drivers.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    # Transformation : Fusion du nom complet
    df['full_name'] = df['givenName'] + " " + df['familyName']
    
    # Nettoyage : Suppression des colonnes techniques
    df = df.drop(columns=['url', 'givenName', 'familyName'], errors='ignore')
    
    # Typage : Conversion des dates
    df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'])
    
    # Sauvegarde
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_DATA_DIR / "drivers.parquet"
    df.to_parquet(output_path, index=False)
    print(f"OK : {len(df)} pilotes sauvegardes dans {output_path}")
    return df

def process_circuits():
    """
    Aplatit et nettoie les donnees des circuits.
    """
    print("Traitement des circuits...")
    with open(RAW_DATA_DIR / "circuits.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Transformation : Aplatissement de la structure imbriquee 'Location'
    df = pd.json_normalize(data, sep='_')
    
    # Renommage
    rename_cols = {
        'Location_lat': 'latitude',
        'Location_long': 'longitude',
        'Location_locality': 'city',
        'Location_country': 'country'
    }
    df = df.rename(columns=rename_cols)
    
    # Nettoyage
    df = df.drop(columns=['url'], errors='ignore')
    
    # Sauvegarde
    output_path = PROCESSED_DATA_DIR / "circuits.parquet"
    df.to_parquet(output_path, index=False)
    print(f"OK : {len(df)} circuits sauvegardes dans {output_path}")
    return df

def process_results():
    """
    Fusionne tous les fichiers de resultats par saison et aplatit les donnees.
    C'est la table centrale du projet.
    """
    print("Traitement des resultats (Fusion massive)...")
    all_files = glob.glob(str(RAW_DATA_DIR / "results" / "results_*.json"))
    
    all_rows = []
    
    for file_path in all_files:
        with open(file_path, "r", encoding="utf-8") as f:
            races = json.load(f)
            
        for race in races:
            # Extraction des informations de base du Grand Prix
            race_info = {
                'season': race.get('season'),
                'round': race.get('round'),
                'raceName': race.get('raceName'),
                'date': race.get('date'),
                'circuitId': race.get('Circuit', {}).get('circuitId')
            }
            
            # Pour chaque pilote dans ce Grand Prix
            results = race.get('Results', [])
            for res in results:
                # On fusionne les infos du GP avec les perfs du pilote
                row = {**race_info, **res}
                
                # Aplatissement manuel des dictionnaires Driver et Constructor
                if 'Driver' in row:
                    row['driverId'] = row['Driver'].get('driverId')
                    row['driver_fullname'] = f"{row['Driver'].get('givenName')} {row['Driver'].get('familyName')}"
                    del row['Driver']
                
                if 'Constructor' in row:
                    row['constructorId'] = row['Constructor'].get('constructorId')
                    row['constructor_name'] = row['Constructor'].get('name')
                    del row['Constructor']
                
                # Nettoyage des dictionnaires de temps (on ne garde que le texte simple)
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
    
    # Nettoyage final des types
    df['season'] = df['season'].astype(int)
    df['round'] = df['round'].astype(int)
    df['points'] = df['points'].astype(float)
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'])
    
    output_path = PROCESSED_DATA_DIR / "results.parquet"
    df.to_parquet(output_path, index=False)
    print(f"OK : {len(df)} lignes de résultats fusionnées dans {output_path}")
    return df

def process_constructors():
    """
    Nettoie les donnees des ecuries.
    """
    print("Traitement des constructeurs...")
    with open(RAW_DATA_DIR / "constructors.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df = df.drop(columns=['url'], errors='ignore')
    
    output_path = PROCESSED_DATA_DIR / "constructors.parquet"
    df.to_parquet(output_path, index=False)
    print(f"OK : {len(df)} constructeurs sauvegardés.")
    return df

if __name__ == "__main__":
    process_drivers()
    process_constructors()
    process_circuits()
    process_results()
