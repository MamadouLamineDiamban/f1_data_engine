import requests
import json
import time
import os
from src.config import JOLPICA_BASE_URL, RAW_DATA_DIR

def fetch_page(endpoint, limit=100, offset=0, retries=5):
    """
    Effectue une requete HTTP GET avec gestion robuste des erreurs et du Rate Limiting.
    """
    url = f"{JOLPICA_BASE_URL}/{endpoint}.json?limit={limit}&offset={offset}"
    
    for attempt in range(retries):
        try:
            # Pause de courtoisie
            time.sleep(0.8) 
            
            response = requests.get(url, timeout=20)
            
            # Gestion du Rate Limiting (Error 429)
            if response.status_code == 429:
                wait_time = (attempt + 1) * 10
                print(f"[429] Limite atteinte. Attente de {wait_time}s...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            wait = (attempt + 1) * 2
            print(f"Tentative {attempt + 1}/{retries} échouée ({e}). Attente {wait}s...")
            time.sleep(wait)
            
    return None

def get_all(endpoint):
    """
    Recupere l'integralite des donnees d'un endpoint via pagination.
    """
    all_results = []
    limit = 100
    offset = 0
    
    # Mapping complet Table/List pour le standard Ergast/Jolpica
    mapping = {
        "drivers": ("DriverTable", "Drivers"),
        "circuits": ("CircuitTable", "Circuits"),
        "constructors": ("ConstructorTable", "Constructors"),
        "results": ("RaceTable", "Races"),
        "seasons": ("SeasonTable", "Seasons"),
        "driverstandings": ("StandingsTable", "StandingsLists"),
        "constructorstandings": ("StandingsTable", "StandingsLists")
    }
    
    # On recupere le type de ressource (ex: 'results' si l'endpoint est '2026/results')
    resource_type = endpoint.split('/')[-1]
    table_key, list_key = mapping.get(resource_type, (f"{resource_type.capitalize()}Table", resource_type.capitalize()))
    
    while True:
        data = fetch_page(endpoint, limit, offset)
        if not data:
            print(f"Arret critique pendant la collecte de {endpoint}")
            break
            
        mr_data = data['MRData']
        new_items = mr_data[table_key][list_key]
        all_results.extend(new_items)
        
        # Identification du nombre reel d'items collectes (ex: resultats dans les courses)
        total = int(mr_data['total'])
        if resource_type == "results":
            # On compte les resultats individuels dans chaque Grand Prix
            current_count = sum(len(race.get('Results', [])) for race in all_results)
        elif "standings" in resource_type:
            # On compte les pilotes/ecuries dans les listes de classement
            current_count = sum(len(s_list.get('DriverStandings', s_list.get('ConstructorStandings', []))) 
                               for s_list in all_results)
        else:
            current_count = len(all_results)
            
        if total > 0:
            print(f"{endpoint} : {current_count} / {total}")
        
        # Condition d'arret basee sur le compte reel ou la fin des donnees
        if current_count >= total or not new_items:
            break
        offset += limit
        
    return all_results

def save_raw_json(data, resource_path):
    """
    Sauvegarde les données au format JSON dans data/raw/.
    Gère la création de sous-dossiers (ex: results/1950.json).
    """
    file_path = RAW_DATA_DIR / f"{resource_path}.json"
    
    # Creation automatique des sous-dossiers
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(file_path, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"Sauvegarde : {file_path}")
    return file_path

def run_main_ingestion():
    """
    Execute le pipeline complet d'ingestion historique.
    """
    # 1. Ressources globales
    print("=== Phase 1 : Donnees de reference ===")
    global_targets = ["drivers", "constructors", "circuits", "seasons"]
    for target in global_targets:
        if not (RAW_DATA_DIR / f"{target}.json").exists():
            data = get_all(target)
            save_raw_json(data, target)
        else:
            print(f"{target} deja present, passage au suivant.")

    # 2. Ressources granulaires
    print("\n=== Phase 2 : Resultats et classements (saison par saison) ===")
    
    # On charge les saisons pour boucler dessus
    with open(RAW_DATA_DIR / "seasons.json", "r") as f:
        seasons = json.load(f)
        years = [s['season'] for s in seasons]

    for year in sorted(years, reverse=True): # On commence par les plus recentes
        # Ingestion des resultats de l'annee
        res_path = f"results/results_{year}"
        if not (RAW_DATA_DIR / f"{res_path}.json").exists():
            print(f"\nSaison {year}...")
            # Resultats
            results = get_all(f"{year}/results")
            save_raw_json(results, res_path)
            # Standings Pilotes
            standings = get_all(f"{year}/driverstandings")
            save_raw_json(standings, f"standings/drivers_{year}")
            
            # Pause de securite entre saisons
            time.sleep(2)
        else:
            print(f"Saison {year} deja archivee.")

if __name__ == "__main__":
    run_main_ingestion()