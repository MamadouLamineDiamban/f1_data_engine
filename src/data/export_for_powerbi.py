import pandas as pd
import numpy as np
import os
from pathlib import Path
from src.config import PROCESSED_DATA_DIR, PROJECT_ROOT

def calculate_elo_ratings(df):
    """
    Calculates Elo ratings for all drivers chronologically.
    Returns a dataframe with driverId and their final Elo rating.
    """
    K_FACTOR = 6
    INITIAL_ELO = 1500

    races_chrono = df[df['position'].notna() & (df['position'] > 0)].sort_values(['date', 'season', 'round'])
    race_keys = races_chrono.groupby(['season', 'round']).first().reset_index()[['season', 'round', 'date']].values

    elo_ratings = {}  # driverId -> current Elo
    elo_history = []

    def expected_score(ra, rb):
        return 1 / (1 + 10 ** ((rb - ra) / 400))

    for season, rnd, date in race_keys:
        race_results = races_chrono[
            (races_chrono['season'] == season) & (races_chrono['round'] == rnd)
        ][['driverId', 'position']].copy()

        drivers_in_race = race_results['driverId'].tolist()

        for did in drivers_in_race:
            if did not in elo_ratings:
                elo_ratings[did] = INITIAL_ELO

        for i, row_a in race_results.iterrows():
            for j, row_b in race_results.iterrows():
                if row_a['driverId'] >= row_b['driverId']:
                    continue
                da, db = row_a['driverId'], row_b['driverId']
                ra, rb = elo_ratings[da], elo_ratings[db]
                ea, eb = expected_score(ra, rb), expected_score(rb, ra)
                sa = 1.0 if row_a['position'] < row_b['position'] else 0.0
                sb = 1.0 - sa
                elo_ratings[da] += K_FACTOR * (sa - ea)
                elo_ratings[db] += K_FACTOR * (sb - eb)

        for did in drivers_in_race:
            elo_history.append({
                'season': season,
                'round': rnd,
                'driverId': did,
                'elo_rating': round(elo_ratings[did], 1)
            })

    return pd.DataFrame(elo_history)

def export_master_dataset():
    """
    Consolidates all datasets into a single denormalized CSV for Power BI.
    """
    print("Loading data...")
    results = pd.read_parquet(PROCESSED_DATA_DIR / "results.parquet")
    drivers = pd.read_parquet(PROCESSED_DATA_DIR / "drivers.parquet")
    constructors = pd.read_parquet(PROCESSED_DATA_DIR / "constructors.parquet")
    circuits = pd.read_parquet(PROCESSED_DATA_DIR / "circuits.parquet")
    
    # Standardize column names for merge if needed
    # (Assuming columns are already clean based on previous work)
    
    print("Merging core datasets...")
    # Join results with drivers
    master = results.merge(drivers, on='driverId', how='left', suffixes=('', '_drv'))
    # Join with constructors
    master = master.merge(constructors, on='constructorId', how='left', suffixes=('', '_const'))
    # Join with circuits
    master = master.merge(circuits, on='circuitId', how='left', suffixes=('', '_circ'))

    print("Calculating Elo ratings...")
    elo_history = calculate_elo_ratings(results)
    master = master.merge(elo_history, on=['season', 'round', 'driverId'], how='left')

    print("Integrating pit stop data...")
    pitstop_path = PROCESSED_DATA_DIR / "pitstops.parquet"
    if pitstop_path.exists():
        pitstops = pd.read_parquet(pitstop_path)
        # Calculate avg pit stop duration per driver per race
        avg_pits = pitstops.groupby(['season', 'round', 'driverId'])['duration_seconds'].mean().reset_index()
        avg_pits.columns = ['season', 'round', 'driverId', 'avg_pit_stop_duration']
        master = master.merge(avg_pits, on=['season', 'round', 'driverId'], how='left')
    else:
        print("Warning: pitstops.parquet not found. Skipping pit stop metrics.")

    # Create exports directory
    export_dir = PROJECT_ROOT / "exports"
    export_dir.mkdir(exist_ok=True)
    
    output_file = export_dir / "f1_master_dataset.csv"
    
    # Final cleanup before export
    # Convert numerical columns for Power BI
    master['season'] = master['season'].astype(int)
    master['round'] = master['round'].astype(int)
    
    print(f"Exporting to {output_file}...")
    master.to_csv(output_file, index=False, encoding='utf-8-sig')
    print("Done! Total rows exported:", len(master))

if __name__ == "__main__":
    export_master_dataset()
