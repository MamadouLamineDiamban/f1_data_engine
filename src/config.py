from pathlib import Path

# Chemin racine du projet
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
AGGREGATED_DATA_DIR = DATA_DIR / "aggregated"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

# Dossier de cache
CACHE_DIR = PROJECT_ROOT / "cache"

# URLs des APIs
JOLPICA_BASE_URL = "https://api.jolpi.ca/ergast/f1"

# Directory creation
for path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, AGGREGATED_DATA_DIR, EXTERNAL_DATA_DIR, CACHE_DIR]:
    path.mkdir(parents=True, exist_ok=True)