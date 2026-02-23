import pandas as pd
from src.config import PROCESSED_DATA_DIR

df = pd.read_parquet(PROCESSED_DATA_DIR / "results.parquet")
df_d = pd.read_parquet(PROCESSED_DATA_DIR / "drivers.parquet")

print("=== RESULTS COLUMNS ===")
print(list(df.columns))
print()
print("=== RESULTS SAMPLE ===")
print(df[['season','round','raceName','driverId','driver_fullname','constructor_name','position','grid','points','status','positionText']].head(5).to_string())
print()
print("=== DRIVERS COLUMNS ===")
print(list(df_d.columns))
print()
print("=== DATE RANGE ===")
print(df['date'].min(), '->', df['date'].max())
print()
print("=== STATUS VALUES SAMPLE ===")
print(df['status'].value_counts().head(20).to_string())
