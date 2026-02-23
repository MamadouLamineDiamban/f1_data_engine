import fastf1
import pandas as pd
import numpy as np
from src.config import CACHE_DIR

# Enable FastF1 cache
fastf1.Cache.enable_cache(CACHE_DIR)

class TelemetryAnalyzer:
    """
    Analyzes high-frequency F1 telemetry data using FastF1.
    Allows for driver-vs-driver lap comparisons.
    """
    
    def __init__(self, year, round_num, session_type='Q'):
        self.year = year
        self.round = round_num
        self.session_type = session_type
        self.session = None
    
    def load_session(self, laps=True, telemetry=True, weather=False):
        """Loads the session data."""
        self.session = fastf1.get_session(self.year, self.round, self.session_type)
        self.session.load(laps=laps, telemetry=telemetry, weather=weather)
        return self.session
    
    def get_driver_fastest_lap(self, driver_code):
        """Returns the fastest lap data and telemetry for a driver."""
        if not self.session:
            self.load_session()
            
        lap = self.session.laps.pick_driver(driver_code).pick_fastest()
        if lap is None:
            return None, None
            
        telemetry = lap.get_telemetry().add_distance()
        return lap, telemetry
    
    def compare_drivers(self, driver_a, driver_b):
        """
        Fetches and prepares telemetry for comparing two drivers.
        Returns a dictionary with telemetry DataFrames.
        """
        lap_a, tel_a = self.get_driver_fastest_lap(driver_a)
        lap_b, tel_b = self.get_driver_fastest_lap(driver_b)
        
        if tel_a is None or tel_b is None:
            return None
            
        return {
            driver_a: {'lap': lap_a, 'telemetry': tel_a},
            driver_b: {'lap': lap_b, 'telemetry': tel_b}
        }

if __name__ == "__main__":
    # Test with Abu Dhabi 2021
    analyzer = TelemetryAnalyzer(2021, 22, 'Q')
    comparison = analyzer.compare_drivers('VER', 'HAM')
    
    if comparison:
        print(f"Loaded telemetry for {list(comparison.keys())}")
        ver_tel = comparison['VER']['telemetry']
        print(f"Verstappen peak speed: {ver_tel['Speed'].max()} km/h")
        print(ver_tel[['Distance', 'Speed', 'Throttle', 'Brake']].head())
    else:
        print("Could not load comparison data.")
