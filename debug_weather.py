import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Simplified version of what's in app.py logic
# Need to get MAPS_KEY from environment or secret manager
# Since I can't easily access secret manager in a standalone script without more setup,
# I'll rely on the fact that I've already seen MAPS_KEY is retrieved in maps_golf_lookup.py
# I will use a placeholder or try to import it if possible.

from maps_golf_lookup import MAPS_KEY

def debug_weather(lat, lng):
    weather_url = f"https://weather.googleapis.com/v1/currentConditions:lookup"
    params = {
        "key": MAPS_KEY,
        "location.latitude": lat,
        "location.longitude": lng
    }
    
    print(f"Calling: {weather_url} with location {lat}, {lng}")
    try:
        response = requests.get(weather_url, params=params, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        data = response.json()
        with open("temp/weather_debug.json", "w") as f:
            json.dump(data, f, indent=2)
        print("Raw response saved to temp/weather_debug.json")
        
        # Check if 'apparentTemperature' exists and what it's called
        return data
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    # Test with a known location (e.g., DC)
    debug_weather(38.9383, -76.8202)
