import googlemaps
import os
import requests
import json
from google.cloud import secretmanager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
secret_places_id = os.environ.get("SECRET_PLACES")
secret_census_id = os.environ.get("SECRET_CENSUS_API")
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

def get_secret(secret_id):
    """Fetch secret payload from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Initialize Clients
MAPS_KEY = get_secret(secret_places_id)
CENSUS_KEY = get_secret(secret_census_id)
gmaps = googlemaps.Client(key=MAPS_KEY)

def get_census_tract(lat, lng):
    """Convert Lat/Lng to Census Tract GEOID using Census Geocoder."""
    url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lng}&y={lat}&benchmark=Public_AR_Current&vintage=Current_Current&format=json"
    try:
        response = requests.get(url)
        data = response.json()
        # Extract State, County, and Tract to form the 11-digit GEOID
        geographies = data['result']['geographies']['Census Tracts'][0]
        return {
            "state": geographies['STATE'],
            "county": geographies['COUNTY'],
            "tract": geographies['TRACT'],
            "geoid": geographies['GEOID']
        }
    except Exception as e:
        return None

def get_demographics(state, county, tract):
    """Fetch ACS 5-Year estimates for a specific tract."""
    # B02001_003E: Black or African American alone
    # B01003_001E: Total Population
    base_url = "https://api.census.gov/data/2022/acs/acs5"
    params = {
        "get": "NAME,B02001_003E,B01003_001E",
        "for": f"census tract:{tract}",
        "in": f"state:{state} county:{county}",
        "key": CENSUS_KEY
    }
    try:
        response = requests.get(base_url, params=params)
        data = response.json()
        # Data format: [["NAME", "B02001_003E", "B01003_001E", ...], ["Tract Name", "Value", "Total", ...]]
        black_pop = int(data[1][1])
        total_pop = int(data[1][2])
        pct_black = (black_pop / total_pop) * 100 if total_pop > 0 else 0
        return {"pct_black": round(pct_black, 2), "total_pop": total_pop}
    except Exception:
        return None

# Execution Logic
radii_miles = [10] # Simplified for demo
print(f"Scanning D.C. area for golf courses and neighborhood demographics...")

places_result = gmaps.places(
    query='golf courses',
    location=(38.9383, -76.8202),
    radius=int(10 * 1609.34)
)

for place in places_result.get('results', []):
    name = place['name']
    lat = place['geometry']['location']['lat']
    lng = place['geometry']['location']['lng']
    
    # 1. Get Census IDs
    geo_info = get_census_tract(lat, lng)
    
    if geo_info:
        # 2. Get Demographic Data
        stats = get_demographics(geo_info['state'], geo_info['county'], geo_info['tract'])
        if stats:
            print(f"Course: {name}")
            print(f"  - Neighborhood: {stats['pct_black']}% Black Population (Total: {stats['total_pop']})")
            if stats['pct_black'] > 50:
                print("  - [INSIGHT]: Located in a Majority-Black Neighborhood.")
        else:
            print(f"Course: {name} - Demographic data unavailable.")
    else:
        print(f"Course: {name} - Geocoding failed.")