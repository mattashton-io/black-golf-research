import googlemaps
import os
import json
import csv
import io
import time
import requests
from datetime import datetime
from google.cloud import secretmanager
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
secret_token_id = os.environ.get("SECRET_PLACES")
secret_bucket_id = os.environ.get("SECRET_BUCKET")
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
secret_census = os.environ.get("SECRET_CENSUS_API")

# Initialize Secret Manager client
secret_client = secretmanager.SecretManagerServiceClient()

# API configuration
API_TIMEOUT = 5 # 5 second timeout for external API calls

def get_secret(secret_id):
    if not secret_id:
        return None
    # If the secret_id itself looks like an API key (no dashes/underscores or specific pattern), return it
    if len(secret_id) > 20 and "-" not in secret_id and "_" not in secret_id:
         return secret_id
         
    try:
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Secret Manager Error for {secret_id}, falling back to env: {e}")
        # Fallback to checking if the secret_id exists as an ENV var itself or 
        # if there's a direct mapping (e.g., PLACES_API_KEY)
        return os.environ.get(secret_id) or os.environ.get(secret_id.upper())

_MAPS_KEY = None
_CENSUS_KEY = None

def get_maps_key():
    global _MAPS_KEY
    if _MAPS_KEY is None:
        _MAPS_KEY = get_secret(secret_token_id)
    return _MAPS_KEY

def get_census_key():
    global _CENSUS_KEY
    if _CENSUS_KEY is None:
        _CENSUS_KEY = get_secret(secret_census)
    return _CENSUS_KEY

def load_from_gcs(zip_code=None):
    """
    Load existing golf course data to avoid redundant API calls.
    If zip_code is provided, it ONLY checks for specific cached results for that zip.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(secret_bucket_id)
        
        # Check for zip-specific cache if provided
        if zip_code:
            zip_blob = bucket.blob(f"cache/{zip_code}_results.json")
            if zip_blob.exists():
                print(f"Loading cached results for zip: {zip_code}")
                content = zip_blob.download_as_text()
                return json.loads(content)
            return None # Return None if specific zip cache requested but not found

        # Fallback to main file ONLY if no zip_code provided (e.g., initial load or global analytics)
        blob = bucket.blob("golf_courses.json")
        if blob.exists():
            content = blob.download_as_text()
            data = json.loads(content)
            if isinstance(data, dict) and "results" in data:
                print(f"Loaded {len(data['results'])} courses from GCS.")
                return {item['place_id']: item for item in data['results']}, data.get("metadata", {})
            elif isinstance(data, list):
                print(f"Loaded {len(data)} courses from GCS (legacy format).")
                return {item['place_id']: item for item in data}, {}
    except Exception as e:
        print(f"Note: Could not load existing data: {e}")
    
    if zip_code:
        return None
    return {}, {}

def save_to_zip_cache(zip_code, data_to_cache):
    """Save results specifically for a zip code to GCS cache."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(secret_bucket_id)
        blob = bucket.blob(f"cache/{zip_code}_results.json")
        blob.upload_from_string(json.dumps(data_to_cache), content_type="application/json")
        print(f"Cached results for zip: {zip_code}")
    except Exception as e:
        print(f"Error caching for zip {zip_code}: {e}")

def update_course_in_cache(zip_code, place_id, updates):
    """Updates specific fields for a course in the zip-specific cache."""
    try:
        data = load_from_gcs(zip_code)
        if not data or "courses" not in data:
            return
        
        # In zip_cache, it's a list under 'courses'
        updated = False
        for course in data["courses"]:
            if course.get("place_id") == place_id:
                for key, val in updates.items():
                    course[key] = val
                updated = True
                break
        
        if updated:
            save_to_zip_cache(zip_code, data)
    except Exception as e:
        print(f"Error updating cache for {place_id} in {zip_code}: {e}")

def get_census_tract(lat, lng):
    """Convert Lat/Lng to Census Tract GEOID using Census Geocoder."""
    url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lng}&y={lat}&benchmark=Public_AR_Current&vintage=Current_Current&format=json"
    try:
        response = requests.get(url, timeout=API_TIMEOUT)
        response.raise_for_status()
        
        # Check if response is JSON
        if "application/json" not in response.headers.get("Content-Type", ""):
            print(f"Error: Census Geocoder returned non-JSON response: {response.text[:100]}")
            return None
            
        data = response.json()
        geographies = data['result']['geographies']['Census Tracts'][0]
        return {
            "state": geographies['STATE'],
            "county": geographies['COUNTY'],
            "tract": geographies['TRACT'],
            "geoid": geographies['GEOID']
        }
    except Exception as e:
        print(f"Error getting census tract: {e}")
        return None

def get_demographics(state, county, tract):
    """Fetch ACS estimates for a specific tract, attempting 2024 ACS1 first, then falling back to 2022 ACS5."""
    # B01003_001E: Total, B02001_003E: Black, B02001_002E: White, B03001_003E: Hispanic, 
    # B02001_005E: Asian, B02001_004E: Native, B02001_006E: Pacific, B19013_001E: Median Income
    # B17001_001E: Poverty Total, B17001_002E: Below Poverty
    vars = "NAME,B01003_001E,B02001_003E,B02001_002E,B03001_003E,B02001_005E,B02001_004E,B02001_006E,B19013_001E,B17001_001E,B17001_002E"
    
    configs = [
        ("2024", "acs/acs1"),
        ("2023", "acs/acs5"),
        ("2022", "acs/acs5")
    ]
    
    for year, dataset in configs:
        base_url = f"https://api.census.gov/data/{year}/{dataset}"
        params = { "get": vars, "for": f"tract:{tract}", "in": f"state:{state} county:{county}", "key": get_census_key() }
        try:
            response = requests.get(base_url, params=params, timeout=API_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                if len(data) > 1:
                    # 0:NAME, 1:Total, 2:Black, 3:White, 4:Hispanic, 5:Asian, 6:Native, 7:Pacific, 8:Income, 9:PovTotal, 10:BelowPov
                    row = data[1]
                    total_pop = int(row[1]) if row[1] else 0
                    
                    def safe_int(val): return int(val) if val and val != "-666666666" else 0
                    
                    black_pop = safe_int(row[2])
                    white_pop = safe_int(row[3])
                    hispanic_pop = safe_int(row[4])
                    median_income = safe_int(row[8])
                    
                    poverty_total = safe_int(row[9])
                    below_poverty = safe_int(row[10])
                    
                    def calc_pct(val, total): return round((val / total) * 100, 2) if total > 0 else 0
                    
                    return {
                        "year_source": year,
                        "total_pop": total_pop,
                        "median_income": median_income,
                        "pct_black": calc_pct(black_pop, total_pop), 
                        "pct_white": calc_pct(white_pop, total_pop),
                        "pct_hispanic": calc_pct(hispanic_pop, total_pop),
                        "pct_asian": calc_pct(safe_int(row[5]), total_pop),
                        "pct_native": calc_pct(safe_int(row[6]), total_pop),
                        "pct_pacific": calc_pct(safe_int(row[7]), total_pop),
                        "pct_poverty": calc_pct(below_poverty, poverty_total),
                        "is_plurality_black": (black_pop > white_pop) and (black_pop > hispanic_pop)
                    }
        except Exception as e:
            print(f"Census Error ({year} {dataset}): {e}")
            continue
    return None

def is_in_holc_redlined_zone(lat, lng):
    """
    Checks if a location is in a historically redlined zone (Grade D).
    Uses a spatial lookup or API call (Mapping Inequality) to determine the 1930s HOLC grade.
    """
    # Placeholder: In a real app, this would query a spatial index from Mapping Inequality.
    # For now, we will simulate a logic that returns a grade based on tract demographics 
    # or proximity to known historic redlined areas.
    # Returning a simulated Grade D for demo if pct_black > 40
    return "D" if lat > 38.9 and lat < 39.0 and lng > -77.0 and lng < -76.8 else "C"

def get_barrier_to_entry(place_types):
    """Maps Google Places types to a user-friendly classification."""
    if not place_types:
        return "Unknown"
    
    types = [t.lower() for t in place_types]
    if "country_club" in types:
        return "Private"
    elif "golf_course" in types:
        # Most likely semi-private or municipal, default to semi-private if not explicitly municipal
        return "Semi-Private"
    return "Municipal/Public"


def export_to_gcs(courses_dict, origin, radii):
    storage_client = storage.Client()
    bucket = storage_client.bucket(secret_bucket_id)

    metadata = {
        "search_origin": {"lat": origin[0], "lng": origin[1]},
        "radii_searched_miles": radii,
        "last_updated": datetime.now().isoformat(),
        "total_courses": len(courses_dict)
    }

    # Export to JSON
    json_output = {
        "metadata": metadata,
        "results": list(courses_dict.values())
    }
    json_data = json.dumps(json_output, indent=2)
    json_blob = bucket.blob("golf_courses.json")
    json_blob.upload_from_string(json_data, content_type="application/json")
    print(f"Successfully exported to gs://{secret_bucket_id}/golf_courses.json")

    # Export to CSV
    output = io.StringIO()
    if courses_dict:
        fieldnames = ['name', 'address', 'lat', 'lng', 'place_id', 'rating', 'user_ratings_total', 
                      'census_geoid', 'pct_black', 'total_pop', 'search_lat', 'search_lng', 'radii_scanned']
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for course in courses_dict.values():
            row = {
                'name': course.get('name'),
                'address': course.get('formatted_address', course.get('vicinity', '')),
                'lat': course['geometry']['location']['lat'],
                'lng': course['geometry']['location']['lng'],
                'place_id': course.get('place_id'),
                'rating': course.get('rating'),
                'user_ratings_total': course.get('user_ratings_total'),
                'census_geoid': course.get('census_geoid'),
                'pct_black': course.get('pct_black'),
                'total_pop': course.get('total_pop'),
                'search_lat': origin[0],
                'search_lng': origin[1],
                'radii_scanned': str(radii)
            }
            writer.writerow(row)

    csv_blob = bucket.blob("golf_courses.csv")
    csv_blob.upload_from_string(output.getvalue(), content_type="text/csv")
    print(f"Successfully exported to gs://{secret_bucket_id}/golf_courses.csv")

def enrich_course_with_demographics(place):
    """Fetches and appends demographic data to a place object if possible."""
    try:
        lat = place['geometry']['location']['lat']
        lng = place['geometry']['location']['lng']
        
        geo_info = get_census_tract(lat, lng)
        if geo_info:
            place['census_geoid'] = geo_info['geoid']
            stats = get_demographics(geo_info['state'], geo_info['county'], geo_info['tract'])
            if stats:
                place['pct_black'] = stats['pct_black']
                place['pct_white'] = stats['pct_white']
                place['pct_hispanic'] = stats['pct_hispanic']
                place['pct_asian'] = stats['pct_asian']
                place['pct_native'] = stats['pct_native']
                place['pct_pacific'] = stats['pct_pacific']
                place['total_pop'] = stats['total_pop']
                place['pct_poverty'] = stats['pct_poverty']
                place['median_income'] = stats.get('median_income')
                place['is_plurality_black'] = stats.get('is_plurality_black', False)
                place['holc_grade'] = is_in_holc_redlined_zone(lat, lng)
                place['barrier_to_entry'] = get_barrier_to_entry(place.get('types', []))
                
                print(f"  - Neighborhood: {stats['pct_black']}% Black (Pop: {stats['total_pop']})")
                if stats['pct_black'] > 50:
                    print("  - [INSIGHT]: Located in a Majority-Black Neighborhood.")
                if stats.get('is_plurality_black'):
                    print("  - [INSIGHT]: Located in a Plurality-Black Neighborhood.")
                return True
            else:
                print("  - Demographic data unavailable.")
        else:
            print("  - Geocoding failed.")
    except Exception as e:
        print(f"  - Error during enrichment: {e}")
    return False

def search_golf_courses(origin_lat, origin_lng, radii_miles, target_black_majority=True):
    """
    Searches for golf courses around a given origin within multiple radii.
    If target_black_majority is True, it will auto-expand up to 25 miles if no 
    Black-majority courses are found within the initial radii.
    """
    gmaps = googlemaps.Client(key=get_maps_key(), timeout=API_TIMEOUT)
    unique_courses = {}
    
    print(f"Starting scan around ({origin_lat}, {origin_lng}) with radii: {radii_miles} miles...")
    
    current_radii = list(radii_miles)
    found_black_majority = False

    def scan_radius(radius_mi):
        nonlocal found_black_majority
        radius_meters = int(radius_mi * 1609.34)
        print(f"\nScanning with radius: {radius_mi} miles ({radius_meters} meters)...")
        
        try:
            places_result = gmaps.places(
                query='golf courses',
                location=(origin_lat, origin_lng),
                radius=radius_meters
            )

            for place in places_result.get('results', []):
                place_id = place['place_id']
                
                if place_id not in unique_courses:
                    unique_courses[place_id] = place
                    print(f"New Course Found - Name: {place['name']}")
                    enrich_course_with_demographics(place)
                    if place.get('pct_black', 0) > 50:
                        found_black_majority = True
        except Exception as e:
            print(f"Error during search at radius {radius_mi}: {e}")

    for r in current_radii:
        scan_radius(r)
        if target_black_majority and found_black_majority:
            break

    # Auto-expansion logic
    if target_black_majority and not found_black_majority:
        max_radius = 25
        last_radius = current_radii[-1] if current_radii else 10
        increment = 5
        
        while last_radius < max_radius and not found_black_majority:
            last_radius = min(last_radius + increment, max_radius)
            print(f"[AUTO-EXPAND] Expanding search to {last_radius} miles...")
            scan_radius(last_radius)
            if last_radius >= max_radius:
                break
            
    return unique_courses

if __name__ == "__main__":
    # Example execution if run directly
    search_origin = (38.9383, -76.8202) # Washington D.C.
    radii = [10, 15]
    results = search_golf_courses(search_origin[0], search_origin[1], radii)
    print(f"Total unique courses found: {len(results)}")