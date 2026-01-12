import googlemaps
import os
import json
import csv
import io
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

def get_secret(secret_id):
    if not secret_id:
        return None
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_places_api_key():
    return get_secret(secret_token_id)

def get_census_api_key():
    return get_secret(secret_census)

MAPS_KEY = get_places_api_key()
CENSUS_KEY = get_census_api_key()

def load_from_gcs():
    """Load existing golf course data to avoid redundant API calls."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(secret_bucket_id)
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
        print(f"Note: Could not load existing data (or bucket empty): {e}")
    return {}, {}

def get_census_tract(lat, lng):
    """Convert Lat/Lng to Census Tract GEOID using Census Geocoder."""
    url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lng}&y={lat}&benchmark=Public_AR_Current&vintage=Current_Current&format=json"
    try:
        response = requests.get(url)
        data = response.json()
        geographies = data['result']['geographies']['Census Tracts'][0]
        return {
            "state": geographies['STATE'],
            "county": geographies['COUNTY'],
            "tract": geographies['TRACT'],
            "geoid": geographies['GEOID']
        }
    except Exception:
        return None

def get_demographics(state, county, tract):
    """Fetch ACS 5-Year estimates for a specific tract."""
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
        black_pop = int(data[1][1])
        total_pop = int(data[1][2])
        pct_black = (black_pop / total_pop) * 100 if total_pop > 0 else 0
        return {"pct_black": round(pct_black, 2), "total_pop": total_pop}
    except Exception as e:
        print(f"Error getting demographics: {e}")
        return None


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

gmaps = googlemaps.Client(key=MAPS_KEY)

# Search parameters
search_origin = (38.9383, -76.8202) # Washington D.C.
radii_miles = [10, 12, 15, 17, 20]

# Load existing data
unique_courses, existing_metadata = load_from_gcs()
course_count = len(unique_courses)

# Check if existing data matches current search origin
existing_origin = existing_metadata.get("search_origin", {})
prev_lat = existing_origin.get("lat")
prev_lng = existing_origin.get("lng")
is_same_origin = (
    prev_lat is not None and prev_lng is not None and
    abs(prev_lat - search_origin[0]) < 1e-6 and
    abs(prev_lng - search_origin[1]) < 1e-6
)

print(f"Starting scan around {search_origin} with radii: {radii_miles} miles...")

for radius_mi in radii_miles:
    # Skip if this radius was already checked for this origin
    if (is_same_origin and 
        radius_mi in existing_metadata.get("radii_searched_miles", [])):
        print(f"\nSkipping radius: {radius_mi} miles (already checked for this origin).")
        continue

    radius_meters = int(radius_mi * 1609.34)
    print(f"\nScanning with radius: {radius_mi} miles ({radius_meters} meters)...")
    
    # Search for golf courses in a specific area
    # You can iterate this over coordinates of historically Black neighborhoods
    # Use the 'places' method for Text Search
    places_result = gmaps.places(
        query='golf courses',
        location=search_origin,
        radius=radius_meters
    )

    for place in places_result.get('results', []):
        place_id = place['place_id']
        if place_id not in unique_courses:
            course_count += 1
            unique_courses[place_id] = place
            print(f"New Course Found - Name: {place['name']}, Lat: {place['geometry']['location']['lat']}, Lng: {place['geometry']['location']['lng']}")
            
            # Fetch Census Data
            geo_info = get_census_tract(place['geometry']['location']['lat'], place['geometry']['location']['lng'])
            if geo_info:
                place['census_geoid'] = geo_info['geoid']
                stats = get_demographics(geo_info['state'], geo_info['county'], geo_info['tract'])
                if stats:
                    place['pct_black'] = stats['pct_black']
                    place['total_pop'] = stats['total_pop']
                    print(f"  - Neighborhood: {stats['pct_black']}% Black (Pop: {stats['total_pop']})")
                    if stats['pct_black'] > 50:
                        print("  - [INSIGHT]: Located in a Majority-Black Neighborhood.")
                else:
                    print("  - Demographic data unavailable.")
            else:
                print("  - Geocoding failed.")
        else:
            # Course already found in a smaller radius
            pass
    print(f"Courses found at {radius_mi} miles: {course_count}")

print(f"\nTotal unique courses found across all radii: {course_count}")

# Merge radii history if origin matches
# Merge radii history if origin matches
final_radii = list(set(radii_miles) | set(existing_metadata.get("radii_searched_miles", []))) if is_same_origin else radii_miles

# Export results to GCS
export_to_gcs(unique_courses, search_origin, sorted(final_radii))