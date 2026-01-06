import googlemaps
import os
import json
import csv
import io
import requests
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import bigquery
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
    except Exception:
        return None

def export_to_gcs(courses_dict):
    storage_client = storage.Client()
    bucket = storage_client.bucket(secret_bucket_id)

    # Export to JSON
    json_data = json.dumps(list(courses_dict.values()), indent=2)
    json_blob = bucket.blob("golf_courses.json")
    json_blob.upload_from_string(json_data, content_type="application/json")
    print(f"Successfully exported {len(courses_dict)} courses to gs://{secret_bucket_id}/golf_courses.json")

    # Export to CSV
    output = io.StringIO()
    if courses_dict:
        fieldnames = ['name', 'address', 'lat', 'lng', 'place_id', 'rating', 'user_ratings_total', 'census_geoid', 'pct_black', 'total_pop']
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for course in courses_dict.values():
            writer.writerow({
                'name': course.get('name'),
                'address': course.get('formatted_address', course.get('vicinity', '')),
                'lat': course['geometry']['location']['lat'],
                'lng': course['geometry']['location']['lng'],
                'place_id': course.get('place_id'),
                'rating': course.get('rating'),
                'user_ratings_total': course.get('user_ratings_total'),
                'census_geoid': course.get('census_geoid'),
                'pct_black': course.get('pct_black'),
                'total_pop': course.get('total_pop')
            })

    csv_blob = bucket.blob("golf_courses.csv")
    csv_blob.upload_from_string(output.getvalue(), content_type="text/csv")
    print(f"Successfully exported {len(courses_dict)} courses to gs://{secret_bucket_id}/golf_courses.csv")

def analyze_with_bigquery():
    """Run a BigQuery analysis on the CSV data stored in GCS."""
    client = bigquery.Client()
    uri = f"gs://{secret_bucket_id}/golf_courses.csv"
    
    # Configure an External Table to query the GCS file directly
    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [uri]
    external_config.options.skip_leading_rows = 1
    external_config.autodetect = True

    table_id = f"{project_id}.temp_golf_analysis.golf_courses_external"
    
    # Simple query to find courses in neighborhoods with > 20% Black population
    query = f"""
        SELECT name, pct_black, total_pop
        FROM `{table_id}`
        WHERE pct_black > 20
        ORDER BY pct_black DESC
    """
    
    print(f"\nRunning BigQuery analysis on {uri}...")
    try:
        # We use a temporary table reference for the query
        job_config = bigquery.QueryJobConfig(table_definitions={ "golf_courses_external": external_config })
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        print("\nBigQuery Results (>20% Black Population):")
        for row in results:
            print(f" - {row.name}: {row.pct_black}%")
    except Exception as e:
        print(f"BigQuery analysis failed: {e}")

gmaps = googlemaps.Client(key=MAPS_KEY)

# Define radii to scan (in miles)
radii_miles = [10] # Reduced for quick testing
unique_courses = {}

print(f"Scanning for golf courses around Washington D.C...")

for radius_mi in radii_miles:
    radius_meters = int(radius_mi * 1609.34)
    places_result = gmaps.places(
        query='golf courses',
        location=(38.9383, -76.8202),
        radius=radius_meters
    )

    for place in places_result.get('results', []):
        place_id = place['place_id']
        if place_id not in unique_courses:
            unique_courses[place_id] = place
            geo_info = get_census_tract(place['geometry']['location']['lat'], place['geometry']['location']['lng'])
            if geo_info:
                place['census_geoid'] = geo_info['geoid']
                stats = get_demographics(geo_info['state'], geo_info['county'], geo_info['tract'])
                if stats:
                    place['pct_black'] = stats['pct_black']
                    place['total_pop'] = stats['total_pop']

# Export to GCS
export_to_gcs(unique_courses)

# Analyze with BigQuery
analyze_with_bigquery()