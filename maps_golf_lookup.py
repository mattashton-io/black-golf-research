import googlemaps
import os
import json
import csv
import io
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
        # Get fieldnames from the first result
        sample_course = next(iter(courses_dict.values()))
        # Flattening simple fields for CSV
        fieldnames = ['name', 'address', 'lat', 'lng', 'place_id', 'rating', 'user_ratings_total']
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
                'user_ratings_total': course.get('user_ratings_total')
            })

    csv_blob = bucket.blob("golf_courses.csv")
    csv_blob.upload_from_string(output.getvalue(), content_type="text/csv")
    print(f"Successfully exported {len(courses_dict)} courses to gs://{secret_bucket_id}/golf_courses.csv")

gmaps = googlemaps.Client(key=get_places_api_key())

# Define radii to scan (in miles)
radii_miles = [10, 12, 15, 17, 20]
unique_courses = {}

print(f"Scanning for golf courses around Washington D.C. with radii: {radii_miles} miles...")

course_count = 0

for radius_mi in radii_miles:
    radius_meters = int(radius_mi * 1609.34)
    print(f"\nScanning with radius: {radius_mi} miles ({radius_meters} meters)...")
    
    # Search for golf courses in a specific area
    # You can iterate this over coordinates of historically Black neighborhoods
    # Use the 'places' method for Text Search
    places_result = gmaps.places(
        query='golf courses',
        location=(38.9383, -76.8202), # Washington D.C.
        radius=radius_meters
    )

    for place in places_result.get('results', []):
        place_id = place['place_id']
        if place_id not in unique_courses:
            course_count += 1
            unique_courses[place_id] = place
            print(f"New Course Found - Name: {place['name']}, Lat: {place['geometry']['location']['lat']}, Lng: {place['geometry']['location']['lng']}")
        else:
            # Course already found in a smaller radius
            pass
    print(f"Courses found at {radius_mi} miles: {course_count}")

print(f"\nTotal unique courses found across all radii: {course_count}")

# Export results to GCS
export_to_gcs(unique_courses)