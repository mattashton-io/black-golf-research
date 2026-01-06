import googlemaps
import os
from google.cloud import secretmanager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
secret_token_id = os.environ.get("SECRET_PLACES")
project_id=os.environ.get("GOOGLE_CLOUD_PROJECT")

# Use Secret Manager as per your best practices
def get_places_api_key():
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_token_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

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