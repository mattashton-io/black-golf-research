import googlemaps
import os
from google.cloud import secretmanager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
secret_token_id = os.environ.get("SECRET_MAPS")
project_id=os.environ.get("GOOGLE_CLOUD_PROJECT")

# Use Secret Manager as per your best practices
def get_places_api_key():
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_token_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

gmaps = googlemaps.Client(key=get_places_api_key())

# Search for golf courses in a specific area
# You can iterate this over coordinates of historically Black neighborhoods
places_result = gmaps.places_search(
    query='golf courses',
    location=(38.9383, -76.8202), # Example: Washington D.C.
    radius=24000 # 15 miles
)

for place in places_result['results']:
    print(f"Name: {place['name']}, Lat: {place['geometry']['location']['lat']}")