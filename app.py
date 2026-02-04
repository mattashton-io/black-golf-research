import os
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_from_directory
from maps_golf_lookup import (
    search_golf_courses, 
    export_to_gcs, 
    load_from_gcs, 
    save_to_zip_cache,
    update_course_in_cache,
    get_demographics,
    get_maps_key,
    get_census_key
)
from analysis import generate_plots
import googlemaps
from google.cloud import bigquery
from dotenv import load_dotenv
from course_agent import enrich_course_details

#loads .env
load_dotenv()

app = Flask(__name__)

# Ensure static/plots exists
os.makedirs("static/plots", exist_ok=True)

# BigQuery client
bq_client = bigquery.Client()

# Geocoding client for zip code lookup
from maps_golf_lookup import API_TIMEOUT
# Client will be initialized lazily in routes to avoid startup block
_gmaps_client = None
def get_gmaps_client():
    global _gmaps_client
    if _gmaps_client is None:
        _gmaps_client = googlemaps.Client(key=get_maps_key(), timeout=API_TIMEOUT)
    return _gmaps_client

def calculate_wind_info(u, v):
    """Converts U and V components (m/s) to magnitude (mph), bearing, and cardinal direction."""
    import math
    if u is None or v is None:
        return None, None, "N/A"
    magnitude = math.sqrt(u**2 + v**2) * 2.23694 # m/s to mph
    # Direction: angle of the vector
    # atan2(u, v) gives the angle in radians from the positive Y axis (North)
    direction_deg = (math.atan2(u, v) * 180 / math.pi + 180) % 360
    
    cardinals = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    ix = round(direction_deg / (360 / len(cardinals))) % len(cardinals)
    return round(magnitude, 1), round(direction_deg, 0), cardinals[ix]

def get_weather_for_location(lat, lng):
    """Fetches latest weather stats from WeatherNext BigQuery and Google Maps Weather API."""
    import requests
    weather_stats = {
        "temperature": None,
        "feels_like": None,
        "humidity": None,
        "precipitation": None,
        "wind_speed": None,
        "wind_direction": "N/A",
        "wind_bearing": None
    }

    # 1. Fetch from Google Maps Weather API (Current humidity and feels-like)
    try:
        weather_url = f"https://weather.googleapis.com/v1/currentConditions:lookup"
        params = {
            "key": get_maps_key(),
            "location.latitude": lat,
            "location.longitude": lng
        }
        w_resp = requests.get(weather_url, params=params, timeout=5)
        if w_resp.status_code == 200:
            w_data = w_resp.json()
            # Save raw weather data for debugging as requested in plan/task.md
            try:
                os.makedirs("temp", exist_ok=True)
                with open("temp/last_weather_call.json", "w") as f:
                    json.dump(w_data, f, indent=2)
            except: pass

            if 'relativeHumidity' in w_data:
                weather_stats['humidity'] = w_data['relativeHumidity']
            if 'feelsLikeTemperature' in w_data:
                temp_c = w_data['feelsLikeTemperature'].get('degrees')
                if temp_c is not None:
                    weather_stats['feels_like'] = round((temp_c * 9/5) + 32, 0)
    except Exception as e:
        print(f"Maps Weather API Error: {e}")

    # 2. Fetch from WeatherNext BigQuery
    try:
        # Create a small bounding box (approx 1km) to optimize spatial join
        delta = 0.01 # ~ ±1km
        poly_wkt = f"POLYGON(({lng-delta} {lat-delta}, {lng+delta} {lat-delta}, {lng+delta} {lat-delta}, {lng-delta} {lat+delta}, {lng-delta} {lat-delta}))"
        
        table_id = "pytutoring-dev.weathernext_2.weathernext_2_0_0"
        query = f"""
        SELECT
            t2.time AS time,
            e.`2m_temperature`,
            e.`total_precipitation_6hr`,
            e.`10m_u_component_of_wind`,
            e.`10m_v_component_of_wind`
        FROM
            `{table_id}` AS t1, 
            t1.forecast as t2, 
            UNNEST(t2.ensemble) as e
        WHERE ST_INTERSECTS(t1.geography_polygon, ST_GEOGFROMTEXT('{poly_wkt}'))
          AND t1.init_time = TIMESTAMP('2025-10-03 00:00:00 UTC') 
        ORDER BY t2.time
        LIMIT 1
        """
        query_job = bq_client.query(query)
        results = query_job.result()
        
        for row in results:
            weather_stats['temperature'] = round((row['2m_temperature'] - 273.15) * 9/5 + 32, 0)
            weather_stats['precipitation'] = round(row['total_precipitation_6hr'] * 39.37, 2)
            
            wind_speed, wind_bearing, wind_dir = calculate_wind_info(
                row['10m_u_component_of_wind'], 
                row['10m_v_component_of_wind']
            )
            weather_stats['wind_speed'] = wind_speed
            weather_stats['wind_bearing'] = wind_bearing
            weather_stats['wind_direction'] = wind_dir
            
            return weather_stats
    except Exception as e:
        print(f"Weather BQ Error: {e}")
    
    return weather_stats if any(v is not None for v in weather_stats.values()) else None

@app.route('/')
def index():
    return render_template('index.html', maps_key=get_maps_key())

def upload_plots_to_gcs(zip_code, plot_files):
    """Uploads generated plot files to GCS under plots/{zip_code}/."""
    from google.cloud import storage
    from maps_golf_lookup import secret_bucket_id
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(secret_bucket_id)
        
        for filename in plot_files:
            local_path = os.path.join("static/plots", filename)
            if os.path.exists(local_path):
                # Target path in GCS: plots/{zip_code}/{filename}
                blob = bucket.blob(f"plots/{zip_code}/{filename}")
                blob.upload_from_filename(local_path)
                print(f"Uploaded {filename} to GCS for zip {zip_code}")
    except Exception as e:
        print(f"Error uploading plots to GCS for {zip_code}: {e}")

@app.route('/search', methods=['POST'])
def search():
    data = request.json
    zip_code = data.get('zip_code')
    target_black_majority = data.get('target_black_majority', True) # Default to True
    
    if not zip_code:
        return jsonify({"error": "No zip code provided"}), 400
    
    # 0. Check GCS Cache
    from maps_golf_lookup import load_from_gcs, save_to_zip_cache, secret_bucket_id
    from google.cloud import storage
    
    cached_data = load_from_gcs(zip_code=zip_code)
    if cached_data and isinstance(cached_data, dict):
        # Ensure cached data is complete for frontend and contains NEW demographics
        courses = cached_data.get('courses', [])
        # Strict validation based on plan/spec.md
        mandatory_fields = ['pct_poverty', 'holc_grade', 'barrier_to_entry', 'total_pop']
        is_complete = courses and all(all(f in c for f in mandatory_fields) for c in courses)
        
        cached_plots = cached_data.get('plots', [])
        
        if all(k in cached_data for k in ['lat', 'lng', 'courses', 'plots']) and is_complete:
            # NEW: Verify that the plots actually exist in GCS
            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(secret_bucket_id)
                all_plots_exist = True
                for plot_filename in cached_plots:
                    # GCS path: plots/{zip_code}/{filename}
                    blob = bucket.blob(f"plots/{zip_code}/{plot_filename}")
                    if not blob.exists():
                        print(f"Cache Invalidation: Plot {plot_filename} missing from GCS.")
                        all_plots_exist = False
                        break
                
                if all_plots_exist:
                    return jsonify(cached_data)
                else:
                    print(f"Incomplete plot cache found for {zip_code} in GCS, re-searching.")
            except Exception as e:
                print(f"Error validating GCS plot cache: {e}")
        else:
            print(f"Incomplete or stale cache found for {zip_code}, re-searching.")

    # 1. Geocode zip code
    try:
        geocode_result = get_gmaps_client().geocode(zip_code)
        if not geocode_result:
            return jsonify({"error": "Could not geocode zip code"}), 404
        
        location = geocode_result[0]['geometry']['location']
        lat, lng = location['lat'], location['lng']
    except Exception as e:
        return jsonify({"error": f"Geocoding error: {str(e)}"}), 500
    
    # 2. Search golf courses (default radius: 10 miles)
    radii = [10]
    courses_dict = search_golf_courses(lat, lng, radii, target_black_majority=target_black_majority)
    
    if not courses_dict:
        result = {
            "lat": lat,
            "lng": lng,
            "courses": [],
            "message": "No golf courses found in this area.",
            "plots": []
        }
        return jsonify(result)
    
    # 3. Convert to DataFrame for analysis
    courses_list = list(courses_dict.values())
    df_data = []
    for c in courses_list:
        df_data.append({
            'name': c.get('name'),
            'lat': c['geometry']['location']['lat'],
            'lng': c['geometry']['location']['lng'],
            'pct_black': c.get('pct_black', 0),
            'pct_poverty': c.get('pct_poverty', 0),
            'is_plurality_black': c.get('is_plurality_black', False),
            'holc_grade': c.get('holc_grade', 'N/A'),
            'barrier_to_entry': c.get('barrier_to_entry', 'Unknown'),
            'total_pop': c.get('total_pop', 0),
            'search_lat': lat,
            'search_lng': lng
        })
    df = pd.DataFrame(df_data)
    
    # 4. Generate plots (Both themes)
    light_plots = generate_plots(df, output_dir="static/plots", zip_code=zip_code, dark_mode=False)
    dark_plots = generate_plots(df, output_dir="static/plots", zip_code=zip_code, dark_mode=True)
    
    # Synchronize plots to GCS
    upload_plots_to_gcs(zip_code, light_plots + dark_plots)
    
    result = {
        "lat": lat,
        "lng": lng,
        "courses": courses_list,
        "plots": light_plots, # Cache just the basename-style list (logic handles suffixes)
        "message": f"Found {len(courses_list)} courses."
    }
    
    # 5. Cache results to GCS
    save_to_zip_cache(zip_code, result)
    
    return jsonify(result)

@app.route('/get_weather', methods=['POST'])
def get_weather():
    data = request.json
    lat = data.get('lat')
    lng = data.get('lng')
    if lat is None or lng is None:
        return jsonify({"error": "Missing lat/lng"}), 400
    
    weather_stats = get_weather_for_location(lat, lng)
    if weather_stats:
        zip_code = data.get('zip_code') # Optional zip from front-end
        place_id = data.get('place_id')
        if zip_code and place_id:
            update_course_in_cache(zip_code, place_id, {"cached_weather": weather_stats})
        return jsonify(weather_stats)
    return jsonify({"error": "Weather data unavailable"}), 404

@app.route('/enrich_course', methods=['POST'])
def enrich_course():
    data = request.json
    name = data.get('name')
    address = data.get('address')
    place_id = data.get('place_id')
    zip_code = data.get('zip_code')
    
    if not name or not address:
        return jsonify({"error": "Missing name or address"}), 400
    
    enrichment = enrich_course_details(name, address)
    
    # Cache enrichment results if metadata provided
    if zip_code and place_id:
        updates = {}
        if enrichment.get('phone'): updates['formatted_phone_number'] = enrichment['phone']
        if enrichment.get('website'): updates['website'] = enrichment['website']
        if updates:
            update_course_in_cache(zip_code, place_id, updates)
            
    return jsonify(enrichment)

def recreate_theme_plots(zip_code, dark_mode=False):
    """Reconstructs DataFrame from cache and generates plots for a specific theme."""
    from maps_golf_lookup import load_from_gcs
    print(f"Recreating {'dark' if dark_mode else 'light'} plots for {zip_code} from cache...")
    
    cached_data = load_from_gcs(zip_code=zip_code)
    if not cached_data or 'courses' not in cached_data:
        print(f"Failed to recreate plots: No cache found for {zip_code}")
        return []
    
    courses_list = cached_data['courses']
    df_data = []
    for c in courses_list:
        df_data.append({
            'name': c.get('name'),
            'lat': c['geometry']['location']['lat'],
            'lng': c['geometry']['location']['lng'],
            'pct_black': c.get('pct_black', 0),
            'is_plurality_black': c.get('is_plurality_black', False),
            'total_pop': c.get('total_pop', 0),
            'search_lat': cached_data.get('lat'),
            'search_lng': cached_data.get('lng'),
            'median_income': c.get('median_income')
        })
    df = pd.DataFrame(df_data)
    
    # Generate requested theme plots
    new_plots = generate_plots(df, output_dir="static/plots", zip_code=zip_code, dark_mode=dark_mode)
    
    # Sync to GCS
    upload_plots_to_gcs(zip_code, new_plots)
    return new_plots

@app.route('/search_plot/<path:filename>')
def serve_plot(filename):
    """Serves a plot file, downloading it from GCS if not found locally, 
    or regenerating it if missing from GCS."""
    local_path = os.path.join('static/plots', filename)
    
    if not os.path.exists(local_path):
        print(f"Plot {filename} not found locally. Attempting GCS download...")
        # Filename format: {zip_code}_{plot_type}(_dark).png
        try:
            name_parts = filename.split('_')
            zip_code = name_parts[0]
            is_dark = "_dark.png" in filename
            
            if zip_code:
                from google.cloud import storage
                from maps_golf_lookup import secret_bucket_id
                storage_client = storage.Client()
                bucket = storage_client.bucket(secret_bucket_id)
                
                # GCS path: plots/{zip_code}/{filename}
                blob = bucket.blob(f"plots/{zip_code}/{filename}")
                if blob.exists():
                    os.makedirs('static/plots', exist_ok=True)
                    blob.download_to_filename(local_path)
                    print(f"Successfully downloaded {filename} from GCS.")
                else:
                    print(f"Plot {filename} not found in GCS. Attempting on-demand generation...")
                    recreate_theme_plots(zip_code, dark_mode=is_dark)
                    # Local path should now exist
                    if not os.path.exists(local_path):
                        print(f"Failed to generate {filename} on-demand.")
        except Exception as e:
            print(f"Error serving/generating plot: {e}")
            
    return send_from_directory('static/plots', filename)

@app.route('/state_tracts', methods=['GET'])
def state_tracts():
    """Fetches all census tracts for a given state with demographic info."""
    state_fips = request.args.get('state')
    if not state_fips:
        return jsonify({"error": "Missing state FIPS code"}), 400
    
    # Fetch all tracts in the state
    vars = "NAME,B01003_001E,B02001_003E,B17001_001E,B17001_002E"
    base_url = f"https://api.census.gov/data/2022/acs/acs5"
    census_key = get_census_key()
    if not census_key:
        # Fallback for testing environment if Secret Manager is down
        census_key = os.environ.get("CENSUS_API_KEY") 
        
    params = { "get": vars, "for": "tract:*", "in": f"state:{state_fips}", "key": census_key }
    
    try:
        import requests
        response = requests.get(base_url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            # Convert to list of dicts
            header = data[0]
            rows = data[1:]
            results = []
            for r in rows:
                item = dict(zip(header, r))
                total_pop = int(item['B01003_001E']) if item['B01003_001E'] else 0
                black_pop = int(item['B02001_003E']) if item['B02001_003E'] else 0
                pov_total = int(item['B17001_001E']) if item['B17001_001E'] else 0
                below_pov = int(item['B17001_002E']) if item['B17001_002E'] else 0
                
                results.append({
                    "name": item['NAME'],
                    "tract": item['tract'],
                    "county": item['county'],
                    "total_pop": total_pop,
                    "pct_black": round((black_pop / total_pop) * 100, 2) if total_pop > 0 else 0,
                    "pct_poverty": round((below_pov / pov_total) * 100, 2) if pov_total > 0 else 0
                })
            return jsonify({"state": state_fips, "count": len(results), "tracts": results})
        else:
            return jsonify({"error": f"Census API error: {response.status_code}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
