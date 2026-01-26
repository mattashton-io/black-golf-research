import os
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_from_directory
from maps_golf_lookup import (
    search_golf_courses, 
    MAPS_KEY, 
    export_to_gcs, 
    load_from_gcs, 
    save_to_zip_cache,
    update_course_in_cache
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
gmaps_client = googlemaps.Client(key=MAPS_KEY, timeout=API_TIMEOUT)

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
            "key": MAPS_KEY,
            "location.latitude": lat,
            "location.longitude": lng
        }
        w_resp = requests.get(weather_url, params=params, timeout=5)
        if w_resp.status_code == 200:
            w_data = w_resp.json()
            # Save raw weather data for debugging as requested in task.md
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
    return render_template('index.html', maps_key=MAPS_KEY)

@app.route('/search', methods=['POST'])
def search():
    data = request.json
    zip_code = data.get('zip_code')
    target_black_majority = data.get('target_black_majority', True) # Default to True
    
    if not zip_code:
        return jsonify({"error": "No zip code provided"}), 400
    
    # 0. Check GCS Cache
    from maps_golf_lookup import load_from_gcs, save_to_zip_cache
    cached_data = load_from_gcs(zip_code=zip_code)
    if cached_data and isinstance(cached_data, dict):
        # Ensure cached data is complete for frontend and contains NEW demographics
        courses = cached_data.get('courses', [])
        has_new_demographics = courses and 'pct_white' in courses[0]
        
        if all(k in cached_data for k in ['lat', 'lng', 'courses', 'plots']) and has_new_demographics:
            return jsonify(cached_data)
        else:
            print(f"Incomplete or stale cache found for {zip_code}, re-searching.")

    # 1. Geocode zip code
    try:
        geocode_result = gmaps_client.geocode(zip_code)
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
            'total_pop': c.get('total_pop', 0),
            'search_lat': lat,
            'search_lng': lng
        })
    df = pd.DataFrame(df_data)
    
    # 4. Generate plots
    plot_files = generate_plots(df, output_dir="static/plots")
    
    result = {
        "lat": lat,
        "lng": lng,
        "courses": courses_list,
        "plots": plot_files,
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

@app.route('/search_plot/<path:filename>')
def serve_plot(filename):
    return send_from_directory('static/plots', filename)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8082))
    app.run(host='0.0.0.0', port=port)
