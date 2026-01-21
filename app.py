import os
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_from_directory
from maps_golf_lookup import search_golf_courses, MAPS_KEY
from analysis import generate_plots
import googlemaps
from google.cloud import bigquery
from dotenv import load_dotenv

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
    """Converts U and V components (m/s) to magnitude (mph) and cardinal direction."""
    import math
    if u is None or v is None:
        return None, "N/A"
    magnitude = math.sqrt(u**2 + v**2) * 2.23694 # m/s to mph
    # Direction: angle of the vector
    # atan2(u, v) gives the angle in radians from the positive Y axis (North)
    # We want the direction the wind is COMING FROM for meteorology, but 
    # WeatherNext U/V usually represents the vector direction (wind blowing TO).
    # However, for consistency with common dashboards, blowing direction is fine.
    # Standard: (atan2(u, v) * 180 / pi + 180) % 360
    direction_deg = (math.atan2(u, v) * 180 / math.pi + 180) % 360
    
    cardinals = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    ix = round(direction_deg / (360 / len(cardinals))) % len(cardinals)
    return round(magnitude, 1), cardinals[ix]

def get_weather_for_location(lat, lng):
    """Fetches latest weather stats from WeatherNext BigQuery dataset."""
    try:
        # Create a small bounding box (approx 1km) to optimize spatial join
        delta = 0.01 # ~ ±1km
        poly_wkt = f"POLYGON(({lng-delta} {lat-delta}, {lng+delta} {lat-delta}, {lng+delta} {lat+delta}, {lng-delta} {lat+delta}, {lng-delta} {lat-delta}))"
        
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
            temp_f = round((row['2m_temperature'] - 273.15) * 9/5 + 32, 0)
            precip_in = round(row['total_precipitation_6hr'] * 39.37, 2)
            
            wind_speed, wind_dir = calculate_wind_info(
                row['10m_u_component_of_wind'], 
                row['10m_v_component_of_wind']
            )
            
            return {
                "temperature": temp_f,
                "precipitation": precip_in,
                "wind_speed": wind_speed,
                "wind_direction": wind_dir,
                "humidity": None # Placeholder for missing variable
            }
    except Exception as e:
        print(f"Weather BQ Error: {e}")
    return None

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
        return jsonify(weather_stats)
    return jsonify({"error": "Weather data unavailable"}), 404

@app.route('/search_plot/<path:filename>')
def serve_plot(filename):
    return send_from_directory('static/plots', filename)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8082))
    app.run(host='0.0.0.0', port=port)
