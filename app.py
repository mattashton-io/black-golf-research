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

def get_weather_for_location(lat, lng):
    """Fetches latest temperature from WeatherNext BigQuery dataset."""
    try:
        query = f"""
        SELECT
            e.`2m_temperature` - 273.15 AS temperature_c
        FROM
            `pytutoring-dev.weathernext_2.weathernext_2_0_0` AS t1, 
            t1.forecast as t2, 
            UNNEST(t2.ensemble) as e
        WHERE ST_INTERSECTS(t1.geography_polygon, ST_GEOGPOINT({lng}, {lat}))
          AND t1.init_time = (SELECT MAX(init_time) FROM `pytutoring-dev.weathernext_2.weathernext_2_0_0`)
        ORDER BY t2.time
        LIMIT 1
        """
        query_job = bq_client.query(query)
        results = query_job.to_dataframe()
        if not results.empty:
            return round(results['temperature_c'].iloc[0], 1)
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
        # Ensure cached data is complete for frontend
        if all(k in cached_data for k in ['lat', 'lng', 'courses', 'plots']):
            return jsonify(cached_data)
        else:
            print(f"Incomplete cache found for {zip_code}, re-searching.")

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
    
    temp = get_weather_for_location(lat, lng)
    return jsonify({"temperature": temp})

@app.route('/search_plot/<path:filename>')
def serve_plot(filename):
    return send_from_directory('static/plots', filename)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8082))
    app.run(host='0.0.0.0', port=port, debug=True)
