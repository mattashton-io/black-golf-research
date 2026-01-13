import os
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_from_directory
from maps_golf_lookup import search_golf_courses, MAPS_KEY
from analysis import generate_plots
import googlemaps
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Ensure static/plots exists
os.makedirs("static/plots", exist_ok=True)

# Geocoding client for zip code lookup
gmaps_client = googlemaps.Client(key=MAPS_KEY)

@app.route('/')
def index():
    return render_template('index.html', maps_key=MAPS_KEY)

@app.route('/search', methods=['POST'])
def search():
    data = request.json
    zip_code = data.get('zip_code')
    
    if not zip_code:
        return jsonify({"error": "No zip code provided"}), 400
    
    # 1. Geocode zip code
    try:
        geocode_result = gmaps_client.geocode(zip_code)
        if not geocode_result:
            return jsonify({"error": "Could not geocode zip code"}), 404
        
        location = geocode_result[0]['geometry']['location']
        lat, lng = location['lat'], location['lng']
    except Exception as e:
        return jsonify({"error": f"Geocoding error: {str(e)}"}), 500
    
    # 2. Search golf courses (radii: 10, 20 miles)
    radii = [10, 20]
    courses_dict = search_golf_courses(lat, lng, radii)
    
    if not courses_dict:
        return jsonify({
            "lat": lat,
            "lng": lng,
            "courses": [],
            "message": "No golf courses found in this area."
        })
    
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
    
    return jsonify({
        "lat": lat,
        "lng": lng,
        "courses": courses_list,
        "plots": plot_files
    })

@app.route('/search_plot/<path:filename>')
def serve_plot(filename):
    return send_from_directory('static/plots', filename)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8082))
    app.run(host='0.0.0.0', port=port, debug=True)
