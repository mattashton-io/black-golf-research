import requests
import json
import os
import time
import zipfile
import io
import argparse
from datetime import datetime

# Configuration
BASE_URL = os.environ.get("TEST_BASE_URL", "http://localhost:8082")
ZCTA_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_zcta_national.zip"
OUTPUT_FILE = "comprehensive_test_report.md"

def get_all_zip_codes():
    """Downloads Census ZCTA list and extracts zip codes."""
    print("Fetching ZCTA list from Census Bureau...")
    try:
        resp = requests.get(ZCTA_URL, timeout=30)
        resp.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            # The file is typically named 2020_Gaz_zcta_national.txt
            filename = z.namelist()[0]
            with z.open(filename) as f:
                lines = f.readlines()
                # Skip header, get first column (GEOID which is the ZIP for ZCTA)
                # Note: The file is tab-delimited
                zips = []
                for line in lines[1:]:
                    parts = line.decode('utf-8').split('\t')
                    if parts:
                        zips.append(parts[0].strip())
                return zips
    except Exception as e:
        print(f"Error fetching zips: {e}")
        return ["20001", "10001", "90210", "60601", "30301"] # Fallback

def test_zip(zip_code):
    results = {
        "zip_code": zip_code,
        "search": "FAIL",
        "weather": "SKIP",
        "courses": 0,
        "errors": []
    }
    
    try:
        # Search
        start = time.time()
        resp = requests.post(f"{BASE_URL}/search", json={"zip_code": zip_code}, timeout=60)
        results["search_latency"] = round(time.time() - start, 2)
        
        if resp.status_code == 200:
            data = resp.json()
            results["search"] = "OK"
            results["courses"] = len(data.get("courses", []))
            
            # Test Weather and Enrichment on the first course
            if results["courses"] > 0:
                course = data["courses"][0]
                place_id = course.get("place_id")
                lat, lng = course["geometry"]["location"]["lat"], course["geometry"]["location"]["lng"]
                
                # Weather
                w_start = time.time()
                w_resp = requests.post(f"{BASE_URL}/get_weather", json={
                    "lat": lat, "lng": lng, "place_id": place_id, "zip_code": zip_code
                }, timeout=15)
                results["weather_latency"] = round(time.time() - w_start, 2)
                
                if w_resp.status_code == 200:
                    results["weather"] = "OK"
                else:
                    results["errors"].append(f"Weather error ({w_resp.status_code})")
                    results["weather"] = "FAIL"
                
                # Enrichment (AI Agent)
                e_start = time.time()
                e_resp = requests.post(f"{BASE_URL}/enrich_course", json={
                    "name": course["name"],
                    "address": course.get("formatted_address") or course.get("vicinity"),
                    "place_id": place_id,
                    "zip_code": zip_code
                }, timeout=30)
                results["enrich_latency"] = round(time.time() - e_start, 2)
                
                if e_resp.status_code == 200:
                    results["enrich"] = "OK"
                    e_data = e_resp.json()
                    if not e_data.get("phone") and not e_data.get("website"):
                        results["errors"].append("Enrichment returned empty data")
                else:
                    results["errors"].append(f"Enrichment error ({e_resp.status_code})")
                    results["enrich"] = "FAIL"
        else:
            results["errors"].append(f"Search error ({resp.status_code})")
    except Exception as e:
        results["errors"].append(str(e))
        
    return results

def run_tests(limit=None):
    zips = get_all_zip_codes()
    if limit:
        zips = zips[:limit]
        
    print(f"Starting tests for {len(zips)} zip codes...")
    all_results = []
    
    consecutive_errors = 0
    last_error = None
    
    for i, z in enumerate(zips):
        print(f"[{i+1}/{len(zips)}] Testing {z}...")
        result = test_zip(z)
        all_results.append(result)
        
        # Sequential Error Tracking
        current_error = result["errors"][0] if result["errors"] else None
        if current_error and current_error == last_error:
            consecutive_errors += 1
            if consecutive_errors >= 3:
                print(f"!!! CRITICAL: Same error encountered 3 times in a row: {current_error}")
                print("Stopping tests.")
                break
        else:
            consecutive_errors = 1 if current_error else 0
            last_error = current_error

        # Periodic report saving
        if (i + 1) % 10 == 0:
            generate_report(all_results, partial=True)
            
    generate_report(all_results)

def generate_report(results, partial=False):
    with open(OUTPUT_FILE, "w") as f:
        f.write(f"# Comprehensive Test Report {'(Partial)' if partial else ''}\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")
        
        success_search = len([r for r in results if r["search"] == "OK"])
        total = len(results)
        
        f.write("## Summary\n")
        f.write(f"- Total Zips: {total}\n")
        f.write(f"- Search Success: {success_search}/{total}\n")
        f.write(f"- Weather OK: {len([r for r in results if r.get('weather') == 'OK'])}/{total}\n")
        f.write(f"- Enrichment OK: {len([r for r in results if r.get('enrich') == 'OK'])}/{total}\n\n")
        
        f.write("## Detailed Log (Sample or Failures)\n")
        f.write("| Zip | Result | Latency (S/W/E) | Courses | Errors |\n")
        f.write("|-----|--------|-----------------|---------|--------|\n")
        for r in results:
            if r["search"] != "OK" or r.get("enrich") == "FAIL" or len(results) < 50:
                lats = f"{r.get('search_latency','?')}/{r.get('weather_latency','?')}/{r.get('enrich_latency','?')}"
                errs = "<br>".join(r["errors"]) if r["errors"] else ""
                f.write(f"| {r['zip_code']} | {r['search']} | {lats} | {r['courses']} | {errs} |\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Limit number of zips to test")
    args = parser.parse_args()
    
    run_tests(limit=args.limit)
