import requests
import json
import os

BASE_URL = "http://localhost:8082"

def test_state_tracts():
    print("\nTesting /state_tracts route...")
    try:
        # Testing with DC (FIPS 11)
        resp = requests.get(f"{BASE_URL}/state_tracts?state=11", timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            print(f"SUCCESS: Found {data.get('count')} tracts in DC.")
            if data.get('tracts'):
                sample = data['tracts'][0]
                print(f"Sample tract: {sample['name']}")
                print(f"Metrics: {sample['pct_black']}% Black, {sample['pct_poverty']}% Poverty")
        else:
            print(f"FAILED: /state_tracts returned {resp.status_code}")
            print(resp.text)
    except Exception as e:
        print(f"ERROR testing /state_tracts: {e}")

def test_search_metrics():
    print("\nTesting /search with new metrics...")
    try:
        # Testing with a DC zip code
        resp = requests.post(f"{BASE_URL}/search", json={"zip_code": "20001"}, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            courses = data.get('courses', [])
            if courses:
                print(f"SUCCESS: Found {len(courses)} courses.")
                course = courses[0]
                print(f"Course: {course['name']}")
                print(f"Poverty Rate: {course.get('pct_poverty')}%")
                print(f"Barrier to Entry: {course.get('barrier_to_entry')}")
                print(f"HOLC Grade: {course.get('holc_grade')}")
            else:
                print("Note: No courses found for 20001 in search.")
        else:
            print(f"FAILED: /search returned {resp.status_code}")
            print(resp.text)
    except Exception as e:
        print(f"ERROR testing /search: {e}")

if __name__ == "__main__":
    # Note: These tests require the server to be running on localhost:8082
    # and valid Census/Google API keys configured.
    test_state_tracts()
    test_search_metrics()
