from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

def run_national_golf_analysis():
    client = bigquery.Client(project=project_id)
    
    # This query joins OSM Golf Courses with Census Demographic Data
    # Fix: Corrected CTE reference from 'census_stats' to 'census_tract_stats'
    query = """
    WITH golf_courses AS (
      -- Extracting all golf courses in the US from OSM
      SELECT 
        all_tags,
        ST_CENTROID(geometry) as course_point -- Using Centroid for more efficient spatial join
      FROM `bigquery-public-data.geo_openstreetmap.planet_features`
      WHERE EXISTS(SELECT 1 FROM UNNEST(all_tags) as tag WHERE tag.key = 'leisure' AND tag.value = 'golf_course')
    ),
    census_tract_stats AS (
      -- Getting demographic percentages from ACS 2020 5-Year
      SELECT 
        geo_id,
        (total_black / total_pop) * 100 as pct_black,
        total_pop
      FROM `bigquery-public-data.census_bureau_acs.censustract_2020_5yr`
      WHERE total_pop > 0
    ),
    tract_geoms AS (
      -- Getting tract boundaries
      SELECT geo_id, tract_geom 
      FROM `bigquery-public-data.geo_census_tracts.us_census_tracts_national`
    ),
    joined_data AS (
      -- Spatial Join: Find which tract each golf course centroid falls into
      SELECT 
        g.all_tags,
        c.pct_black,
        c.total_pop
      FROM golf_courses g
      JOIN tract_geoms t ON ST_INTERSECTS(g.course_point, t.tract_geom)
      JOIN census_tract_stats c ON t.geo_id = c.geo_id -- FIXED: Referenced correct CTE name
    )
    -- Aggregation for Research Insight
    SELECT 
      CASE 
        WHEN pct_black > 50 THEN 'Majority Black (>50%)'
        WHEN pct_black > 25 THEN 'Significant Black (25-50%)'
        ELSE 'Minimal Black (<25%)'
      END as demographic_category,
      COUNT(*) as course_count,
      ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) as percentage_of_total_us_courses
    FROM joined_data
    GROUP BY 1
    ORDER BY percentage_of_total_us_courses DESC
    """
    
    print(f"Running National Research Query in Project: {project_id}...")
    try:
        query_job = client.query(query)
        results = query_job.to_dataframe()
        
        print("\n--- National Research Results ---")
        print(results)
        return results
    except Exception as e:
        print(f"Error executing BigQuery: {e}")
        return None

if __name__ == "__main__":
    run_national_golf_analysis()