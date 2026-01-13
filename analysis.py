import os
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import storage
from dotenv import load_dotenv
import io
import math

def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points in miles."""
    R = 3958.8  # Earth radius in miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def run_analysis():
    # Load environment variables
    load_dotenv()
    bucket_name = os.environ.get("SECRET_BUCKET")
    
    if not bucket_name:
        print("Error: SECRET_BUCKET environment variable not set.")
        return

    print(f"Connecting to GCS bucket: {bucket_name}...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("golf_courses.csv")
        
        if not blob.exists():
            print("Error: golf_courses.csv not found in the bucket.")
            return
            
        content = blob.download_as_text()
        print("Successfully downloaded data from GCS.")
        
        # Load into pandas
        df = pd.read_csv(io.StringIO(content))
        print(f"Loaded {len(df)} records into DataFrame.")
        
        if 'pct_black' not in df.columns:
            print("Error: 'pct_black' column not found in the CSV.")
            return
            
        # Create Histogram
        plt.figure(figsize=(10, 6))
        plt.hist(df['pct_black'].dropna(), bins=20, color='skyblue', edgecolor='black')
        plt.title('Distribution of Black Population % Near Golf Courses')
        plt.xlabel('Percentage of Black Population (%)')
        plt.ylabel('Number of Golf Courses')
        plt.grid(axis='y', alpha=0.75)
        
        # Save the plot
        output_hist = "demographic_distribution.png"
        plt.savefig(output_hist)
        print(f"Histogram saved as {output_hist}")
        
        # Create Binary Split Bar Plot (> 51% vs Not)
        plt.figure(figsize=(8, 6))
        
        # Calculate split
        majority_black = df[df['pct_black'] > 51].shape[0]
        not_majority_black = df[df['pct_black'] <= 51].shape[0]
        
        categories = ['Majority Black (> 51%)', 'Other (≤ 51%)']
        counts = [majority_black, not_majority_black]
        
        plt.bar(categories, counts, color=['salmon', 'lightgreen'], edgecolor='black')
        plt.title('Golf Courses by Neighborhood Demographics (Binary Split)')
        plt.ylabel('Number of Golf Courses')
        
        # Add labels on top of bars
        for i, v in enumerate(counts):
            plt.text(i, v + 0.5, str(v), ha='center', fontweight='bold')
            
        # Save binary plot
        output_binary = "demographic_split.png"
        plt.savefig(output_binary)
        print(f"Binary split plot saved as {output_binary}")

        # --- New Analysis: Cumulative Distance Plot ---
        print("Calculating distances and cumulative population distribution...")
        
        # Determine search origin (take from first record)
        origin_lat = df['search_lat'].iloc[0]
        origin_lng = df['search_lng'].iloc[0]
        
        # Calculate distance for each course
        df['distance'] = df.apply(lambda row: haversine(origin_lat, origin_lng, row['lat'], row['lng']), axis=1)
        
        # Calculate Black population for each tract
        df['black_pop'] = (df['pct_black'] / 100) * df['total_pop']
        
        # Sort by distance
        df_sorted = df.sort_values(by='distance').copy()
        
        # Calculate cumulative sums
        df_sorted['cum_black_pop'] = df_sorted['black_pop'].cumsum()
        total_black_pop = df_sorted['black_pop'].sum()
        df_sorted['relative_fraction'] = df_sorted['cum_black_pop'] / total_black_pop
        
        # Calculate Average Black Population Distance
        avg_dist = (df_sorted['distance'] * df_sorted['black_pop']).sum() / total_black_pop
        print(f"Average Black Population Distance from origin: {avg_dist:.22f} miles")

        # Create Plot
        plt.figure(figsize=(10, 6))
        plt.plot(df_sorted['distance'], df_sorted['relative_fraction'], marker='o', linestyle='-', color='purple')
        plt.title('Cumulative Fraction of Black Population by Distance from Search Origin')
        plt.xlabel('Distance from Origin (miles)')
        plt.ylabel('Relative Fraction of Total Black Population')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.ylim(0, 1.05)
        
        # Add a vertical line for the average distance
        plt.axvline(x=avg_dist, color='red', linestyle='--', label=f'Avg Distance: {avg_dist:.2f} mi')
        plt.legend()

        output_cum = "cumulative_distance_histogram.png"
        plt.savefig(output_cum)
        print(f"Cumulative distance plot saved as {output_cum}")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    run_analysis()
