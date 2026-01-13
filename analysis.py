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

def generate_plots(df, output_dir="static"):
    """
    Generates demographic analysis plots from a DataFrame and saves them to output_dir.
    Returns a list of saved filenames.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    saved_files = []

    if 'pct_black' not in df.columns:
        print("Error: 'pct_black' column not found in the DataFrame.")
        return []

    # Create Histogram
    plt.figure(figsize=(10, 6))
    plt.hist(df['pct_black'].dropna(), bins=20, color='skyblue', edgecolor='black')
    plt.title('Distribution of Black Population % Near Golf Courses')
    plt.xlabel('Percentage of Black Population (%)')
    plt.ylabel('Number of Golf Courses')
    plt.grid(axis='y', alpha=0.75)
    
    path_hist = os.path.join(output_dir, "demographic_distribution.png")
    plt.savefig(path_hist)
    plt.close()
    saved_files.append("demographic_distribution.png")
    
    # Create Binary Split Bar Plot (> 51% vs Not)
    plt.figure(figsize=(8, 6))
    majority_black = df[df['pct_black'] > 51].shape[0]
    not_majority_black = df[df['pct_black'] <= 51].shape[0]
    categories = ['Majority Black (> 51%)', 'Other (≤ 51%)']
    counts = [majority_black, not_majority_black]
    plt.bar(categories, counts, color=['salmon', 'lightgreen'], edgecolor='black')
    plt.title('Golf Courses by Neighborhood Demographics (Binary Split)')
    plt.ylabel('Number of Golf Courses')
    for i, v in enumerate(counts):
        plt.text(i, v + 0.5, str(v), ha='center', fontweight='bold')
    
    path_binary = os.path.join(output_dir, "demographic_split.png")
    plt.savefig(path_binary)
    plt.close()
    saved_files.append("demographic_split.png")

    # Cumulative Distance Plot
    if 'search_lat' in df.columns and 'search_lng' in df.columns:
        origin_lat = df['search_lat'].iloc[0]
        origin_lng = df['search_lng'].iloc[0]
        
        df['distance'] = df.apply(lambda row: haversine(origin_lat, origin_lng, row['lat'], row['lng']), axis=1)
        df['black_pop'] = (df['pct_black'] / 100) * df['total_pop']
        df_sorted = df.sort_values(by='distance').copy()
        df_sorted['cum_black_pop'] = df_sorted['black_pop'].cumsum()
        total_black_pop = df_sorted['black_pop'].sum()
        
        if total_black_pop > 0:
            df_sorted['relative_fraction'] = df_sorted['cum_black_pop'] / total_black_pop
            avg_dist = (df_sorted['distance'] * df_sorted['black_pop']).sum() / total_black_pop

            plt.figure(figsize=(10, 6))
            plt.plot(df_sorted['distance'], df_sorted['relative_fraction'], marker='o', linestyle='-', color='purple')
            plt.title('Cumulative Fraction of Black Population by Distance')
            plt.xlabel('Distance from Origin (miles)')
            plt.ylabel('Relative Fraction of Total Black Population')
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.ylim(0, 1.05)
            plt.axvline(x=avg_dist, color='red', linestyle='--', label=f'Avg Distance: {avg_dist:.2f} mi')
            plt.legend()
            
            path_cum = os.path.join(output_dir, "cumulative_distance_histogram.png")
            plt.savefig(path_cum)
            plt.close()
            saved_files.append("cumulative_distance_histogram.png")

    return saved_files

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
        generate_plots(df, output_dir=".")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    run_analysis()
