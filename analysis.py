import os
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import storage
from dotenv import load_dotenv
import io
import math
import matplotlib.patheffects as path_effects

# Task 3: Global Matplotlib Styling
plt.rcParams['font.size'] = 12
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Liberation Sans', 'Arial', 'DejaVu Sans']
plt.rcParams['axes.linewidth'] = 1.5
plt.rcParams['axes.edgecolor'] = '#1A1A1A'
plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False
plt.rcParams['grid.alpha'] = 0
plt.rcParams['text.color'] = '#1A1A1A'

PARCHMENT = '#E8D9C5'
DUB_INDIGO = '#2C3E75'
DUB_RED = '#D22030'
DUB_GOLD = '#E2A62C'
GOLF_GREEN = '#006442'
INK_BLACK = '#1A1A1A'

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
    plt.hist(df['pct_black'].dropna(), bins=20, color=DUB_INDIGO, edgecolor='black', linewidth=1.5)
    plt.title('DISTRIBUTION OF BLACK POPULATION %', fontweight='bold')
    plt.xlabel('PERCENTAGE (%)')
    plt.ylabel('COURSES')
    
    path_hist = os.path.join(output_dir, "demographic_distribution.png")
    plt.savefig(path_hist, facecolor=PARCHMENT)
    plt.close()
    saved_files.append("demographic_distribution.png")
    
    # Create Binary + Plurality Split Bar Plot
    plt.figure(figsize=(8, 6))
    
    majority_black = df[df['pct_black'] > 50].shape[0]
    # Plurality is True AND not Majority (>50)
    plurality_black = df[
        (df['is_plurality_black'] == True) & 
        (df['pct_black'] <= 50)
    ].shape[0]
    
    # Other is everything else
    other_courses = len(df) - majority_black - plurality_black
    
    categories = ['Majority Black', 'Plurality Black', 'Other']
    counts = [majority_black, plurality_black, other_courses]
    colors = [DUB_RED, DUB_GOLD, GOLF_GREEN]
    
    bars = plt.bar(categories, counts, color=colors, edgecolor='black', linewidth=1.5)
    plt.title('NEIGHBORHOOD DEMOGRAPHICS', fontweight='bold')
    plt.ylabel('COURSES')
    
    for bar, count in zip(bars, counts):
        if count > 0:
            text = plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                            str(count), ha='center', fontweight='bold', color=INK_BLACK)
            text.set_path_effects([path_effects.withStroke(linewidth=3, foreground='white')])
    
    path_binary = os.path.join(output_dir, "demographic_split.png")
    plt.savefig(path_binary, facecolor=PARCHMENT)
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
            plt.savefig(path_cum, facecolor=PARCHMENT)
            plt.close()
            saved_files.append("cumulative_distance_histogram.png")

    # Task 5: Horizontal Comparative Bar
    plt.figure(figsize=(10, 3))
    avg_black_pct = 13.5 # National Average approx
    local_avg_pct = df['pct_black'].mean()
    
    plt.barh(['National Average', 'Local Results'], [avg_black_pct, local_avg_pct], 
             color=[DUB_GOLD, INK_BLACK], edgecolor='black', linewidth=1.5)
    plt.title('LOCAL VS NATIONAL DEMOGRAPHIC COMPARISON', fontweight='bold')
    plt.xlim(0, max(avg_black_pct, local_avg_pct) * 1.2)
    for i, v in enumerate([avg_black_pct, local_avg_pct]):
        plt.text(v + 0.5, i, f"{v:.1f}%", va='center', fontweight='bold')
    
    path_comp = os.path.join(output_dir, "comparative_bar.png")
    plt.savefig(path_comp, facecolor=PARCHMENT)
    plt.close()
    saved_files.append("comparative_bar.png")

    # Task 8: The Research Funnel Pyramid
    plt.figure(figsize=(8, 6))
    layers = ['ZIPS SCANNED', 'COURSES FOUND', 'MAJORITY BLACK']
    # If the user only scanned one zip, we set it to 1. 
    # In a real loop, we'd pass the actual count.
    counts = [1, len(df), len(df[df['pct_black'] > 50])]
    colors = [DUB_INDIGO, DUB_RED, DUB_GOLD]
    
    for i, (layer, count, color) in enumerate(zip(layers, counts, colors)):
        width = 1.0 - (i * 0.2)
        plt.barh(i, width, color=color, edgecolor='black', linewidth=1.5)
        text = plt.text(0, i, f"{layer}\n({count})", ha='center', va='center', color='black', fontweight='bold')
        text.set_path_effects([path_effects.withStroke(linewidth=3, foreground='white')])
    
    plt.axis('off')
    plt.title('THE RESEARCH FUNNEL', fontweight='bold', pad=20)
    
    # Task 6: Scaled Progression Circles
    plt.figure(figsize=(6, 8))
    radii_mi = [10, 15, 25]
    colors_circ = [PARCHMENT, DUB_GOLD, DUB_RED, DUB_INDIGO]
    
    # Calculate counts for each radius
    if 'distance' not in df.columns and 'search_lat' in df.columns:
        origin_lat = df['search_lat'].iloc[0]
        origin_lng = df['search_lng'].iloc[0]
        df['distance'] = df.apply(lambda row: haversine(origin_lat, origin_lng, row['lat'], row['lng']), axis=1)
    
    if 'distance' in df.columns:
        counts_at_radii = [len(df[df['distance'] <= r]) for r in radii_mi]
        # Normalize radii for visualization
        max_c = max(counts_at_radii) if counts_at_radii else 1
        vis_radii = [ (c/max_c)**0.5 for c in counts_at_radii ]
        
        for i, (v_r, c, r_mi) in enumerate(zip(vis_radii, counts_at_radii, radii_mi)):
            circle = plt.Circle((0, -i*2.5), v_r, color=colors_circ[i % len(colors_circ)], ec='black', lw=1.5)
            plt.gca().add_artist(circle)
            plt.text(0, -i*2.5, f"{r_mi}mi\n{c} Courses", ha='center', va='center', fontweight='bold')
            
        plt.xlim(-1.5, 1.5)
        plt.ylim(-7, 1.5)
        plt.axis('off')
        plt.title('SCALED SEARCH PROGRESSION', fontweight='bold')
        
        path_circles = os.path.join(output_dir, "progression_circles.png")
        plt.savefig(path_circles, facecolor=PARCHMENT)
        plt.close()
        saved_files.append("progression_circles.png")

    # Task 7: Stacked Income Correlation Bar
    if 'median_income' in df.columns:
        plt.figure(figsize=(10, 4))
        # Create income brackets
        bins = [0, 50000, 100000, 150000, float('inf')]
        labels = ['<$50k', '$50k-100k', '$100k-150k', '$150k+']
        df['income_bracket'] = pd.cut(df['median_income'], bins=bins, labels=labels)
        
        income_counts = df['income_bracket'].value_counts().reindex(labels).fillna(0)
        
        plt.barh(labels, income_counts, color=[DUB_INDIGO, DUB_RED, DUB_GOLD, GOLF_GREEN], 
                 edgecolor='black', linewidth=1.5)
        plt.title('MEDIAN INCOME CORRELATION', fontweight='bold')
        plt.xlabel('COURSE COUNT')
        
        path_income = os.path.join(output_dir, "income_correlation.png")
        plt.savefig(path_income, facecolor=PARCHMENT)
        plt.close()
        saved_files.append("income_correlation.png")

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
        generate_plots(df, output_dir="static/plots")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    run_analysis()
