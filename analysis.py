import os
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import storage
from dotenv import load_dotenv
import io
import math

# Task 3: Global Matplotlib Styling
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Liberation Sans', 'Arial', 'DejaVu Sans']
plt.rcParams['font.size'] = 12
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

def generate_plots(df, output_dir="static", zip_code=None, dark_mode=False):
    """
    Generates demographic analysis plots from a DataFrame and saves them to output_dir.
    Returns a list of saved filenames.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    saved_files = []
    prefix = f"{zip_code}_" if zip_code else ""
    suffix = "_dark" if dark_mode else ""
    
    # Theme configuration
    bg_color = '#1a1b1e' if dark_mode else PARCHMENT
    text_color = '#ced4da' if dark_mode else INK_BLACK
    edge_color = '#373a40' if dark_mode else INK_BLACK
    
    # Update global style for this run
    plt.rcParams['text.color'] = text_color
    plt.rcParams['axes.labelcolor'] = text_color
    plt.rcParams['axes.edgecolor'] = edge_color
    plt.rcParams['xtick.color'] = text_color
    plt.rcParams['ytick.color'] = text_color

    if 'pct_black' not in df.columns:
        print("Error: 'pct_black' column not found in the DataFrame.")
        return []

    # Create Histogram
    plt.figure(figsize=(10, 6))
    plt.hist(df['pct_black'].dropna(), bins=20, color=DUB_INDIGO, edgecolor='black', linewidth=1.5)
    plt.title('DISTRIBUTION OF BLACK POPULATION %', fontweight='bold', color=text_color)
    plt.xlabel('PERCENTAGE (%)', color=text_color)
    plt.ylabel('COURSES', color=text_color)
    
    filename_hist = f"{prefix}demographic_distribution{suffix}.png"
    path_hist = os.path.join(output_dir, filename_hist)
    plt.savefig(path_hist, facecolor=bg_color, edgecolor='none')
    plt.close()
    saved_files.append(filename_hist)
    
    # Create Binary Split Bar Plot (> 50% vs Plurality vs Other)
    plt.figure(figsize=(10, 6))
    majority_black = df[df['pct_black'] > 50].shape[0]
    # Plurality is when it's NOT majority but is_plurality_black is True
    if 'is_plurality_black' in df.columns:
        plurality_black = df[(df['pct_black'] <= 50) & (df['is_plurality_black'] == True)].shape[0]
        other = df[(df['pct_black'] <= 50) & (df['is_plurality_black'] == False)].shape[0]
        categories = ['Majority Black (>50%)', 'Plurality Black', 'Other']
        counts = [majority_black, plurality_black, other]
        colors = [DUB_RED, DUB_GOLD, GOLF_GREEN]
    else:
        not_majority_black = df[df['pct_black'] <= 50].shape[0]
        categories = ['Majority Black (> 50%)', 'Other (≤ 50%)']
        counts = [majority_black, not_majority_black]
        colors = [DUB_RED, GOLF_GREEN]

    plt.bar(categories, counts, color=colors, edgecolor='black', linewidth=1.5)
    plt.title('NEIGHBORHOOD DEMOGRAPHIC SPLIT', fontweight='bold', color=text_color)
    plt.ylabel('COURSES', color=text_color)
    for i, v in enumerate(counts):
        plt.text(i, v + 0.1, str(v), ha='center', fontweight='bold', color=text_color)
    
    filename_binary = f"{prefix}demographic_split{suffix}.png"
    path_binary = os.path.join(output_dir, filename_binary)
    plt.savefig(path_binary, facecolor=bg_color, edgecolor='none')
    plt.close()
    saved_files.append(filename_binary)

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
            plt.title('Cumulative Fraction of Black Population by Distance', color=text_color)
            plt.xlabel('Distance from Origin (miles)', color=text_color)
            plt.ylabel('Relative Fraction of Total Black Population', color=text_color)
            plt.grid(True, linestyle='--', alpha=0.3)
            plt.ylim(0, 1.05)
            plt.axvline(x=avg_dist, color='red', linestyle='--', label=f'Avg Distance: {avg_dist:.2f} mi')
            plt.legend()
            
            filename_cum = f"{prefix}cumulative_distance_histogram{suffix}.png"
            path_cum = os.path.join(output_dir, filename_cum)
            plt.savefig(path_cum, facecolor=bg_color, edgecolor='none')
            plt.close()
            saved_files.append(filename_cum)

    # Task 5: Horizontal Comparative Bar
    plt.figure(figsize=(10, 3))
    avg_black_pct = 13.5 # National Average approx
    local_avg_pct = df['pct_black'].mean()
    
    plt.barh(['National Average', 'Local Results'], [avg_black_pct, local_avg_pct], 
             color=[DUB_GOLD, INK_BLACK if not dark_mode else '#ced4da'], edgecolor=edge_color, linewidth=1.5)
    plt.title('LOCAL VS NATIONAL DEMOGRAPHIC COMPARISON', fontweight='bold', color=text_color)
    plt.xlim(0, max(avg_black_pct, local_avg_pct) * 1.2)
    for i, v in enumerate([avg_black_pct, local_avg_pct]):
        plt.text(v + 0.5, i, f"{v:.1f}%", va='center', fontweight='bold', color=text_color)
    
    filename_comp = f"{prefix}comparative_bar{suffix}.png"
    path_comp = os.path.join(output_dir, filename_comp)
    plt.savefig(path_comp, facecolor=bg_color, edgecolor='none')
    plt.close()
    saved_files.append(filename_comp)

    # Task 8: The Research Funnel Pyramid
    plt.figure(figsize=(8, 8))
    # Logic: Zips -> Courses -> Enriched -> Majority Black
    enriched_count = df[df['total_pop'] > 0].shape[0] if 'total_pop' in df.columns else len(df)
    maj_black_count = df[df['pct_black'] > 50].shape[0]
    
    layers = ['ZIPS SCANNED', 'COURSES FOUND', 'ENRICHED DATA', 'MAJORITY BLACK']
    counts = [1, len(df), enriched_count, maj_black_count]
    colors = [DUB_INDIGO, DUB_RED, DUB_GOLD, GOLF_GREEN]
    
    for i, (layer, count, color) in enumerate(zip(layers, counts, colors)):
        # Pyramid effect: width decreases as density increases in the funnel
        width = 1.0 - (i * 0.2)
        left = (1.0 - width) / 2
        plt.barh(3-i, width, left=left, color=color, edgecolor='black', linewidth=1.5)
        plt.text(0.5, 3-i, f"{layer}\n({count})", ha='center', va='center', color='white' if i < 2 else 'black', fontweight='bold', fontsize=10)
    
    plt.xlim(0, 1)
    plt.axis('off')
    plt.title('THE RESEARCH FUNNEL', fontweight='bold', pad=20, color=text_color)
    
    filename_funnel = f"{prefix}research_funnel{suffix}.png"
    path_funnel = os.path.join(output_dir, filename_funnel)
    plt.savefig(path_funnel, facecolor=bg_color, edgecolor='none')
    plt.close()
    saved_files.append(filename_funnel)
    
    # Task 6: Scaled Progression Circles
    plt.figure(figsize=(6, 8))
    radii_mi = [10, 15, 25]
    colors_circ = [PARCHMENT if not dark_mode else '#25262b', DUB_GOLD, DUB_RED, DUB_INDIGO]
    
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
            circle = plt.Circle((0, -i*2.5), v_r, color=colors_circ[i % len(colors_circ)], ec=edge_color, lw=1.5)
            plt.gca().add_artist(circle)
            plt.text(0, -i*2.5, f"{r_mi}mi\n{c} Courses", ha='center', va='center', fontweight='bold', color=INK_BLACK if i % len(colors_circ) == 0 and not dark_mode else 'white')
            
        plt.xlim(-1.5, 1.5)
        plt.ylim(-7, 1.5)
        plt.axis('off')
        plt.title('SCALED SEARCH PROGRESSION', fontweight='bold', color=text_color)
        
        filename_circles = f"{prefix}progression_circles{suffix}.png"
        path_circles = os.path.join(output_dir, filename_circles)
        plt.savefig(path_circles, facecolor=bg_color, edgecolor='none')
        plt.close()
        saved_files.append(filename_circles)

    # Task 7: Stacked Income Correlation Bar
    if 'median_income' in df.columns and 'distance' in df.columns:
        plt.figure(figsize=(10, 5))
        
        # Distance groups
        dist_bins = [0, 10, 25, float('inf')]
        dist_labels = ['Near (<10mi)', 'Mid (10-25mi)', 'Far (25mi+)']
        df['dist_group'] = pd.cut(df['distance'], bins=dist_bins, labels=dist_labels)
        
        # Income brackets
        inc_bins = [0, 50000, 100000, 150000, float('inf')]
        inc_labels = ['<$50k', '$50k-100k', '$100k-150k', '$150k+']
        df['inc_bracket'] = pd.cut(df['median_income'], bins=inc_bins, labels=inc_labels)
        
        # Pivot table for stacked bar
        pivot_df = df.pivot_table(index='dist_group', columns='inc_bracket', values='name', aggfunc='count', observed=False).fillna(0)
        
        # Du Bois Style: Hand-drawn feel with connecting lines
        colors_inc = [DUB_INDIGO, DUB_RED, DUB_GOLD, GOLF_GREEN]
        bottom = [0] * len(pivot_df)
        
        segment_boundaries = [] # To store (x_start, x_end) for connecting lines
        
        for i, (label, color) in enumerate(zip(inc_labels, colors_inc)):
            counts = pivot_df[label].values
            plt.barh(pivot_df.index.astype(str), counts, left=bottom, color=color, edgecolor='black', linewidth=1.2, label=label)
            
            # Store boundaries for connecting lines
            current_boundaries = []
            for j, val in enumerate(counts):
                current_boundaries.append((bottom[j], bottom[j] + val))
            segment_boundaries.append(current_boundaries)
            
            bottom = [b + v for b, v in zip(bottom, counts)]

        # Add thin connecting lines to show demographic shifts
        for i in range(len(inc_labels)):
            for j in range(len(pivot_df) - 1):
                # Connect boundary of segment i in group j to segment i in group j+1
                y1, y2 = j, j + 1
                # Boundaries of segment i are at segment_boundaries[i]
                x1_top = segment_boundaries[i][j][1]
                x2_top = segment_boundaries[i][j+1][1]
                plt.plot([x1_top, x2_top], [y1 + 0.4, y2 - 0.4], color='black', alpha=0.3, linewidth=0.8)

        plt.title('INCOME DISTRIBUTION BY DISTANCE', fontweight='bold', color=text_color)
        plt.legend(title='Income Bracket', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xlabel('COURSE COUNT', color=text_color)
        plt.tight_layout()
        
        filename_income = f"{prefix}income_correlation{suffix}.png"
        path_income = os.path.join(output_dir, filename_income)
        plt.savefig(path_income, facecolor=bg_color, edgecolor='none')
        plt.close()
        saved_files.append(filename_income)

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
