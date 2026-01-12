import os
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import storage
from dotenv import load_dotenv
import io

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
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    run_analysis()
