import pandas as pd
import glob
import os
import tqdm

def process_india_data():
    # File is in /aadhaarpulse/src/processing/
    # Project root is /aadhaarpulse/
    # Files are in / (parent of project root)
    agg_script_dir = os.path.dirname(__file__)
    project_root = os.path.dirname(os.path.dirname(agg_script_dir))
    parent_dir = os.path.dirname(project_root)
    
    data_out_dir = os.path.join(project_root, "data")
    os.makedirs(data_out_dir, exist_ok=True)

    STATE_CENTERS = {
        "Andhra Pradesh": (15.9129, 79.7400), "Arunachal Pradesh": (28.2180, 94.7278), "Assam": (26.2006, 92.9376),
        "Bihar": (25.0961, 85.3131), "Chhattisgarh": (21.2787, 81.8661), "Goa": (15.2993, 74.1240),
        "Gujarat": (22.2587, 71.1924), "Haryana": (29.0588, 76.0856), "Himachal Pradesh": (31.1048, 77.1734),
        "Jharkhand": (23.6102, 85.2799), "Karnataka": (15.3173, 75.7139), "Kerala": (10.8505, 76.2711),
        "Madhya Pradesh": (22.9734, 78.6569), "Maharashtra": (19.7515, 75.7139), "Manipur": (24.6637, 93.9063),
        "Meghalaya": (25.4670, 91.3659), "Mizoram": (23.1645, 92.9376), "Nagaland": (26.1584, 94.5624),
        "Odisha": (20.9517, 85.0985), "Punjab": (31.1471, 75.3412), "Rajasthan": (27.0238, 74.2179),
        "Sikkim": (27.5330, 88.5122), "Tamil Nadu": (11.1271, 78.6569), "Telangana": (18.1124, 79.0193),
        "Tripura": (23.9408, 91.9882), "Uttar Pradesh": (26.8467, 80.9462), "Uttarakhand": (30.0668, 79.0193),
        "West Bengal": (22.9868, 87.8550), "Delhi": (28.6139, 77.2090), "Jammu and Kashmir": (33.7782, 76.5762),
        "Ladakh": (34.1526, 77.5771), "Puducherry": (11.9416, 79.8083), "Andaman and Nicobar Islands": (11.7401, 92.6586),
        "Chandigarh": (30.7333, 76.7794), "Dadra and Nagar Haveli and Daman and Diu": (20.1809, 73.0169),
        "Lakshadweep": (10.5667, 72.6417)
    }

    print(f"--- Starting Pan-India Data Processing ---")
    print(f"Searching in: {parent_dir}")

    # 1. Gather file groups
    demo_files = glob.glob(os.path.join(parent_dir, "api_data_aadhar_demographic_*.csv"))
    bio_files = glob.glob(os.path.join(parent_dir, "api_data_aadhar_biometric_*.csv"))
    enrol_files = glob.glob(os.path.join(parent_dir, "api_data_aadhar_enrolment_*.csv"))

    if not demo_files:
        print("Error: No data files found! Check parent directory paths.")
        return

    def aggregate_files(files, group_cols, agg_dict):
        dfs = []
        for f in tqdm.tqdm(files, desc="Processing files"):
            df = pd.read_csv(f)
            # Standardize date format to YYYY-MM-DD
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], dayfirst=True).dt.strftime('%Y-%m-%d')
            
            # Aggregate immediately to save memory
            agg_df = df.groupby(group_cols).agg(agg_dict).reset_index()
            dfs.append(agg_df)
        
        # Combine all parts and aggregate again
        combined = pd.concat(dfs)
        return combined.groupby(group_cols).agg(agg_dict).reset_index()

    # Define aggregation logic
    agg_group = ['date', 'state', 'district']
    
    demo_agg_dict = {
        'demo_age_5_17': 'sum',
        'demo_age_17_': 'sum'
    }
    
    bio_agg_dict = {
        'bio_age_5_17': 'sum',
        'bio_age_17_': 'sum'
    }
    
    enrol_agg_dict = {
        'age_0_5': 'sum',
        'age_5_17': 'sum',
        'age_18_greater': 'sum'
    }

    # Process and Aggregate
    print("\n[1/3] Aggregating Demographic Data...")
    demo_final = aggregate_files(demo_files, agg_group, demo_agg_dict)
    
    print("\n[2/3] Aggregating Biometric Data...")
    bio_final = aggregate_files(bio_files, agg_group, bio_agg_dict)
    
    print("\n[3/3] Aggregating Enrolment Data...")
    enrol_final = aggregate_files(enrol_files, agg_group, enrol_agg_dict)

    # 2. Merge all sources
    print("\nMerging datasets...")
    merged = pd.merge(demo_final, bio_final, on=agg_group, how='outer')
    merged = pd.merge(merged, enrol_final, on=agg_group, how='outer')
    
    # Fill NaNs with 0
    merged = merged.fillna(0)

    # 3. Add Metric for "Total Digital Growth" or similar
    merged['total_updates'] = merged['demo_age_5_17'] + merged['demo_age_17_'] + \
                             merged['bio_age_5_17'] + merged['bio_age_17_']
    merged['total_enrolments'] = merged['age_0_5'] + merged['age_5_17'] + merged['age_18_greater']

    # 4. Add Coordinates
    print("\nAssigning coordinates...")
    # Get unique districts and assign them a jittered coordinate based on state center
    unique_districts = merged[['state', 'district']].drop_duplicates()
    
    import random
    random.seed(42) # Consistent jitter
    
    def get_coords(row):
        state = row['state']
        base_lat, base_lon = STATE_CENTERS.get(state, (20.5937, 78.9629)) # India center fallback
        # Jitter more for India-wide to avoid overlapping in state center
        return base_lat + random.uniform(-0.5, 0.5), base_lon + random.uniform(-0.5, 0.5)

    coords_map = unique_districts.apply(lambda r: get_coords(r), axis=1)
    unique_districts['latitude'] = [c[0] for c in coords_map]
    unique_districts['longitude'] = [c[1] for c in coords_map]
    
    merged = merged.merge(unique_districts, on=['state', 'district'], how='left')

    # 5. Save
    output_path = os.path.join(data_out_dir, "india_aggregated.csv")
    merged.to_csv(output_path, index=False)
    print(f"\n--- SUCCESS ---")
    print(f"Processed data saved to: {output_path}")
    print(f"Total aggregated records: {len(merged)}")

if __name__ == "__main__":
    process_india_data()
