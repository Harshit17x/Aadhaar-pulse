import pandas as pd
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
import os

# Initialize Faker
fake = Faker('en_IN')
Faker.seed(42)
random.seed(42)

def generate_mock_data(num_records=50000):
    """Generates synthetic Aadhaar update logs with biases and anomalies."""
    print(f"Generating {num_records} records...")
    
    data = []
    
    # Time window: Last 30 days
    end_time = datetime.now()
    start_time = end_time - timedelta(days=30)
    
    # Load Pincode Master to ensure valid mappings
    try:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
        pincode_df = pd.read_csv(os.path.join(data_dir, 'pincode_master.csv'))
        valid_pincodes = pincode_df['Pincode'].astype(str).tolist()
        
        # Get pincodes for bias scenarios
        lucknow_pincodes = pincode_df[pincode_df['District'] == 'Lucknow']['Pincode'].astype(str).tolist()
        noida_pincodes = pincode_df[pincode_df['District'] == 'Noida']['Pincode'].astype(str).tolist()
        
        if not lucknow_pincodes or not noida_pincodes:
            # Fallback if specific districts missing
            lucknow_pincodes = valid_pincodes[:5]
            noida_pincodes = valid_pincodes[-5:]
            
    except Exception as e:
        print(f"Warning: Could not load pincode_master.csv: {e}")
        # Fallback to random if file missing
        valid_pincodes = [fake.postcode() for _ in range(100)]
        lucknow_pincodes = valid_pincodes[:5]
        noida_pincodes = valid_pincodes[-5:]

    for _ in range(num_records):
        # Basic Random Data
        record = {
            'UpdateID': str(uuid.uuid4()),
            'Timestamp': fake.date_time_between(start_date=start_time, end_date=end_time),
            'Source_Pincode': str(random.choice(valid_pincodes)),
            'Dest_Pincode': str(random.choice(valid_pincodes)),
            'Update_Type': random.choice(['Address', 'Mobile', 'Biometric']),
            'Age': random.randint(0, 100),
            'Gender': random.choice(['Male', 'Female', 'Other'])
        }
        data.append(record)

    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(data)

    # --- INJECT BIAS ---
    # Trend: Migration from Lucknow to Noida
    # Applying to ~10% of data
    bias_count = int(num_records * 0.10)
    print(f"Injecting bias into {bias_count} records (Lucknow -> Noida migration)...")
    
    # We'll just modify the first 'bias_count' records for simplicity, 
    # or random indices. Let's pick random indices to distribute it.
    bias_indices = random.sample(range(num_records), bias_count)
    
    for idx in bias_indices:
        df.at[idx, 'Source_Pincode'] = random.choice(lucknow_pincodes)
        df.at[idx, 'Dest_Pincode'] = random.choice(noida_pincodes)
        df.at[idx, 'Update_Type'] = 'Address' # Mostly address changes for migration

    # --- INJECT ANOMALY ---
    # Anomaly: One pincode having 500 updates in 1 hour
    anomaly_pincode = random.choice(valid_pincodes)
    anomaly_count = 500
    print(f"Injecting anomaly: {anomaly_count} updates at {anomaly_pincode} in 1 hour...")
    
    # Create a tight timestamps window
    anomaly_base_time = end_time - timedelta(days=2) # 2 days ago
    
    anomaly_records = []
    for i in range(anomaly_count):
        # All within same hour
        anomaly_time = anomaly_base_time + timedelta(minutes=random.randint(0, 59))
        
        record = {
            'UpdateID': str(uuid.uuid4()),
            'Timestamp': anomaly_time,
            'Source_Pincode': anomaly_pincode,
            'Dest_Pincode': anomaly_pincode, # update within same area or relevant logic
            'Update_Type': 'Biometric', # Sudden check or update drive
            'Age': random.randint(18, 60),
            'Gender': random.choice(['Male', 'Female', 'Other'])
        }
        anomaly_records.append(record)
        
    df_anomaly = pd.DataFrame(anomaly_records)
    
    # Append anomaly to main dataframe
    df = pd.concat([df, df_anomaly], ignore_index=True)
    
    # Sort by Timestamp
    df = df.sort_values(by='Timestamp').reset_index(drop=True)
    
    return df

if __name__ == "__main__":
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'raw_aadhaar_logs.csv')
    
    df = generate_mock_data()
    df.to_csv(output_file, index=False)
    print(f"Successfully generated {len(df)} records saved to {output_file}")
