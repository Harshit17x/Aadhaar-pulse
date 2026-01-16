import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import os
import logging
from math import radians, cos, sin, asin, sqrt

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def detect_anomalies():
    """
    Detects anomalies using Isolation Forest based on Daily_Volume and Avg_Distance.
    """
    data_path = os.path.join(os.path.dirname(__file__), '../../data/district_flows.csv')
    
    if not os.path.exists(data_path):
        logging.error(f"Data file not found at {data_path}")
        return

    logging.info("Loading data...")
    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # Calculate Distance for each flow
    # Using vectorized operations if possible, but map is easier for now
    logging.info("Calculating distances...")
    df['distance'] = df.apply(lambda row: haversine(row['source_lon'], row['source_lat'], 
                                                    row['dest_lon'], row['dest_lat']), axis=1)
    
    # Aggregate by Date and District
    # Daily_Volume = Sum(count)
    # Avg_Distance = Weighted Average of Distance? or just Mean? 
    # Plan said "Avg_Distance". Let's do Weighted Average by count (person-km)
    
    # Helper to calculate weighted average
    def weighted_avg_dist(x):
        return np.average(x['distance'], weights=x['count'])

    # Groupby is tricky with weighted avg in one shot. 
    # Let's calculate total_person_km first
    df['person_km'] = df['distance'] * df['count']
    
    daily_stats = df.groupby(['date', 'source_district']).agg(
        total_volume=('count', 'sum'),
        total_person_km=('person_km', 'sum')
    ).reset_index()
    
    daily_stats['avg_distance'] = daily_stats['total_person_km'] / daily_stats['total_volume']
    
    # Features for Anomaly Detection
    X = daily_stats[['total_volume', 'avg_distance']]
    
    logging.info("Training Isolation Forest...")
    clf = IsolationForest(contamination=0.05, random_state=42)
    daily_stats['anomaly_score'] = clf.fit_predict(X)
    daily_stats['score_val'] = clf.decision_function(X)
    
    # Flag detected anomalies (IsolationForest returns -1 for anomalies)
    # Flag detected anomalies (IsolationForest returns -1 for anomalies)
    # We rename 'anomaly_score' to 'is_anomaly_flag' to avoid confusion, 
    # but the dashboard wants 'is_anomaly' (boolean) and 'anomaly_score' (float/int).
    
    daily_stats['is_anomaly'] = daily_stats['anomaly_score'] == -1
    
    # Merge back to original flows
    # We want to mark all flows from an anomalous district-day as anomalous
    logging.info("Merging anomalies back to flow data...")
    
    df_merged = df.merge(
        daily_stats[['date', 'source_district', 'is_anomaly', 'score_val']],
        on=['date', 'source_district'],
        how='left'
    )
    
    df_merged.rename(columns={'score_val': 'anomaly_score'}, inplace=True)
    
    # Save back to CSV
    logging.info(f"Saving updated data to {data_path}...")
    df_merged.to_csv(data_path, index=False)
    
    logging.info(f"Detected {daily_stats['is_anomaly'].sum()} anomalous district-days.")
    
    return df_merged

if __name__ == "__main__":
    detect_anomalies()
