import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

def generate_mock_data():
    """Generates mock data for the Aadhaar Pulse dashboard."""
    
    # Constants
    STATES = ['Uttar Pradesh', 'Maharashtra', 'Bihar', 'Delhi', 'Karnataka', 'Rajasthan']
    DISTRICTS = {
        'Uttar Pradesh': ['Lucknow', 'Kanpur', 'Varanasi', 'Noida', 'Agra'],
        'Maharashtra': ['Mumbai', 'Pune', 'Nagpur', 'Nashik', 'Thane'],
        'Bihar': ['Patna', 'Gaya', 'Muzaffarpur', 'Bhagalpur'],
        'Delhi': ['New Delhi', 'North Delhi', 'South Delhi'],
        'Karnataka': ['Bangalore', 'Mysore', 'Hubli'],
        'Rajasthan': ['Jaipur', 'Jodhpur', 'Udaipur']
    }
    
    # Lat/Lon approximations for visualization
    # Ideally this would come from a proper geojson/CSV
    COORDS = {
        'Lucknow': (26.8467, 80.9462), 'Kanpur': (26.4499, 80.3319),
        'Varanasi': (25.3176, 82.9739), 'Noida': (28.5355, 77.3910), 'Agra': (27.1767, 78.0081),
        'Mumbai': (19.0760, 72.8777), 'Pune': (18.5204, 73.8567),
        'Patna': (25.5941, 85.1376), 'Bangalore': (12.9716, 77.5946),
        'Jaipur': (26.9124, 75.7873), 'New Delhi': (28.6139, 77.2090)
    }

    # Generate dates required for 3 months
    end_date = datetime.now()
    dates = [end_date - timedelta(days=x) for x in range(90)]
    
    print("Generating mock data...")
    
    rows = []
    
    # Generate random flows
    for date in dates:
        # Create 50 random transactions per day
        for _ in range(50):
            src_state = random.choice(STATES)
            dest_state = random.choice(STATES)
            
            # Weighted probability to make some flows more common (e.g., to Cities)
            if random.random() > 0.7:
                dest_state = 'Maharashtra' if random.random() > 0.5 else 'Delhi'

            if src_state == dest_state and random.random() > 0.3:
                 dest_state = random.choice([s for s in STATES if s != src_state])
            
            src_dist = random.choice(DISTRICTS.get(src_state, ['Unknown']))
            dest_dist = random.choice(DISTRICTS.get(dest_state, ['Unknown']))
            
            # Anomaly injection
            count = int(np.random.normal(100, 20))
            is_anomaly = False
            anomaly_score = 0.0
            
            # Inject spike anomaly randomly
            if random.random() < 0.05:
                count = int(np.random.normal(500, 50))
                is_anomaly = True
                anomaly_score = 0.95
            
            src_lat, src_lon = COORDS.get(src_dist, (20.5937, 78.9629)) # Default to India center
            dest_lat, dest_lon = COORDS.get(dest_dist, (20.5937, 78.9629))
            
            # Jitter coords slightly so lines don't perfectly overlap
            src_lat += random.uniform(-0.01, 0.01)
            src_lon += random.uniform(-0.01, 0.01)
            
            rows.append({
                'date': date.strftime('%Y-%m-%d'),
                'source_state': src_state,
                'source_district': src_dist,
                'source_lat': src_lat,
                'source_lon': src_lon,
                'dest_state': dest_state,
                'dest_district': dest_dist,
                'dest_lat': dest_lat,
                'dest_lon': dest_lon,
                'count': count,
                'is_anomaly': is_anomaly,
                'anomaly_score': anomaly_score
            })
            
    df = pd.DataFrame(rows)
    
    # Ensure data directory exists
    os.makedirs(os.path.join(os.path.dirname(__file__), '../../data'), exist_ok=True)
    
    output_path = os.path.join(os.path.dirname(__file__), '../../data/district_flows.csv')
    df.to_csv(output_path, index=False)
    print(f"Mock data generated at {output_path}")

if __name__ == "__main__":
    generate_mock_data()
