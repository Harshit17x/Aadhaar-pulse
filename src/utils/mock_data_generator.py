
import pandas as pd
import random
import datetime
import os

def generate_mock_data():
    # 1. Generate Pincode Master
    print("Generating pincode_master.csv...")
    # Comprehensive Pan-India Coordinates
    DISTRICT_COORDS = {
        # North
        "Lucknow": (26.8467, 80.9462), "Kanpur": (26.4499, 80.3319), "Varanasi": (25.3176, 82.9739), "Meerut": (28.9845, 77.7064),
        "Dehradun": (30.3165, 78.0322), "Haridwar": (29.9457, 78.1642), "Shimla": (31.1048, 77.1734), "Chandigarh": (30.7333, 76.7794),
        "Ludhiana": (30.9010, 75.8573), "Amritsar": (31.6340, 74.8723), "Gurugram": (28.4595, 77.0266), "Faridabad": (28.4089, 77.3178),
        "Srinagar": (34.0837, 74.7973), "Jammu": (32.7266, 74.8570), "Leh": (34.1526, 77.5771),
        # West
        "Mumbai": (19.0760, 72.8777), "Pune": (18.5204, 73.8567), "Nagpur": (21.1458, 79.0882), "Ahmedabad": (23.0225, 72.5714),
        "Surat": (21.1702, 72.8311), "Jaipur": (26.9124, 75.7873), "Jodhpur": (26.2389, 73.0243), "Panaji": (15.4909, 73.8278),
        # East & Central
        "Kolkata": (22.5726, 88.3639), "Siliguri": (26.7271, 88.3953), "Patna": (25.5941, 85.1376), "Gaya": (24.7955, 85.0006),
        "Bhubaneswar": (20.2961, 85.8245), "Cuttack": (20.4625, 85.8830), "Ranchi": (23.3441, 85.3094), "Jamshedpur": (22.8046, 86.2029),
        "Raipur": (21.2514, 81.6296), "Bhopal": (23.2599, 77.4126), "Indore": (22.7196, 75.8577),
        # South
        "Bengaluru": (12.9716, 77.5946), "Mysuru": (12.2958, 76.6394), "Hyderabad": (17.3850, 78.4867), "Warangal": (17.9689, 79.5941),
        "Visakhapatnam": (17.6868, 83.2185), "Vijayawada": (16.5062, 80.6480), "Chennai": (13.0827, 80.2707), "Coimbatore": (11.0168, 76.9558),
        "Kochi": (9.9312, 76.2673), "Thiruvananthapuram": (8.5241, 76.9366),
        # Northeast & Islands
        "Guwahati": (26.1158, 91.7086), "Shillong": (25.5788, 91.8933), "Imphal": (24.8170, 93.9368), "Agartala": (23.8315, 91.2868),
        "Itanagar": (27.0844, 93.6053), "Gangtok": (27.3314, 88.6138), "Aizawl": (23.7271, 92.7176), "Kohima": (25.6751, 94.1086),
        "Port Blair": (11.6234, 92.7265), "Kavaratti": (10.5667, 72.6417)
    }
    districts = list(DISTRICT_COORDS.keys())
    districts = list(DISTRICT_COORDS.keys())
    
    DISTRICT_TO_STATE = {
        "Lucknow": "Uttar Pradesh", "Kanpur": "Uttar Pradesh", "Varanasi": "Uttar Pradesh", "Meerut": "Uttar Pradesh",
        "Dehradun": "Uttarakhand", "Haridwar": "Uttarakhand", "Shimla": "Himachal Pradesh", "Chandigarh": "Chandigarh",
        "Ludhiana": "Punjab", "Amritsar": "Punjab", "Gurugram": "Haryana", "Faridabad": "Haryana",
        "Srinagar": "Jammu and Kashmir", "Jammu": "Jammu and Kashmir", "Leh": "Ladakh",
        "Mumbai": "Maharashtra", "Pune": "Maharashtra", "Nagpur": "Maharashtra", "Ahmedabad": "Gujarat",
        "Surat": "Gujarat", "Jaipur": "Rajasthan", "Jodhpur": "Rajasthan", "Panaji": "Goa",
        "Kolkata": "West Bengal", "Siliguri": "West Bengal", "Patna": "Bihar", "Gaya": "Bihar",
        "Bhubaneswar": "Odisha", "Cuttack": "Odisha", "Ranchi": "Jharkhand", "Jamshedpur": "Jharkhand",
        "Raipur": "Chhattisgarh", "Bhopal": "Madhya Pradesh", "Indore": "Madhya Pradesh",
        "Bengaluru": "Karnataka", "Mysuru": "Karnataka", "Hyderabad": "Telangana", "Warangal": "Telangana",
        "Visakhapatnam": "Andhra Pradesh", "Vijayawada": "Andhra Pradesh", "Chennai": "Tamil Nadu", "Coimbatore": "Tamil Nadu",
        "Kochi": "Kerala", "Thiruvananthapuram": "Kerala",
        "Guwahati": "Assam", "Shillong": "Meghalaya", "Imphal": "Manipur", "Agartala": "Tripura",
        "Itanagar": "Arunachal Pradesh", "Gangtok": "Sikkim", "Aizawl": "Mizoram", "Kohima": "Nagaland",
        "Port Blair": "Andaman and Nicobar Islands", "Kavaratti": "Lakshadweep"
    }
    pincode_data = []
    
    for district in districts:
        state = DISTRICT_TO_STATE.get(district, "Other")
        # Generate 5-10 pincodes per district
        for i in range(random.randint(5, 10)):
            pincode = random.randint(110000, 850000) # National range
            base_lat, base_lon = DISTRICT_COORDS[district]
            # Add small jitter so pincodes aren't identical
            lat = round(base_lat + random.uniform(-0.05, 0.05), 4)
            lon = round(base_lon + random.uniform(-0.05, 0.05), 4)
            pincode_data.append({
                "Pincode": pincode,
                "District": district,
                "State": state,
                "Latitude": lat,
                "Longitude": lon
            })
            
    df_pincode = pd.DataFrame(pincode_data).drop_duplicates(subset=["Pincode"])
    
    # Save Pincode Master
    os.makedirs("data", exist_ok=True)
    df_pincode.to_csv("data/pincode_master.csv", index=False)
    print(f"Saved {len(df_pincode)} pincodes to data/pincode_master.csv")

    # 2. Generate Raw Aadhaar Logs
    print("Generating raw_aadhaar_logs.csv...")
    
    update_types = ["Address", "DoB Update", "Mobile Update", "Email Update", "Biometric Update"]
    
    all_pincodes = df_pincode["Pincode"].tolist()
    
    logs = []
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2025, 12, 31)
    date_range = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    
    # Generate ~30000 logs
    for _ in range(30000):
        date = random.choice(date_range)
        uid = f"UID{random.randint(100000000000, 999999999999)}"
        update_type = random.choice(update_types)
        
        # Determine source and dest pincode
        # Ideally for address change, source and dest should differ. 
        # For others, 'previous' might not be relevant location-wise for migration but let's simulate location
        # Let's assume the log captures the location at time of update.
        # But for 'migration' we specifically need Address Change flows.
        
        if update_type == "Address":
            # Migration Event
            prev_pincode = random.choice(all_pincodes)
            curr_pincode = random.choice(all_pincodes)
            while prev_pincode == curr_pincode:
                curr_pincode = random.choice(all_pincodes)
        else:
            # Static Event (just an update at current location)
            curr_pincode = random.choice(all_pincodes)
            prev_pincode = curr_pincode # Or null
            
        logs.append({
            "Timestamp": date.strftime("%Y-%m-%d %H:%M:%S"),
            "Aadhaar_ID": uid,
            "Update_Type": update_type,
            "Source_Pincode": prev_pincode,
            "Dest_Pincode": curr_pincode
        })
        
    df_logs = pd.DataFrame(logs)
    df_logs.to_csv("data/raw_aadhaar_logs.csv", index=False)
    print(f"Saved {len(df_logs)} logs to data/raw_aadhaar_logs.csv")

if __name__ == "__main__":
    generate_mock_data()
