
import pandas as pd
import os

def load_data(data_dir="data"):
    """Loads raw logs and pincode master data."""
    logs_path = os.path.join(data_dir, "raw_aadhaar_logs.csv")
    pincode_path = os.path.join(data_dir, "pincode_master.csv")
    
    if not os.path.exists(logs_path) or not os.path.exists(pincode_path):
        raise FileNotFoundError("Input files not found. Please generate mock data first.")
        
    logs = pd.read_csv(logs_path)
    pincode_master = pd.read_csv(pincode_path)
    return logs, pincode_master

def process_migration_data(logs, pincode_master):
    """Aggregates migration flows between districts with coordinates."""
    
    # Filter for Address Change only
    migration_logs = logs[logs["Update_Type"] == "Address"].copy()
    
    # Ensure pincodes are int/str consistency
    migration_logs["Source_Pincode"] = migration_logs["Source_Pincode"].astype(int)
    migration_logs["Dest_Pincode"] = migration_logs["Dest_Pincode"].astype(int)
    pincode_master["Pincode"] = pincode_master["Pincode"].astype(int)
    
    # --- Prepare District Master (Centroids) ---
    # Take the first entry for State/Lat/Lon per district for simplicity
    district_master = pincode_master.groupby("District")[["Latitude", "Longitude"]].mean().reset_index()
    # State cannot be mean, so we merge it back
    district_states = pincode_master.groupby("District")["State"].first().reset_index()
    district_master = district_master.merge(district_states, on="District")
    
    # Join Source District
    migration_logs = migration_logs.merge(
        pincode_master[["Pincode", "District"]], 
        left_on="Source_Pincode", 
        right_on="Pincode", 
        how="inner"
    ).rename(columns={"District": "source_district"}).drop(columns=["Pincode"])
    
    # Join Destination District
    migration_logs = migration_logs.merge(
        pincode_master[["Pincode", "District"]], 
        left_on="Dest_Pincode", 
        right_on="Pincode", 
        how="inner"
    ).rename(columns={"District": "dest_district"}).drop(columns=["Pincode"])
    
    # Convert Timestamp to Date
    migration_logs["date"] = pd.to_datetime(migration_logs["Timestamp"]).dt.date
    
    # Aggregation: Group by Date, Source, Dest
    daily_flows = migration_logs.groupby(["date", "source_district", "dest_district"]).size().reset_index(name="count")
    
    # --- Enrich with Coordinates ---
    # Source Info
    daily_flows = daily_flows.merge(
        district_master.rename(columns={
            "District": "source_district", 
            "State": "source_state",
            "Latitude": "source_lat", 
            "Longitude": "source_lon"
        }),
        on="source_district",
        how="left"
    )
    
    # Dest Info
    daily_flows = daily_flows.merge(
        district_master.rename(columns={
            "District": "dest_district", 
            "State": "dest_state",
            "Latitude": "dest_lat", 
            "Longitude": "dest_lon"
        }),
        on="dest_district",
        how="left"
    )

    return daily_flows

def calculate_net_migration(daily_flows):
    """Calculates Net Migration (Inflow - Outflow) per district per day."""
    
    # Inflow: Dest is the district
    inflow = daily_flows.groupby(["date", "dest_district"])["count"].sum().reset_index()
    inflow.rename(columns={"dest_district": "district", "count": "inflow"}, inplace=True)
    
    # Outflow: Source is the district
    outflow = daily_flows.groupby(["date", "source_district"])["count"].sum().reset_index()
    outflow.rename(columns={"source_district": "district", "count": "outflow"}, inplace=True)
    
    # Merge
    net_migration = pd.merge(inflow, outflow, on=["date", "district"], how="outer").fillna(0)
    net_migration["net_migration"] = net_migration["inflow"] - net_migration["outflow"]
    
    return net_migration

def main():
    print("Loading data...")
    logs, pincode_master = load_data()
    
    print("Processing migration flows...")
    daily_flows = process_migration_data(logs, pincode_master)
    
    print("Calculating net migration (for verification, not saving separately yet)...")
    net_migration = calculate_net_migration(daily_flows)
    
    # Requirement: Save Processed Data: district_flows.csv
    # The prompt mainly asked for district_flows.csv with fields: Migration_Flow_Count, Net_Migration?
    # Actually the Goal says: Calculate Metrics: Migration_Flow_Count, Net_Migration.
    # Usually "Flows" implies Source->Dest. Net Migration is per District.
    # Saving both might be best, OR adding Net Migration to the flow is weird because Net is per node, not per edge.
    # The prompt says "Save to data/district_flows.csv". 
    # I will save the Edge list (Source->Dest) as district_flows.csv.
    # I can also save a 'district_stats.csv' if needed, but the prompt specifically asked for 'district_flows.csv'.
    # I'll save the flows. 
    
    output_path = "data/district_flows.csv"
    daily_flows.to_csv(output_path, index=False)
    print(f"Saved processed data to {output_path}")

    net_output_path = "data/district_net_migration.csv"
    net_migration.to_csv(net_output_path, index=False)
    print(f"Saved net migration data to {net_output_path}")

    print("Sample Output (Flows):")
    print(daily_flows.head())
    print("\nSample Output (Net Migration):")
    print(net_migration.head())

if __name__ == "__main__":
    main()
