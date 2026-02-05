import os
import requests
import time
import csv
from datetime import datetime, time as dtime

# Base URL for the Car Park API
BASE_URL = "https://api.transport.nsw.gov.au/v1/carpark"
API_KEY = os.getenv("TFNSW_API_KEY")

headers = {
    "Authorization": f"apikey {API_KEY}",
    "Accept": "application/json"
}

TARGET_IDS = [
    "6", # Gordon Henry St (north)
    "7", # Kiama
    "8", # Gosford
    "9", # Revesby
    "10", # Warriewood
    "11", # Narrabeen
    "12", # Mona Vale
    "13", # Dee Why
    "14", # West Ryde
    "15", # Sutherland East Parade
    "16", # Leppington
    "17", # Edmonson Park (south)
    "18", # St Marys
    "19", # Campbelltown Farrow Rd (north)
    "20", # Campbelltown Hurley St
    "21", # Penrith (at-grade)
    "22", # Penrith (multi-level)
    "23", # Warwick Farm
    "24", # Schofields
    "25", # Hornsby
    "26", # Tallawong P1
    "27", # Tallawong P2
    "28", # Tallawong P3
    "29", # Kellyville (north)
    "30", # Kellyville (south)
    "31", # Bella Vista
    "32", # Hills Showground
    "33", # Cherrybrook
    "34", # Lindfield Village Green
    "35", # Beverly Hills
    "36", # Emu Plains
    "37", # Riverwood
    "38", # North Rocks
    "39", # Edmonson Park (north)
    "486", # Ashfield
    "487", # Kogarah
    "488", # Seven Hills
    "489", # Manly Vale
    "490" # Brookvale
]

def fetch_all_park_and_ride():
    results = []
    print(f"Starting fetch for {len(TARGET_IDS)} locations...")

    for fid in TARGET_IDS:
        try:
            params = {'facility': fid}
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                loc = data.get("location", {})
                occ = data.get("occupancy", {})
                
                # Get the raw value from the API
                raw_occupied = occ.get("total")
                spots = int(data.get("spots") or 0)
                
                # 1. Handle the -1 Sentinel Value (Sensor Error)
                if raw_occupied == "-1" or raw_occupied == -1 or raw_occupied is None:
                    occupied = None
                    available = None
                    status = "Unknown (Sensor Error)"
                else:
                    # 2. If data is valid, perform the math
                    occupied = int(raw_occupied)
                    # Formula: Availability = spots - total
                    available = max(0, spots - occupied)
                    
                    # Status thresholds per recommendation
                    status = "Available"
                    if available < 1:
                        status = "Full"
                    elif available < (spots * 0.1):
                        status = "Almost Full"

                # 3. Append to results (CSV handles 'None' as an empty cell)
                results.append({
                    "timestamp_utc": data.get("MessageDate"),
                    "facility_id": fid,
                    "facility_name": data.get("facility_name"),
                    "tfnsw_facility_id": data.get("tfnsw_facility_id"),
                    "suburb": loc.get("suburb"),
                    "latitude": loc.get("latitude"),
                    "longitude": loc.get("longitude"),
                    "spots": spots,
                    "occupied": occupied,
                    "available": available,
                    "status": status
                })
                print(f" ✓ {data.get('facility_name')}")
            
            time.sleep(0.2) # Recommended delay between requests

        except Exception as e:
            print(f" ! Error fetching ID {fid}: {e}")

    return results

def save_to_csv(data):
    if not data:
        return

    # 1. Define the relative path to /data/raw
    # '..' goes up one level from /scripts, then into /data/raw
    base_dir = os.path.dirname(os.path.dirname(__file__))
    output_dir = os.path.join(base_dir, "data", "raw")
    
    # 2. Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)

    # 3. Determine filename based on current year
    current_year = datetime.now().year
    filename = f"{current_year}_occupancy_data.csv"
    full_path = os.path.join(output_dir, filename)
    
    fieldnames = [
        "timestamp_utc", "facility_id", "facility_name", "tfnsw_facility_id",
        "suburb", "latitude", "longitude", "spots", "occupied", "available", "status"
    ]
    
    file_exists = os.path.isfile(full_path)
    
    with open(full_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
            
        writer.writerows(data)
    
    print(f"Successfully appended data to {full_path}")

if __name__ == "__main__":
    final_data = fetch_all_park_and_ride()
    if final_data:
        save_to_csv(final_data)
    else:
        print("No data fetched.")
