import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Configuration
RAW_DIR = Path("data/raw")
HOLIDAY_FILE = Path("data/config/school_holidays.json")
MASTER_FILE = Path("data/processed/master_stats.csv")
DECAY_RATE = 0.96  # Weight = 0.96^(months_old)

def load_holiday_ranges():
    """Loads holiday date ranges from your manual JSON file."""
    if not HOLIDAY_FILE.exists():
        return []
    with open(HOLIDAY_FILE, 'r') as f:
        data = json.load(f)
        # Convert string dates to datetime objects for easy comparison
        return [
            (datetime.strptime(h['start'], '%Y-%m-%d').date(), 
             datetime.strptime(h['end'], '%Y-%m-%d').date())
            for h in data.get("nsw_school_holidays", [])
        ]

def is_school_holiday(dt, holiday_ranges):
    """Checks if a given datetime is within any of the holiday ranges."""
    check_date = dt.date()
    for start, end in holiday_ranges:
        if start <= check_date <= end:
            return True
    return False

def load_all_raw_data():
    all_files = list(RAW_DIR.rglob("*.csv"))
    if not all_files:
        return None

    li = [pd.read_csv(f) for f in all_files]
    df = pd.concat(li, axis=0, ignore_index=True)

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df["timestamp_local"] = df["timestamp_utc"].dt.tz_convert("Australia/Sydney")
    
    df = df[df["occupied"].notna() & (df["occupied"] >= 0)]
    return df

def create_model_keys(df):
    # 1. Load your manual holidays
    holiday_ranges = load_holiday_ranges()
    
    # 2. Flag rows (True/False)
    df["is_school_holiday"] = df["timestamp_local"].apply(
        lambda x: is_school_holiday(x, holiday_ranges)
    )
    
    # 3. Time keys
    df["day_of_week"] = df["timestamp_local"].dt.dayofweek
    df["hour"] = df["timestamp_local"].dt.hour
    df["minute"] = df["timestamp_local"].dt.minute
    df["time_bin"] = df["hour"] * 6 + (df["minute"] // 10)
    
    return df

def calculate_monthly_weight(dt):
    """Calculates weight based on months elapsed since the data point."""
    today = datetime.now()
    months_diff = (today.year - dt.year) * 12 + (today.month - dt.month)
    return max(0.05, DECAY_RATE ** max(0, months_diff))

def aggregate_to_stats(df):
    # 1. Calculate weights for every row
    df['weight'] = df['timestamp_local'].apply(calculate_monthly_weight)
    
    # 2. Prepare metrics
    df["is_full"] = (df["available"] == 0).astype(int)
    
    # 3. Weighted Aggregation
    stats = df.groupby([
        "facility_name", "day_of_week", "time_bin", "is_school_holiday"
    ]).apply(lambda x: pd.Series({
        "n": x["weight"].sum(),
        "sum_available": (x["available"] * x["weight"]).sum(),
        "sum_sq_available": ((x["available"]**2) * x["weight"]).sum(),
        "full_count": (x["is_full"] * x["weight"]).sum()
    })).reset_index()
    
    return stats

def main():
    df = load_all_raw_data()
    if df is not None:
        df = create_model_keys(df)
        stats = aggregate_to_stats(df)
        
        MASTER_FILE.parent.mkdir(parents=True, exist_ok=True)
        stats.to_csv(MASTER_FILE, index=False)
        print(f"Master stats updated successfully.")

if __name__ == "__main__":
    main()