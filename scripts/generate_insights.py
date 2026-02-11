import pandas as pd
import numpy as np
import json
import os

# --- Configuration ---
LOW_OBSERVATION_THRESHOLD = 5
Z_SCORE_99 = 2.576
FULL_THRESHOLD = 0.8

# 1. Facility Name Mapping
NAME_MAPPING = {
    "Park&Ride - Ashfield": "Ashfield",
    "Park&Ride - Bella Vista": "Bella Vista",
    "Park&Ride - Beverly Hills": "Beverly Hills",
    "Park&Ride - Brookvale": "Brookvale",
    "Park&Ride - Campbelltown Farrow Rd (north)": "Campbelltown Farrow Rd (north)",
    "Park&Ride - Campbelltown Hurley St": "Campbelltown Hurley St",
    "Park&Ride - Cherrybrook": "Cherrybrook",
    "Park&Ride - Dee Why": "Dee Why",
    "Park&Ride - Edmondson Park (north)": "Edmondson Park (north)",
    "Park&Ride - Edmondson Park (south)": "Edmondson Park (south)",
    "Park&Ride - Emu Plains": "Emu Plains",
    "Park&Ride - Gordon Henry St (north)": "Gordon Henry St (north)",
    "Park&Ride - Gosford": "Gosford",
    "Park&Ride - Hills Showground": "Hills Showground",
    "Park&Ride - Hornsby": "Hornsby",
    "Park&Ride - Kellyville (north)": "Kellyville (north)",
    "Park&Ride - Kellyville (south)": "Kellyville (south)",
    "Park&Ride - Kiama": "Kiama",
    "Park&Ride - Kogarah": "Kogarah",
    "Park&Ride - Leppington": "Leppington",
    "Park&Ride - Lindfield Village Green": "Lindfield Village Green",
    "Park&Ride - Manly Vale": "Manly Vale",
    "Park&Ride - Mona Vale": "Mona Vale",
    "Park&Ride - Narrabeen": "Narrabeen",
    "Park&Ride - North Rocks": "North Rocks",
    "Park&Ride - Penrith (at-grade)": "Penrith (at-grade)",
    "Park&Ride - Penrith (multi-level)": "Penrith (multi-level)",
    "Park&Ride - Revesby": "Revesby",
    "Park&Ride - Riverwood": "Riverwood",
    "Park&Ride - Schofields": "Schofields",
    "Park&Ride - Seven Hills": "Seven Hills",
    "Park&Ride - St Marys": "St Marys",
    "Park&Ride - Sutherland": "Sutherland",
    "Park&Ride - Tallawong P1": "Tallawong P1",
    "Park&Ride - Tallawong P2": "Tallawong P2",
    "Park&Ride - Tallawong P3": "Tallawong P3",
    "Park&Ride - Warriewood": "Warriewood",
    "Park&Ride - Warwick Farm": "Warwick Farm",
    "Park&Ride - West Ryde": "West Ryde"
}

# 2. Day Mapping
DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

# 3. Sort Order (To ensure Monday is the first day in the dropdown)
DAY_SORT_ORDER = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6
}

def get_time_label(bin_index):
    """
    Reverse engineers the time_bin logic:
    bin_index = hour * 6 + (minute // 10)
    """
    # 1. Calculate the hour (0-23)
    hour = bin_index // 6
    
    # 2. Calculate the minute (the start of the 10-min window)
    minute = (bin_index % 6) * 10
    
    # 3. Determine AM/PM
    period = "AM" if hour < 12 else "PM"
    
    # 4. Convert 24h to 12h format
    display_hour = hour % 12
    if display_hour == 0:
        display_hour = 12
        
    return f"{display_hour}:{minute:02d} {period}"

def process_insights(stats_csv_path, output_path):
    if not os.path.exists(stats_csv_path):
        print(f"Error: {stats_csv_path} not found.")
        return

    df = pd.read_csv(stats_csv_path)
    
    # Statistical Calculations
    df['mean_available'] = df['sum_available'] / df['n']
    df['variance'] = np.where(
        df['n'] > 1,
        (df['sum_sq_available'] - (df['sum_available']**2 / df['n'])) / (df['n'] - 1),
        0
    ).clip(min=0)
    df['std_err'] = np.sqrt(df['variance']) / np.sqrt(df['n'])
    df['ci_99_lower'] = (df['mean_available'] - (Z_SCORE_99 * df['std_err'])).clip(lower=0)
    df['ci_99_upper'] = df['mean_available'] + (Z_SCORE_99 * df['std_err'])
    df['prob_full'] = (df['full_count'] / df['n']).round(4)

    readable_output = []
    
    # Group by Facility, Holiday Status, and Day
    group_cols = ['facility_name', 'is_school_holiday', 'day_of_week']
    for (facility_raw, is_holiday, day_num), group in df.groupby(group_cols):
        sorted_group = group.sort_values('time_bin')
        
        pretty_name = NAME_MAPPING.get(facility_raw, facility_raw)
        day_name = DAYS[day_num]
        
        # Calculate Fill Time Label
        full_bins = sorted_group[sorted_group['prob_full'] >= FULL_THRESHOLD]
        fill_time_bin = full_bins['time_bin'].iloc[0] if not full_bins.empty else None
        fill_time_label = get_time_label(int(fill_time_bin)) if fill_time_bin is not None else "Never"
        
        # Calculate Empty Time Label
        empty_time_label = "Unknown"
        if not full_bins.empty:
            min_avail_idx = sorted_group['mean_available'].idxmin()
            post_peak = sorted_group.loc[min_avail_idx:]
            recovery = post_peak[post_peak['mean_available'] > (post_peak['mean_available'].min() + 5)]
            empty_bin = recovery['time_bin'].iloc[0] if not recovery.empty else full_bins['time_bin'].iloc[-1]
            empty_time_label = get_time_label(int(empty_bin))

        # Generate Time Series
        clean_series = []
        for _, row in sorted_group.iterrows():
            clean_series.append({
                "bin": int(row['time_bin']),
                "label": get_time_label(int(row['time_bin'])),
                "avg": round(float(row['mean_available']), 1),
                "low": round(float(row['ci_99_lower']), 1),
                "high": round(float(row['ci_99_upper']), 1),
                "full_prob": round(float(row['prob_full']), 3),
                "low_data": "YES" if row['n'] <= LOW_OBSERVATION_THRESHOLD else "NO"
            })

        readable_output.append({
            "facility": pretty_name,
            "day": day_name,
            "day_priority": DAY_SORT_ORDER[day_name],
            "status": "Holiday" if is_holiday else "Term Time",
            "summary": {
                "fill_time": fill_time_label,
                "empty_time": empty_time_label,
                "max_full_prob": float(sorted_group['prob_full'].max())
            },
            "series": clean_series
        })

    # Sort by Facility name first, then by the custom day_priority (0=Mon, 6=Sun)
    readable_output.sort(key=lambda x: (x['facility'], x['status'], x['day_priority']))

    # Save Output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(readable_output, f, indent=2)
    
    print(f"Successfully generated readable insights at {output_path}")

if __name__ == "__main__":
    process_insights('data/processed/master_stats.csv', 'data/processed/insights.json')