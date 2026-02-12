import pandas as pd
import numpy as np
import calendar
import json
import os

# --- Configuration ---
LOW_OBSERVATION_THRESHOLD = 5
FULL_PROB_THRESHOLD = 0.80  # X%: Must be full at least 80% of observed days
RECOVERY_SPOTS = 10         # N: Number of spots to signal "availability"
RECOVERY_PROB = 0.20        # The chance of being full must drop significantly

# Facility Name Mapping
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

DAYS = list(calendar.day_name)
DAY_SORT_ORDER = {name: i for i, name in enumerate(DAYS)}

def get_time_label(bin_index):
    hour = bin_index // 6
    minute = (bin_index % 6) * 10
    period = "AM" if hour < 12 else "PM"
    display_hour = hour % 12
    if display_hour == 0: display_hour = 12
    return f"{display_hour}:{minute:02d} {period}"

def process_insights(stats_csv_path, output_path):
    if not os.path.exists(stats_csv_path):
        print(f"Error: {stats_csv_path} not found.")
        return

    df = pd.read_csv(stats_csv_path)
    
    # Statistical Calculations
    df['mean_available'] = df['sum_available'] / df['n']
    df['variance'] = np.where(
        df['n'] > 1.1,
        (df['sum_sq_available'] - (df['sum_available']**2 / df['n'])) / (df['n'] - 1),
        0
    ).clip(min=0)
    df['std_err'] = np.sqrt(df['variance']) / np.sqrt(df['n'])
    df['prob_full'] = (df['full_count'] / df['n']).round(4)

    readable_output = []
    
    group_cols = ['facility_name', 'is_school_holiday', 'day_of_week']
    for (facility_raw, is_school_holiday, day_num), group in df.groupby(group_cols):
        sorted_group = group.sort_values('time_bin')
        
        # 1. Aggregate Low Data Flag
        is_low_data_aggregate = (group['n'] <= LOW_OBSERVATION_THRESHOLD).any()

        pretty_name = NAME_MAPPING.get(facility_raw, facility_raw)
        day_name = DAYS[day_num]
        
        # 2. Probability-based Fill/Empty Logic
        max_daily_prob = sorted_group['prob_full'].max()
        
        if max_daily_prob < FULL_PROB_THRESHOLD:
            fill_time_label = "Rarely full"
            empty_time_label = "Available"
        else:
            # Calculate Fill Time (First bin hitting threshold)
            typical_fill_bins = sorted_group[sorted_group['prob_full'] >= FULL_PROB_THRESHOLD]
            fill_time_bin = typical_fill_bins['time_bin'].iloc[0]
            fill_time_label = get_time_label(int(fill_time_bin))

            # Calculate Empty Time (Recovery threshold)
            min_avail_idx = sorted_group['mean_available'].idxmin()
            post_peak = sorted_group.loc[min_avail_idx:]

            recovery = post_peak[
                (post_peak['mean_available'] >= RECOVERY_SPOTS) & 
                (post_peak['prob_full'] < RECOVERY_PROB)
            ]

            if not recovery.empty:
                empty_bin = recovery['time_bin'].iloc[0]
                empty_time_label = get_time_label(int(empty_bin))
            else:
                # Fallback if data ends while still full
                empty_time_label = "After 10:00PM"

        # 3. Generate Time Series
        clean_series = []
        for _, row in sorted_group.iterrows():
            clean_series.append({
                "bin": int(row['time_bin']),
                "label": get_time_label(int(row['time_bin'])),
                "avg": round(float(row['mean_available']), 1),
                "se": round(float(row['std_err']), 2),
                "full_prob": round(float(row['prob_full']), 3)
            })

        readable_output.append({
            "facility": pretty_name,
            "day": day_name,
            "day_priority": DAY_SORT_ORDER[day_name],
            "status": "School Holiday" if is_school_holiday else "Normal Period",
            "low_data_warning": bool(is_low_data_aggregate),
            "summary": {
                "fill_time": fill_time_label,
                "empty_time": empty_time_label,
                "max_full_prob": float(max_daily_prob)
            },
            "series": clean_series
        })

    # Sort Output
    readable_output.sort(key=lambda x: (x['facility'], x['status'], x['day_priority']))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(readable_output, f, indent=2)
    
    print(f"Successfully generated insights at {output_path}")

if __name__ == "__main__":
    process_insights('data/processed/master_stats.csv', 'data/processed/insights.json')