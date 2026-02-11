import pandas as pd
import numpy as np
import json
import os

# Configuration
LOW_OBSERVATION_THRESHOLD = 5  # Adjust as needed
Z_SCORE_99 = 2.576
FULL_THRESHOLD = 0.8  # 80% prob of being full

def process_insights(stats_csv_path, output_path):
    if not os.path.exists(stats_csv_path):
        print(f"Error: {stats_csv_path} not found.")
        return

    df = pd.read_csv(stats_csv_path)
    
    # 1. Statistical Calculations
    # Calculate mean
    df['mean_available'] = df['sum_available'] / df['n']
    
    # Calculate variance: n=1 results in 0, n>1 uses Bessel's correction
    df['variance'] = np.where(
        df['n'] > 1,
        (df['sum_sq_available'] - (df['sum_available']**2 / df['n'])) / (df['n'] - 1),
        0
    ).clip(min=0)
    
    # Standard Error for CI
    # If variance is 0 (like when n=1), std_err becomes 0
    df['std_err'] = np.sqrt(df['variance']) / np.sqrt(df['n'])
    
    # 2. Confidence Intervals & Quality Flags
    df['ci_99_lower'] = (df['mean_available'] - (Z_SCORE_99 * df['std_err'])).clip(lower=0)
    df['ci_99_upper'] = df['mean_available'] + (Z_SCORE_99 * df['std_err'])
    
    # Requirement: Flag for low observations
    df['low_data_flag'] = (df['n'] <= LOW_OBSERVATION_THRESHOLD).map({True: "YES", False: "NO"})
    
    # 3. Fullness Probability
    df['prob_full'] = (df['full_count'] / df['n']).round(4)
    
    # 4. Identify Key Events (Time of Day Insights)
    insights_summary = []
    
    for (facility, is_holiday), group in df.groupby(['facility_name', 'is_school_holiday']):
        sorted_group = group.sort_values('time_bin')
        
        # Insight: Typical Fill Time
        full_bins = sorted_group[sorted_group['prob_full'] >= FULL_THRESHOLD]
        fill_time = str(full_bins['time_bin'].iloc[0]) if not full_bins.empty else "Never"
        
        # Insight: Typical Emptying Time
        # Logic: Find the bin with the lowest mean availability, 
        # then find the first time AFTER that where availability increases by > 5%
        emptying_time = "Unknown"
        if not full_bins.empty:
            min_avail_idx = sorted_group['mean_available'].idxmin()
            post_peak = sorted_group.loc[min_avail_idx:]
            
            # Find first bin where availability is significantly higher than the minimum
            recovery = post_peak[post_peak['mean_available'] > (post_peak['mean_available'].min() + 5)]
            if not recovery.empty:
                emptying_time = str(recovery['time_bin'].iloc[0])
            else:
                emptying_time = str(full_bins['time_bin'].iloc[-1]) # Fallback to last full bin

        insights_summary.append({
            "facility": facility,
            "is_holiday": bool(is_holiday),
            "summary": {
                "fill_time_est": fill_time,
                "empty_time_est": emptying_time,
                "avg_daily_max_prob_full": float(sorted_group['prob_full'].max())
            },
            "series": sorted_group.to_dict(orient='records')
        })

    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save to final destination for GitHub Pages
    with open(output_path, 'w') as f:
        json.dump(insights_summary, f, indent=2)
    print(f"Insights generated at {output_path}")

if __name__ == "__main__":
    process_insights('data/processed/master_stats.csv', 'data/processed/insights.json')