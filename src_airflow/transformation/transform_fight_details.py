import pandas as pd
import re

SIGNIFICANT_STRIKES_CSV = '/opt/airflow/data_airflow/raw/fight_significant_strikes_totals.csv'
FIGHT_TOTALS_CSV = '/opt/airflow/data_airflow/raw/fight_totals.csv'
OUTPUT_CSV = '/opt/airflow/data_airflow/processed/fight_details.csv'

def split_x_of_y(val):
    m = re.match(r"(\d+)\s*of\s*(\d+)", str(val))
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None

def percent_to_float(val):
    if pd.isna(val): return None
    val = str(val).replace('%', '').strip()
    try:
        return float(val) / 100
    except:
        return None

def ctrl_to_seconds(val):
    if pd.isna(val): return None
    m = re.match(r"(\d+):(\d+)", str(val))
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    return None

def td_split(val):
    if pd.isna(val): return None, None
    if val == "---": return None, None
    return split_x_of_y(val)

def extract_fighter_id(url):
    return url.split('/fighter-details/')[-1] if pd.notnull(url) else ""

def extract_fight_id(url):
    return url.split('/fight-details/')[-1] if pd.notnull(url) else ""

def main():
    df_strikes = pd.read_csv(SIGNIFICANT_STRIKES_CSV)
    df_totals = pd.read_csv(FIGHT_TOTALS_CSV)

    # Extract only the unique IDs for fighter_url and fight_url
    df_strikes['fighter_url'] = df_strikes['fighter_url'].apply(extract_fighter_id)
    df_strikes['fight_url'] = df_strikes['fight_url'].apply(extract_fight_id)
    df_totals['fighter_url'] = df_totals['fighter_url'].apply(extract_fighter_id)
    df_totals['fight_url'] = df_totals['fight_url'].apply(extract_fight_id)

    # Join on fight_url and fighter_url (if both have fighter_url, otherwise just fight_url)
    join_cols = ['fight_url', 'fighter_url'] if 'fighter_url' in df_strikes.columns and 'fighter_url' in df_totals.columns else ['fight_url']
    df_merged = pd.merge(df_strikes, df_totals, on=join_cols, how='inner')

    # Split "X of Y" columns
    for col, new_base in [
        ('Sig. str', 'significant_strikes'),
        ('Head', 'head_strikes'),
        ('Body', 'body_strikes'),
        ('Leg', 'leg_strikes'),
        ('Distance', 'distance_strikes'),
        ('Clinch', 'clinch_strikes'),
        ('Ground', 'ground_strikes'),
        ('Total str.', 'total_strikes'),
    ]:
        landed, attempted = zip(*df_merged[col].map(split_x_of_y))
        df_merged[f'{new_base}_landed'] = landed
        df_merged[f'{new_base}_attempted'] = attempted

    # Split Td (Takedowns)
    td_landed, td_attempted = zip(*df_merged['Td'].map(td_split))
    df_merged['takedowns_landed'] = td_landed
    df_merged['takedowns_attempted'] = td_attempted

    # Convert percentage columns
    df_merged['significant_strike_accuracy'] = df_merged['Sig. str. %_x'].map(percent_to_float)
    df_merged['significant_strike_accuracy_opponent'] = df_merged['Sig. str. %_y'].map(percent_to_float)
    df_merged['takedown_accuracy'] = df_merged['Td %'].map(percent_to_float)

    # Convert KD, Sub. att, Rev. to integer
    df_merged['knockdowns'] = df_merged['KD'].astype(int)
    df_merged['submission_attempts'] = df_merged['Sub. att'].astype(int)
    df_merged['reversals'] = df_merged['Rev.'].astype(int)

    # Convert Ctrl to seconds
    df_merged['control_time_seconds'] = df_merged['Ctrl'].map(ctrl_to_seconds)

    # Select and rename columns
    df_clean = df_merged[[
        'fighter_url', 'fight_url', 'referee',
        'significant_strikes_landed', 'significant_strikes_attempted', 'significant_strike_accuracy',
        'head_strikes_landed', 'head_strikes_attempted',
        'body_strikes_landed', 'body_strikes_attempted',
        'leg_strikes_landed', 'leg_strikes_attempted',
        'distance_strikes_landed', 'distance_strikes_attempted',
        'clinch_strikes_landed', 'clinch_strikes_attempted',
        'ground_strikes_landed', 'ground_strikes_attempted',
        'total_strikes_landed', 'total_strikes_attempted',
        'takedowns_landed', 'takedowns_attempted', 'takedown_accuracy',
        'knockdowns', 'submission_attempts', 'reversals', 'control_time_seconds',
        'significant_strike_accuracy_opponent'
    ]]

    df_clean.to_csv(OUTPUT_CSV, index=False)
    print(f"Output saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()