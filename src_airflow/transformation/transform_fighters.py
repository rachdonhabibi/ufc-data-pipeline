import pandas as pd
import re
from datetime import datetime

INPUT_CSV = '/opt/airflow/data_airflow/raw/fighters.csv'
OUTPUT_CSV = '/opt/airflow/data_airflow/processed/fighters_cleaned.csv'

def height_to_cm(height):
    m = re.match(r"(\d+)'[\s]*(\d+)", str(height))
    if m:
        feet, inches = int(m.group(1)), int(m.group(2))
        return int(round(feet * 30.48 + inches * 2.54))
    return None

def weight_to_kg(weight):
    m = re.match(r"(\d+)\s*lbs", str(weight))
    if m:
        lbs = int(m.group(1))
        return int(round(lbs * 0.453592))
    return None

def reach_to_cm(reach):
    m = re.match(r"(\d+)", str(reach))
    if m:
        inches = int(m.group(1))
        return int(round(inches * 2.54))
    return None

def parse_record(record):
    record = str(record).strip()
    wins = losses = draws = nc = None
    m = re.match(r"(\d+)[-/](\d+)[-/](\d+)(?:\s*\((\d+)\s*NC\))?", record)
    if m:
        wins, losses, draws = int(m.group(1)), int(m.group(2)), int(m.group(3))
        nc = int(m.group(4)) if m.group(4) else None
        return pd.Series([wins, losses, draws, nc])
    m = re.match(r"(\d+)[/](\d+)[/](\d+)", record)
    if m:
        wins, losses, draws = int(m.group(1)), int(m.group(2)), int(m.group(3))
        nc = None
        return pd.Series([wins, losses, draws, nc])
    return pd.Series([None, None, None, None])

def percent_to_float(val):
    if pd.isna(val): return None
    val = str(val).replace('%', '').strip()
    try:
        return float(val)
    except:
        return None

def calc_age(dob):
    if pd.isna(dob): return None
    today = datetime.today()
    try:
        dob_dt = pd.to_datetime(dob, errors='coerce')
        if pd.isna(dob_dt): return None
        return today.year - dob_dt.year - ((today.month, today.day) < (dob_dt.month, dob_dt.day))
    except:
        return None

def extract_fighter_id(url):
    return url.split('/fighter-details/')[-1] if pd.notnull(url) else ""

def main():
    df = pd.read_csv(INPUT_CSV)
    df['fighter_url'] = df['fighter_url'].apply(extract_fighter_id)
    df['height_cm'] = df['height'].apply(height_to_cm)
    df['weight_kg'] = df['weight'].apply(weight_to_kg)
    df['reach_cm'] = df['reach'].apply(reach_to_cm)
    df['dob'] = pd.to_datetime(df['dob'], errors='coerce')
    df[['wins', 'losses', 'draws', 'nc']] = df['record'].apply(parse_record)
    df['age'] = df['dob'].apply(calc_age)
    df['reach_to_height_ratio'] = df['reach_cm'] / df['height_cm']
    df['total_fights'] = df[['wins', 'losses', 'draws', 'nc']].fillna(0).sum(axis=1).astype(int)

    df['significant_strikes_landed_per_minute'] = df['slpm'].astype(float)
    df['significant_strike_accuracy'] = df['str_acc'].apply(percent_to_float)
    df['significant_strikes_absorbed_per_minute'] = df['sapm'].astype(float)
    df['significant_strike_defense'] = df['str_def'].apply(percent_to_float)
    df['takedown_average_per_15_min'] = df['td_avg'].astype(float)
    df['takedown_accuracy'] = df['td_acc'].apply(percent_to_float)
    df['takedown_defense'] = df['td_def'].apply(percent_to_float)
    df['submission_average_per_15_min'] = df['sub_avg'].astype(float)

    df = df[['fighter_name', 'fighter_url', 'height_cm', 'weight_kg', 'reach_cm', 'stance', 'dob', 'age',
             'wins', 'losses', 'draws', 'nc', 'total_fights', 'reach_to_height_ratio',
             'record',
             'significant_strikes_landed_per_minute',
             'significant_strike_accuracy',
             'significant_strikes_absorbed_per_minute',
             'significant_strike_defense',
             'takedown_average_per_15_min',
             'takedown_accuracy',
             'takedown_defense',
             'submission_average_per_15_min'
    ]]
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Cleaned fighters saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()