import pandas as pd
import psycopg2

CSV_PATH = '/opt/airflow/data_airflow/processed/fighters_cleaned.csv'

DB_CONFIG = {
    'dbname': 'UFC',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': 5432
}

def clean_row(row):
    # Remove 'significant_strike_defense' from row
    row = row.drop('significant_strike_defense', errors='ignore')
    return [None if (pd.isna(x) or x == '') else x for x in row]

def main():
    df = pd.read_csv(CSV_PATH)
    df = df.fillna('')  # Replace NaN with empty string for all columns

    # Drop the column if present
    if 'significant_strike_defense' in df.columns:
        df = df.drop(columns=['significant_strike_defense'])

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        values = clean_row(row)
        cur.execute("""
            INSERT INTO fighters (
                fighter_name, fighter_url, height_cm, weight_kg, reach_cm, stance, dob, age,
                wins, losses, draws, nc, total_fights, reach_to_height_ratio, record,
                significant_strikes_landed_per_minute, significant_strike_accuracy,
                significant_strikes_absorbed_per_minute,
                takedown_average_per_15_min, takedown_accuracy, takedown_defense,
                submission_average_per_15_min
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT unique_fighter_identity DO NOTHING
        """, values)
    conn.commit()
    cur.close()
    conn.close()
    print("Loaded fighters table.")

if __name__ == "__main__":
    main()