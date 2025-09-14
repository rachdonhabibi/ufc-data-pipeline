import pandas as pd
import psycopg2

CSV_PATH = '/opt/airflow/data_airflow/processed/all_event_fights_transformed.csv'

DB_CONFIG = {
    'dbname': 'UFC',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': 5432
}

def get_fighter_id(cur, fighter_url):
    cur.execute("SELECT fighter_id FROM fighters WHERE fighter_url = %s", (fighter_url,))
    result = cur.fetchone()
    return result[0] if result else None

def get_event_id(cur, event_url):
    cur.execute("SELECT event_id FROM events WHERE event_url = %s", (event_url,))
    result = cur.fetchone()
    return result[0] if result else None

def main():
    df = pd.read_csv(CSV_PATH)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        fighter1_id = get_fighter_id(cur, row['fighter_1_url'])
        fighter2_id = get_fighter_id(cur, row['fighter_2_url'])
        winner_id = get_fighter_id(cur, row['winner']) if pd.notnull(row['winner']) and row['winner'] != '' else None
        event_id = get_event_id(cur, row['event_url'])
        fight_url = row['fight_url']

        cur.execute("""
            INSERT INTO fights (
                fight_url, event_id, fighter1_id, fighter2_id, weight_class, method, method_type,
                round, time, time_seconds, total_fight_time_seconds, winner_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fight_url) DO NOTHING
        """, (
            fight_url, event_id, fighter1_id, fighter2_id, row['weight_class'], row['method'], row['method_type'],
            row['round'], row['time'], row['time_seconds'], row['total_fight_time_seconds'], winner_id
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Loaded fights table.")

if __name__ == "__main__":
    main()