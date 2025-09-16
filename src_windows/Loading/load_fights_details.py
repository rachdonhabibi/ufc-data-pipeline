import pandas as pd
import psycopg2
#
CSV_PATH = r'd:\1 Projects\UFC\data\processed\fight_details.csv'

DB_CONFIG = {
    'dbname': 'UFC',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': 5432
}

def get_fighter_id(cur, fighter_url):
    cur.execute("SELECT fighter_id FROM fighters WHERE fighter_url = %s", (fighter_url,))
    result = cur.fetchone()
    return result[0] if result else None

def get_fight_id(cur, fight_url):
    cur.execute("SELECT fight_id FROM fights WHERE fight_url = %s", (fight_url,))
    result = cur.fetchone()
    return result[0] if result else None

def main():
    df = pd.read_csv(CSV_PATH)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        fighter_id = get_fighter_id(cur, row['fighter_url'])
        fight_id = get_fight_id(cur, row['fight_url'])

        cur.execute("""
            INSERT INTO fight_details (
                fighter_id, fight_id, referee,
                significant_strikes_landed, significant_strikes_attempted, significant_strike_accuracy,
                head_strikes_landed, head_strikes_attempted,
                body_strikes_landed, body_strikes_attempted,
                leg_strikes_landed, leg_strikes_attempted,
                distance_strikes_landed, distance_strikes_attempted,
                clinch_strikes_landed, clinch_strikes_attempted,
                ground_strikes_landed, ground_strikes_attempted,
                total_strikes_landed, total_strikes_attempted,
                takedowns_landed, takedowns_attempted, takedown_accuracy,
                knockdowns, submission_attempts, reversals, control_time_seconds,
                significant_strike_accuracy_opponent
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
        """, (
            fighter_id, fight_id, row['referee'],
            row['significant_strikes_landed'], row['significant_strikes_attempted'], row['significant_strike_accuracy'],
            row['head_strikes_landed'], row['head_strikes_attempted'],
            row['body_strikes_landed'], row['body_strikes_attempted'],
            row['leg_strikes_landed'], row['leg_strikes_attempted'],
            row['distance_strikes_landed'], row['distance_strikes_attempted'],
            row['clinch_strikes_landed'], row['clinch_strikes_attempted'],
            row['ground_strikes_landed'], row['ground_strikes_attempted'],
            row['total_strikes_landed'], row['total_strikes_attempted'],
            row['takedowns_landed'], row['takedowns_attempted'], row['takedown_accuracy'],
            row['knockdowns'], row['submission_attempts'], row['reversals'], row['control_time_seconds'],
            row['significant_strike_accuracy_opponent']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Loaded fight_details table.")

if __name__ == "__main__":
    main()