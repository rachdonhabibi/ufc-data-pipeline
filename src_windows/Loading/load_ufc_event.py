import pandas as pd
import psycopg2

CSV_PATH = r'd:\1 Projects\UFC\data\processed\ufc_events_transformed.csv'

DB_CONFIG = {
    'dbname': 'UFC',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': 5432
}

def get_geo_id(cur, city, state_or_region, country):
    cur.execute("""
        SELECT geo_id FROM geo_dim
        WHERE city = %s AND state_or_region = %s AND country = %s
        LIMIT 1
    """, (city, state_or_region, country))
    result = cur.fetchone()
    return result[0] if result else None

def main():
    df = pd.read_csv(CSV_PATH)
    df['city'] = df['city'].fillna('').astype(str)
    df['state_or_region'] = df['state_or_region'].fillna('').astype(str)
    df['country'] = df['country'].fillna('').astype(str)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Make sure unique constraint exists in events
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'unique_event'
            ) THEN
                ALTER TABLE events ADD CONSTRAINT unique_event UNIQUE (event_name, event_date);
            END IF;
        END
        $$;
    """)

    for _, row in df.iterrows():
        geo_id = get_geo_id(cur, row['city'], row['state_or_region'], row['country'])
        cur.execute("""
            INSERT INTO events (event_name, event_date, event_url, geo_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT unique_event DO NOTHING
        """, (row['event_name'], row['event_date'], row['event_url'], geo_id))
    conn.commit()
    cur.close()
    conn.close()
    print("Loaded events table.")

if __name__ == "__main__":
    main()