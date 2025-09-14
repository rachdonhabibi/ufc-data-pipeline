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

def main():
    df = pd.read_csv(CSV_PATH)
    geo_df = df[['city', 'state_or_region', 'country']].drop_duplicates()
    geo_df['city'] = geo_df['city'].fillna('').astype(str)
    geo_df['state_or_region'] = geo_df['state_or_region'].fillna('').astype(str)
    geo_df['country'] = geo_df['country'].fillna('').astype(str)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Make sure unique constraint exists in geo_dim
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'unique_geo'
            ) THEN
                ALTER TABLE geo_dim ADD CONSTRAINT unique_geo UNIQUE (city, state_or_region, country);
            END IF;
        END
        $$;
    """)

    for _, row in geo_df.iterrows():
        cur.execute("""
            INSERT INTO geo_dim (city, state_or_region, country)
            VALUES (%s, %s, %s)
            ON CONFLICT ON CONSTRAINT unique_geo DO NOTHING
        """, (row['city'], row['state_or_region'], row['country']))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(geo_df)} unique locations into geo_dim.")

if __name__ == "__main__":
    main()