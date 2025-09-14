import pandas as pd

INPUT_CSV = r'd:\1 Projects\UFC\data\raw\ufc_events.csv'
OUTPUT_CSV = r'd:\1 Projects\UFC\data\processed\ufc_events_transformed.csv'

def split_location(location):
    parts = [p.strip() for p in location.split(',')]
    if len(parts) == 3:
        city, state_or_region, country = parts
    elif len(parts) == 2:
        city, country = parts
        state_or_region = ""
    elif len(parts) == 1:
        city = parts[0]
        state_or_region = ""
        country = ""
    else:
        city = state_or_region = country = ""
    return pd.Series([city, state_or_region, country])

def extract_event_id(url):
    return url.split('/event-details/')[-1] if pd.notnull(url) else ""

def main():
    df = pd.read_csv(INPUT_CSV)
    df['event_date'] = pd.to_datetime(df['event_date_iso'])
    df[['city', 'state_or_region', 'country']] = df['location'].apply(split_location)
    df['event_url'] = df['event_url'].apply(extract_event_id)
    df = df[['event_name', 'event_date', 'city', 'state_or_region', 'country', 'event_url']]
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Transformed CSV saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()