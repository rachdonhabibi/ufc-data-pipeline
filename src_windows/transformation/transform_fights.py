import pandas as pd

INPUT_CSV = r'd:\1 Projects\UFC\data\raw\all_event_fights.csv'
OUTPUT_CSV = r'd:\1 Projects\UFC\data\processed\all_event_fights_transformed.csv'

METHOD_MAP = {
    "KO/TKO": "Knockout or Technical Knockout",
    "U-DEC": "Unanimous Decision",
    "S-DEC": "Split Decision",
    "M-DEC": "Majority Decision",
    "SUB": "Submission",
    "DQ": "Disqualification",
    "CNC": "Cancelled",
    "NC": "No Contest"
}

def time_to_seconds(t):
    try:
        m, s = map(int, str(t).split(':'))
        return m * 60 + s
    except:
        return None

def extract_id(url, key):
    try:
        return url.split(f'/{key}/')[-1] if pd.notnull(url) else ""
    except Exception:
        return ""

def extract_fight_id(url):
    return extract_id(url, 'fight-details')

def extract_fighter_id(url):
    return extract_id(url, 'fighter-details')

def extract_event_id(url):
    return extract_id(url, 'event-details')

def main():
    df = pd.read_csv(INPUT_CSV)
    df['fight_url'] = df['fight_url'].apply(extract_fight_id)
    df['fighter_1_url'] = df['fighter_1_url'].apply(extract_fighter_id)
    df['fighter_2_url'] = df['fighter_2_url'].apply(extract_fighter_id)
    df['event_url'] = df['event_url'].apply(extract_event_id)
    df['winner'] = df['winner'].apply(extract_fighter_id)
    df['round'] = df['round'].astype('Int64')
    df['time_seconds'] = df['time'].apply(time_to_seconds)
    df['weight_class'] = df['weight_class'].astype('category')
    df['method'] = df['method'].astype('category')
    df['method_type'] = df['method'].map(METHOD_MAP)
    df['method_type'] = df['method_type'].astype('category')
    df['method_type'] = df['method_type'].cat.add_categories(["Other"])
    df['method_type'] = df['method_type'].fillna("Other")
    df['total_fight_time_seconds'] = ((df['round'] - 1) * 5 * 60).fillna(0) + df['time_seconds'].fillna(0)
    df = df[['fight_url', 'fighter_1_url', 'fighter_2_url', 'event_url',
             'weight_class', 'method', 'method_type', 'round', 'time', 'time_seconds',
             'total_fight_time_seconds', 'winner']]
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Transformed CSV saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()