import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
#
BASE_URL = "http://ufcstats.com/statistics/events/completed"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_event_links(skip_upcoming: bool = True):
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", class_="b-statistics__table-events")
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []

    today = pd.Timestamp.today().normalize()
    events = []
    for row in tbody.find_all("tr"):
        a_tag = row.find("a", class_="b-link")
        date_span = row.find("span", class_="b-statistics__date")
        tds = row.find_all("td")
        if not (a_tag and date_span and len(tds) > 1):
            continue

        event_name = a_tag.get_text(strip=True)
        event_url = a_tag.get("href")
        event_date_raw = date_span.get_text(strip=True)
        event_dt = pd.to_datetime(event_date_raw, errors="coerce")
        if skip_upcoming and pd.notnull(event_dt) and event_dt.normalize() > today:
            continue  # ignore upcoming events that appear at the top

        location = tds[1].get_text(strip=True)
        events.append({
            "event_name": event_name,
            "event_url": event_url,
            "event_date": event_date_raw,              # original text (e.g., "August 23, 2025")
            "event_date_iso": event_dt.strftime("%Y-%m-%d") if pd.notnull(event_dt) else None,  # ISO date
            "location": location,
        })
    return events

if __name__ == "__main__":
    events = get_event_links()
    test_limit = 2
    events = events[:test_limit]
    df = pd.DataFrame(events)
    output_path = r"D:\1 Projects\UFC\data\raw\ufc_events.csv"
    df.to_csv(output_path, index=False)
    print(f"Scraped {len(events)} event(s) data saved to {output_path}")
