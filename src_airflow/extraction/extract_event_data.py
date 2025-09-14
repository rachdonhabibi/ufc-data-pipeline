import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "http://ufcstats.com/statistics/events/completed"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_event_links(test_limit: int = 50):
    events = []
    page = 1
    while len(events) < test_limit:
        url = f"{BASE_URL}?page={page}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        table = soup.find("table", class_="b-statistics__table-events")
        if not table:
            break
        tbody = table.find("tbody")
        if not tbody:
            break

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
            location = tds[1].get_text(strip=True)
            events.append({
                "event_name": event_name,
                "event_url": event_url,
                "event_date": event_date_raw,
                "event_date_iso": event_dt.strftime("%Y-%m-%d") if pd.notnull(event_dt) else None,
                "location": location,
            })
            if len(events) >= test_limit:
                break

        # Si moins de 25 événements sur la page, c'est la dernière
        if len(tbody.find_all("tr")) < 25 or len(events) >= test_limit:
            break
        page += 1

    return events

if __name__ == "__main__":
    test_limit = 7
    events = get_event_links(test_limit=test_limit)
    df = pd.DataFrame(events)
    output_path = "/opt/airflow/data_airflow/raw/ufc_events.csv"
    df.to_csv(output_path, index=False)
    print(f"Scraped {len(events)} event(s) data saved to {output_path}")
