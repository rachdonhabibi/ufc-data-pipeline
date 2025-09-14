import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

INPUT_CSV = Path(r'd:\1 Projects\UFC\data\raw\all_event_fights.csv')
OUTPUT_CSV = Path(r'd:\1 Projects\UFC\data\raw\fight_significant_strikes_totals.csv')
HEADERS = {"User-Agent": "Mozilla/5.0"}

COLUMNS = [
    "fighter_url",
    "fight_url",
    "Sig. str",
    "Sig. str. %",
    "Head",
    "Body",
    "Leg",
    "Distance",
    "Clinch",
    "Ground"
]

def extract_significant_strikes_totals(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"Failed to get significant strikes totals for {url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    expected_headers = [
        "Fighter", "Sig. str", "Sig. str. %", "Head", "Body", "Leg", "Distance", "Clinch", "Ground"
    ]
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead:
            continue
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
        if headers == expected_headers:
            tbody = table.find("tbody")
            if not tbody:
                continue
            tr = tbody.find("tr")
            if not tr:
                continue
            tds = tr.find_all("td")
            if len(tds) != 9:
                continue
            fighter_links = tds[0].find_all("a")
            if len(fighter_links) != 2:
                continue
            fighter_urls = [a.get("href", "").strip() for a in fighter_links]
            rows = []
            for fidx in range(2):
                row = {"fighter_url": fighter_urls[fidx]}
                for col, td in zip(COLUMNS[2:], tds[1:]):
                    ps = td.find_all("p")
                    val = ps[fidx].get_text(strip=True) if len(ps) > fidx else ""
                    row[col] = val
                rows.append(row)
            return rows
    return []

if __name__ == "__main__":
    fights_df = pd.read_csv(INPUT_CSV, dtype=str, encoding="utf-8-sig")
    fight_urls = fights_df.get("fight_url", pd.Series([], dtype=str)).dropna().tolist()

    results = []
    total = len(fight_urls)
    for i, url in enumerate(fight_urls, 1):
        print(f"[{i}/{total}] {url}")
        rows = extract_significant_strikes_totals(url)
        for r in rows:
            r["fight_url"] = url
            results.append(r)
        time.sleep(1)

    df = pd.DataFrame(results, columns=COLUMNS)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Done. Output: {OUTPUT_CSV}")