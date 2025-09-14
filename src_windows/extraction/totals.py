import csv
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

INPUT_CSV = Path(r'd:\1 Projects\UFC\data\raw\all_event_fights.csv')
OUTPUT_CSV = Path(r'd:\1 Projects\UFC\data\raw\fight_totals.csv')
HEADERS = {"User-Agent": "Mozilla/5.0"}

COLUMNS = [
    "fighter_url",
    "fight_url",
    "referee",
    "KD",
    "Sig. str.",
    "Sig. str. %",
    "Total str.",
    "Td",
    "Td %",
    "Sub. att",
    "Rev.",
    "Ctrl"
]

def extract_referee(soup):
    # Find the referee name in the fight details section
    ref_span = soup.find("i", string=lambda s: s and "Referee:" in s)
    if ref_span:
        parent = ref_span.parent
        span = parent.find("span")
        if span:
            return span.get_text(strip=True)
    return ""

def extract_totals_table(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[warn] GET {url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    referee = extract_referee(soup)

    # Cherche la bonne section/table
    for section in soup.select('section.b-fight-details__section.js-fight-section'):
        table = section.find('table')
        if not table:
            continue
        thead = table.find('thead')
        if not thead:
            continue
        headers = [th.get_text(strip=True) for th in thead.find_all('th')]
        if headers[:5] == ["Fighter", "KD", "Sig. str.", "Sig. str. %", "Total str."]:
            tbody = table.find('tbody')
            if not tbody:
                continue
            tr = tbody.find('tr')
            if not tr:
                continue
            tds = tr.find_all('td')
            if len(tds) != 10:
                continue
            fighter_links = tds[0].find_all('a')
            if len(fighter_links) != 2:
                continue
            fighter_urls = [a.get("href", "").strip() for a in fighter_links]
            rows = []
            for fidx in range(2):
                row = {
                    "fighter_url": fighter_urls[fidx],
                    "referee": referee
                }
                for col, td in zip(COLUMNS[3:], tds[1:]):
                    ps = td.find_all('p')
                    val = ps[fidx].get_text(strip=True) if len(ps) > fidx else ""
                    row[col] = val
                rows.append(row)
            return rows
    return []

def main():
    fights_df = pd.read_csv(INPUT_CSV, dtype=str, encoding="utf-8-sig")
    fight_urls = fights_df.get("fight_url", pd.Series([], dtype=str)).dropna().tolist()

    results = []
    total = len(fight_urls)
    for i, url in enumerate(fight_urls, 1):
        if i % 25 == 0 or i == 1:
            print(f"[info] totals {i}/{total}")
        rows = extract_totals_table(url)
        for r in rows:
            r["fight_url"] = url
            results.append(r)
        time.sleep(0.5)  # Un peu plus long pour Windows

    df = pd.DataFrame(results, columns=COLUMNS)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"[done] {len(df)} rows -> {OUTPUT_CSV}")

if __name__ == "__main__":
    main()