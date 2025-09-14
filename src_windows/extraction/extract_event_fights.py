import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}

def _clean(s):
    return re.sub(r"\s+", " ", s).strip() if s else None

def _fight_link(tr):
    # Prefer data-link, fallback to onclick or an <a> href
    url = tr.get("data-link")
    if url:
        return url
    

def get_event_fights(event_url: str):
    resp = requests.get(event_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", class_="b-fight-details__table")
    if not table:
        return []

    fights = []
    for tr in table.select("tbody tr.b-fight-details__table-row"):
        classes = tr.get("class") or []
        if "js-fight-details-click" not in classes:
            continue

        tds = tr.find_all("td")
        if len(tds) < 10:
            continue

        # Fighter URLs (first two black links)
        a_tags = tr.select("a.b-link.b-link_style_black")
        f1_url = a_tags[0].get("href") if len(a_tags) > 0 else None
        f2_url = a_tags[1].get("href") if len(a_tags) > 1 else None

        # Winner: if W/L cell contains "win", the first fighter is winner; else "DRAW"
        wl_text = _clean(tds[0].get_text(" ", strip=True)).lower()
        winner = f1_url if ("win" in wl_text) else "DRAW"

        # Weight class (first line only)
        wc_text = tds[6].get_text("\n", strip=True)
        weight_class = _clean(wc_text.split("\n")[0])

        # Method (first line only)
        method_text = tds[7].get_text("\n", strip=True)
        method = _clean(method_text.split("\n")[0])

        # Round and Time
        round_ = _clean(tds[8].get_text(strip=True))
        time_ = _clean(tds[9].get_text(strip=True))

        fight_url = _fight_link(tr)

        fights.append({
            "fight_url": fight_url,
            "fighter_1_url": f1_url,
            "fighter_2_url": f2_url,
            "event_url": event_url,
            "weight_class": weight_class,
            "method": method,
            "round": round_,
            "time": time_,
            "winner": winner,
        })
    return fights

if __name__ == "__main__":
    events_df = pd.read_csv(r"D:\1 Projects\UFC\data\raw\ufc_events.csv")
    rows = []
    for _, row in events_df.iterrows():
        try:
            rows.extend(get_event_fights(row["event_url"]))
            time.sleep(0.4)
        except Exception as e:
            print(f"[warn] {row['event_url']}: {e}")

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["fight_url"]).drop_duplicates(subset=["fight_url"])
    df = df[["fight_url","fighter_1_url","fighter_2_url","event_url","weight_class","method","round","time","winner"]]
    out = r"D:\1 Projects\UFC\data\raw\all_event_fights.csv"
    df.to_csv(out, index=False)
    print(f"Saved {len(df)} fights -> {out}")