import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

HEADERS = {"User-Agent": "Mozilla/5.0"}

OUT_CSV = "/opt/airflow/data_airflow/raw/fighters.csv"
FIGHTS_CSV = "/opt/airflow/data_airflow/raw/all_event_fights.csv"

def _clean(s):
    return re.sub(r"\s+", " ", s).strip() if s else None

def _strip_label_from_value(li, label_text: str) -> str:
    # li contains "<i>Label:</i> value", return only the "value" part (case-insensitive)
    full = li.get_text(" ", strip=True)
    lt = (label_text or "").strip()
    if lt and full.lower().startswith(lt.lower()):
        return full[len(lt):].lstrip(" :").strip()
    return full

def scrape_fighter_details(url: str) -> dict:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[warn] get {url}: {e}")
        return {
            "fighter_name": None, "fighter_url": url, "height": None, "weight": None, "reach": None,
            "stance": None, "dob": None, "record": None,
            "slpm": None, "str_acc": None, "sapm": None, "str_def": None,
            "td_avg": None, "td_acc": None, "td_def": None, "sub_avg": None
        }

    soup = BeautifulSoup(r.text, "html.parser")

    # Name & Record
    name_tag = soup.select_one("span.b-content__title-highlight")
    fighter_name = _clean(name_tag.get_text(strip=True)) if name_tag else None

    record_tag = soup.select_one("span.b-content__title-record")
    record = None
    if record_tag:
        record = record_tag.get_text(strip=True)
        if record.lower().startswith("record:"):
            record = record[len("Record:"):].strip()

    height = weight = reach = stance = dob = None
    slpm = str_acc = sapm = str_def = td_avg = td_acc = td_def = sub_avg = None

    # Physical stats box
    info_box = soup.select_one("div.b-list__info-box_style_small-width")
    if info_box:
        for li in info_box.select("li"):
            i_tag = li.find("i")
            if not i_tag:
                continue
            label = (i_tag.get_text(strip=True) or "").lower()  # e.g., "height:"
            value = _strip_label_from_value(li, i_tag.get_text(strip=True))
            value = _clean(value)
            if "height" in label:
                height = value
            elif "weight" in label:
                weight = value
            elif "reach" in label:
                reach = value
            elif "stance" in label:
                stance = value
            elif "dob" in label:
                dob = value

    # Career stats box
    stats_box = soup.select_one("div.b-list__info-box_style_middle-width")
    if stats_box:
        for li in stats_box.select("li"):
            i_tag = li.find("i")
            if not i_tag:
                continue
            label = (i_tag.get_text(strip=True) or "").lower()
            value = _strip_label_from_value(li, i_tag.get_text(strip=True))
            value = _clean(value)
            if "slpm" in label:
                slpm = value
            elif "str. acc." in label or "str acc" in label:
                str_acc = value
            elif "sapm" in label:
                sapm = value
            elif "str. def." in label or "str def" in label:
                str_def = value
            elif "td avg." in label or "td avg" in label:
                td_avg = value
            elif "td acc." in label or "td acc" in label:
                td_acc = value
            elif "td def." in label or "td def" in label:
                td_def = value
            elif "sub. avg." in label or "sub avg" in label:
                sub_avg = value

    return {
        "fighter_name": fighter_name,
        "fighter_url": url,
        "height": height,
        "weight": weight,
        "reach": reach,
        "stance": stance,
        "dob": dob,
        "record": record,
        "slpm": slpm,
        "str_acc": str_acc,
        "sapm": sapm,
        "str_def": str_def,
        "td_avg": td_avg,
        "td_acc": td_acc,
        "td_def": td_def,
        "sub_avg": sub_avg,
    }

if __name__ == "__main__":
    fights_df = pd.read_csv(FIGHTS_CSV, dtype=str)
    u1 = fights_df["fighter_1_url"].dropna().tolist() if "fighter_1_url" in fights_df else []
    u2 = fights_df["fighter_2_url"].dropna().tolist() if "fighter_2_url" in fights_df else []
    unique_urls = sorted({u for u in (u1 + u2) if isinstance(u, str) and u.startswith("http")})

    results = []
    for i, url in enumerate(unique_urls, 1):
        results.append(scrape_fighter_details(url))
        if i % 25 == 0:
            print(f"[info] scraped {i}/{len(unique_urls)}")
        time.sleep(0.3)  # be polite

    df = pd.DataFrame(results)
    # Ensure uniqueness by fighter_url (keep first)
    df = df.drop_duplicates(subset=["fighter_url"])
    df = df[ [
        "fighter_name","fighter_url","height","weight","reach","stance","dob",
        "record",
        "slpm","str_acc","sapm","str_def","td_avg","td_acc","td_def","sub_avg"
    ] ]
    df.to_csv(OUT_CSV, index=False)
    print(f"Saved {len(df)} fighters -> {OUT_CSV}")