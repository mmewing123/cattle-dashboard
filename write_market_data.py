"""
write_market_data.py
────────────────────
Pulls USDA MARS corn, hay, and WDG prices and writes docs/market_data.json
for the rm-comparison.html dashboard.
"""

import os, json, requests, argparse
from datetime import datetime
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("--api-key", default=os.environ.get("MARS_API_KEY", ""))
parser.add_argument("--output",  default="docs/market_data.json")
args = parser.parse_args()

API_KEY  = args.api_key
OUT_FILE = args.output
BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2/reports"

session = requests.Session()
session.auth = (API_KEY, "")
session.headers.update({"Accept": "application/json"})


def normalize_date(d):
    """Convert MM/DD/YYYY to YYYY-MM-DD. Pass through YYYY-MM-DD unchanged."""
    d = str(d).strip()[:10]
    if "/" in d:
        try:
            p = d.split("/")
            year = p[2] if len(p[2]) == 4 else "20" + p[2]
            return f"{year}-{p[0].zfill(2)}-{p[1].zfill(2)}"
        except Exception:
            return d
    return d


def unpack_rows(data):
    """Flatten MARS nested section structure into a flat list of rows."""
    results = data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    rows = []
    for item in results:
        if isinstance(item, dict) and "results" in item and isinstance(item["results"], list):
            rows.extend(item["results"])
        elif isinstance(item, dict):
            rows.append(item)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# CORN — report 3225: Nebraska Daily Elevator Grain Bids
# ─────────────────────────────────────────────────────────────────────────────
def fetch_corn():
    print("  Fetching report 3225 (corn)...")
    resp = session.get(f"{BASE_URL}/3225", params={"allSections": "true", "lastDays": 365}, timeout=60)
    resp.raise_for_status()
    rows = unpack_rows(resp.json())
    print(f"  Total rows: {len(rows)}")

    corn_rows = [r for r in rows if "corn" in str(r.get("commodity", r.get("Commodity", ""))).lower()]
    print(f"  Corn rows: {len(corn_rows)}")

    by_date = defaultdict(list)
    for row in corn_rows:
        price = row.get("avg_price") or row.get("Avg_Price") or row.get("price")
        date  = row.get("report_date") or row.get("Report_Date") or row.get("report_begin_date")
        if not price or not date:
            continue
        try:
            by_date[normalize_date(date)].append(float(price))
        except (ValueError, TypeError):
            pass

    result = [{"date": d, "price": round(sum(v)/len(v), 4)} for d, v in sorted(by_date.items())]
    print(f"  Corn data points: {len(result)}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# HAY — report 2935: Nebraska Direct Hay Report
# ─────────────────────────────────────────────────────────────────────────────
def fetch_hay():
    print("  Fetching report 2935 (hay)...")
    resp = session.get(f"{BASE_URL}/2935", params={"allSections": "true", "lastDays": 365}, timeout=60)
    resp.raise_for_status()
    rows = unpack_rows(resp.json())
    print(f"  Total rows: {len(rows)}")

    alfa_by_date  = defaultdict(list)
    grass_by_date = defaultdict(list)

    for row in rows:
        commodity = str(row.get("class", row.get("Commodity", ""))).lower()
        date_raw  = row.get("report_begin_date") or row.get("report_date") or row.get("Report_Date", "")
        price_str = row.get("wtd_Avg_Price") or row.get("Wtd_Avg") or row.get("avg_price") or row.get("Price")

        if not date_raw or not price_str:
            continue
        try:
            price = float(price_str)
            if price < 10:
                continue
        except (ValueError, TypeError):
            continue

        date = normalize_date(date_raw)

        if "alfalfa" in commodity:
            alfa_by_date[date].append(price)
        elif any(g in commodity for g in ["grass", "brome", "prairie", "meadow", "native"]):
            grass_by_date[date].append(price)

    alfa_result  = [{"date": d, "price": round(sum(v)/len(v), 2)} for d, v in sorted(alfa_by_date.items())]
    grass_result = [{"date": d, "price": round(sum(v)/len(v), 2)} for d, v in sorted(grass_by_date.items())]

    print(f"  Alfalfa data points: {len(alfa_result)}")
    print(f"  Grass data points:   {len(grass_result)}")
    return alfa_result, grass_result


# ─────────────────────────────────────────────────────────────────────────────
# WDG — report 3618: National Weekly Grain Co-Products Report
# ─────────────────────────────────────────────────────────────────────────────
def fetch_wdg():
    print("  Fetching report 3618 (WDG)...")
    resp = session.get(f"{BASE_URL}/3618", params={"allSections": "true", "lastDays": 365}, timeout=60)
    resp.raise_for_status()
    raw = resp.json()

    sections = raw if isinstance(raw, list) else raw.get("results", [])

    detail_rows = []
    for section in sections:
        if isinstance(section, dict) and "detail" in str(section.get("reportSection", "")).lower():
            detail_rows = section.get("results", [])
            print(f"  Report Detail: {len(detail_rows)} rows")
            break

    if not detail_rows:
        print("  WARNING: Report Detail section not found")
        return []

    by_date = defaultdict(list)
    for row in detail_rows:
        commodity = str(row.get("commodity", "")).lower()
        location  = str(row.get("trade_loc", "")).lower()
        price_str = row.get("price")
        date_raw  = row.get("report_begin_date") or row.get("report_date")

        # Must be Nebraska WDG (wet distillers grain 65-70%)
        if "nebraska" not in location:
            continue
        if not any(x in commodity for x in ["wet", "wdg"]):
            continue
        if "65" not in commodity and "wet" not in commodity:
            continue

        if not price_str or not date_raw:
            continue
        try:
            price = float(price_str)
            if price < 5:
                continue
        except (ValueError, TypeError):
            continue

        by_date[normalize_date(date_raw)].append(price)

    # Debug: print unique Nebraska commodities found to confirm matching
    ne_commodities = set(
        str(r.get("commodity", ""))
        for r in detail_rows
        if "nebraska" in str(r.get("trade_loc", "")).lower()
    )
    print(f"  Nebraska commodities found: {ne_commodities}")

    result = [{"date": d, "price": round(sum(v)/len(v), 2)} for d, v in sorted(by_date.items())]
    print(f"  WDG data points: {len(result)}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("\nFetching corn data...")
    corn = fetch_corn()

    print("\nFetching hay data...")
    alfa, grass = fetch_hay()

    print("\nFetching WDG data...")
    wdg = fetch_wdg()

    market = {
        "generated":   datetime.today().strftime("%Y-%m-%d %H:%M"),
        "corn":        corn,
        "hay_alfalfa": alfa,
        "hay_grass":   grass,
        "wdg":         wdg,
    }

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(market, f, indent=2)

    print(f"\nWritten {OUT_FILE}")
    print(f"  Corn pts:    {len(corn)}")
    print(f"  Alfalfa pts: {len(alfa)}")
    print(f"  Grass pts:   {len(grass)}")
    print(f"  WDG pts:     {len(wdg)}")


if __name__ == "__main__":
    main()
