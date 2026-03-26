"""
write_market_data.py
────────────────────
Add this file to your cattle-dashboard/ GitHub repo.
It is called automatically by the GitHub Actions workflow (or you can run it manually).

What it does:
  - Pulls corn elevator bids for the Alliance, NE area from USDA MARS API (report 3225)
  - Pulls Nebraska Direct Hay prices from USDA MARS API (report 2935)
  - Writes market_data.json into the docs/ folder so the rm-comparison.html page
    can overlay USDA benchmarks on your Centerpoint purchase data

Requirements: same MARS_API_KEY secret already used by build_dashboard.py
"""

import os, json, requests, argparse
from datetime import datetime, timedelta
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("--api-key", default=os.environ.get("MARS_API_KEY", ""))
parser.add_argument("--output",  default="docs/market_data.json")
args = parser.parse_args()

API_KEY  = args.api_key
OUT_FILE = args.output
BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2/reports"

# Must use Basic Auth (key as username, empty password) — same as build_dashboard.py
session = requests.Session()
session.auth = (API_KEY, "")
session.headers.update({"Accept": "application/json"})

# ── How far back to pull ──────────────────────────────────────────────────────
START_DATE = (datetime.today() - timedelta(days=365)).strftime("%m/%d/%Y")
END_DATE   =  datetime.today().strftime("%m/%d/%Y")

# ─────────────────────────────────────────────────────────────────────────────
# CORN  — USDA report 3225: Nebraska Daily Elevator Grain Bids
# We filter for Alliance and nearby Panhandle locations then take the daily avg
# ─────────────────────────────────────────────────────────────────────────────
ALLIANCE_LOCATIONS = {
    "ALLIANCE", "HEMINGFORD", "HAY SPRINGS", "GORDON", "CHADRON",
    "CRAWFORD", "RUSHVILLE", "HAY SPRINGS", "SCOTTSBLUFF"
}

def fetch_corn():
    params = {
        "q":               "commodity=Corn;report_begin_date=" + START_DATE,
        "report_end_date": END_DATE,
        "allSections":     "true",
    }
    resp = session.get(f"{BASE_URL}/3225", params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    # Group bids by date, average the Panhandle elevator cash bids
    # Field names from build_dashboard.py: report_date, trade_loc, avg_price
    by_date = defaultdict(list)
    results = data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    # Handle nested structure (sections with their own results arrays)
    rows = []
    for item in results:
        if isinstance(item, dict) and "results" in item and isinstance(item["results"], list):
            rows.extend(item["results"])
        else:
            rows.append(item)

    for row in rows:
        loc = str(row.get("trade_loc", row.get("Location", ""))).upper()
        if not any(a in loc for a in ALLIANCE_LOCATIONS):
            continue
        bid = row.get("avg_price") or row.get("Cash_Price") or row.get("Price")
        date = str(row.get("report_date", row.get("Report_Date", "")))
        # Normalize MM/DD/YYYY → YYYY-MM-DD
        if date and "/" in date:
            try:
                p = date.split("/")
                date = f"{p[2][:4]}-{p[0].zfill(2)}-{p[1].zfill(2)}"
            except Exception:
                pass
        date = date[:10]
        if bid and date:
            try:
                by_date[date].append(float(bid))
            except (ValueError, TypeError):
                pass

    # Return list sorted by date, price = avg bid in $/bu
    result = []
    for date in sorted(by_date):
        avg = sum(by_date[date]) / len(by_date[date])
        result.append({"date": date, "price": round(avg, 4)})
    print(f"  Corn: {len(result)} daily data points")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# HAY  — USDA report 2935: Nebraska Direct Hay Report
# Alfalfa (Supreme/Premium) and Grass Hay, $/ton
# ─────────────────────────────────────────────────────────────────────────────
def fetch_hay():
    params = {
        "q":               "report_begin_date=" + START_DATE,
        "report_end_date": END_DATE,
        "allSections":     "true",
    }
    resp = session.get(f"{BASE_URL}/2935", params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    alfa_by_date  = defaultdict(list)
    grass_by_date = defaultdict(list)

    results = data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    rows = []
    for item in results:
        if isinstance(item, dict) and "results" in item and isinstance(item["results"], list):
            rows.extend(item["results"])
        else:
            rows.append(item)

    for row in rows:
        # build_dashboard.py uses: class, quality, report_begin_date, wtd_Avg_Price
        commodity  = str(row.get("class", row.get("Commodity", ""))).lower()
        grade      = str(row.get("quality", row.get("Grade", ""))).lower()
        date       = str(row.get("report_begin_date", row.get("Report_Date", "")))[:10]
        price_str  = row.get("wtd_Avg_Price") or row.get("Wtd_Avg") or row.get("Price")

        if not date or not price_str:
            continue
        try:
            price = float(price_str)
        except (ValueError, TypeError):
            continue

        if "alfalfa" in commodity and any(g in grade for g in ["supreme","premium","good"]):
            alfa_by_date[date].append(price)
        elif any(g in commodity for g in ["grass","brome","prairie","meadow"]):
            grass_by_date[date].append(price)

    alfa_result  = [{"date": d, "price": round(sum(v)/len(v), 2)}
                    for d, v in sorted(alfa_by_date.items())]
    grass_result = [{"date": d, "price": round(sum(v)/len(v), 2)}
                    for d, v in sorted(grass_by_date.items())]

    print(f"  Alfalfa: {len(alfa_result)} data points")
    print(f"  Grass:   {len(grass_result)} data points")
    return alfa_result, grass_result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("Fetching corn data…")
    corn = fetch_corn()

    print("Fetching hay data…")
    alfa, grass = fetch_hay()

    market = {
        "generated":   datetime.today().strftime("%Y-%m-%d %H:%M"),
        "corn":        corn,
        "hay_alfalfa": alfa,
        "hay_grass":   grass,
    }

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(market, f, indent=2)

    print(f"\n✓ Written {OUT_FILE}")
    print(f"  Corn pts:    {len(corn)}")
    print(f"  Alfalfa pts: {len(alfa)}")
    print(f"  Grass pts:   {len(grass)}")


if __name__ == "__main__":
    main()
