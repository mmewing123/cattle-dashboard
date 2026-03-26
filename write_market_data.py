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

import os, json, requests
from datetime import datetime, timedelta
from collections import defaultdict

API_KEY  = os.environ["MARS_API_KEY"]
BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2/reports"
HEADERS  = {"Accept": "application/json", "API_KEY": API_KEY}
OUT_FILE = "docs/market_data.json"   # adjust path if your HTML lives elsewhere

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
        "q":         "commodity=Corn;report_begin_date=" + START_DATE,
        "report_end_date": END_DATE,
        "allSections": "true",
    }
    resp = requests.get(f"{BASE_URL}/3225", headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Group bids by date, average the Panhandle elevator cash bids
    by_date = defaultdict(list)
    for row in data.get("results", []):
        loc = str(row.get("Location", "")).upper()
        if not any(a in loc for a in ALLIANCE_LOCATIONS):
            continue
        bid = row.get("Cash_Price") or row.get("Bid_Price") or row.get("Price")
        date = row.get("Report_Date", "")[:10]   # YYYY-MM-DD
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
        "q": "report_begin_date=" + START_DATE,
        "report_end_date": END_DATE,
        "allSections": "true",
    }
    resp = requests.get(f"{BASE_URL}/2935", headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    alfa_by_date  = defaultdict(list)
    grass_by_date = defaultdict(list)

    for row in data.get("results", []):
        commodity  = str(row.get("Commodity",  "")).lower()
        grade      = str(row.get("Grade",       "")).lower()
        date       = str(row.get("Report_Date", ""))[:10]
        price_str  = row.get("Wtd_Avg") or row.get("Price") or row.get("High")

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
