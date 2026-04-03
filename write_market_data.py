"""
write_market_data.py
────────────────────
Pulls USDA MARS corn/hay/WDG prices + CME corn futures via Alpha Vantage
and writes docs/market_data.json for the BRI Operations Dashboard.
"""

import os, json, requests, argparse, time
from datetime import datetime, date, timedelta
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("--api-key",  default=os.environ.get("MARS_API_KEY", ""))
parser.add_argument("--av-key",   default=os.environ.get("ALPHAVANTAGE_API_KEY", ""))
parser.add_argument("--output",   default="docs/market_data.json")
args = parser.parse_args()

MARS_KEY = args.api_key
AV_KEY   = args.av_key
OUT_FILE = args.output
MARS_URL = "https://marsapi.ams.usda.gov/services/v1.2/reports"
AV_URL   = "https://www.alphavantage.co/query"

mars = requests.Session()
mars.auth = (MARS_KEY, "")
mars.headers.update({"Accept": "application/json"})


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def normalize_date(d):
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
    results = data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    rows = []
    for item in results:
        if isinstance(item, dict) and "results" in item and isinstance(item["results"], list):
            rows.extend(item["results"])
        elif isinstance(item, dict):
            rows.append(item)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# CORN CASH — report 3225: Nebraska Daily Elevator Grain Bids
# ─────────────────────────────────────────────────────────────────────────────
def fetch_corn():
    print("  Fetching report 3225 (corn cash)...")
    resp = mars.get(f"{MARS_URL}/3225", params={"allSections": "true", "lastDays": 365}, timeout=60)
    resp.raise_for_status()
    rows = unpack_rows(resp.json())

    corn_rows = [r for r in rows if "corn" in str(r.get("commodity", r.get("Commodity", ""))).lower()]

    by_date = defaultdict(list)
    for row in corn_rows:
        price = row.get("avg_price") or row.get("Avg_Price") or row.get("price")
        date_ = row.get("report_date") or row.get("Report_Date") or row.get("report_begin_date")
        if not price or not date_:
            continue
        try:
            by_date[normalize_date(date_)].append(float(price))
        except (ValueError, TypeError):
            pass

    result = [{"date": d, "price": round(sum(v)/len(v), 4)} for d, v in sorted(by_date.items())]
    print(f"  Corn cash data points: {len(result)}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# HAY — report 2935: Nebraska Direct Hay Report
# ─────────────────────────────────────────────────────────────────────────────
def fetch_hay():
    print("  Fetching report 2935 (hay)...")
    resp = mars.get(f"{MARS_URL}/2935", params={"allSections": "true", "lastDays": 365}, timeout=60)
    resp.raise_for_status()
    rows = unpack_rows(resp.json())

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

        date_ = normalize_date(date_raw)

        if "alfalfa" in commodity:
            alfa_by_date[date_].append(price)
        elif any(g in commodity for g in ["grass", "brome", "prairie", "meadow", "native"]):
            grass_by_date[date_].append(price)

    alfa_result  = [{"date": d, "price": round(sum(v)/len(v), 2)} for d, v in sorted(alfa_by_date.items())]
    grass_result = [{"date": d, "price": round(sum(v)/len(v), 2)} for d, v in sorted(grass_by_date.items())]
    print(f"  Alfalfa: {len(alfa_result)}  Grass: {len(grass_result)}")
    return alfa_result, grass_result


# ─────────────────────────────────────────────────────────────────────────────
# WDG — report 3618: National Weekly Grain Co-Products Report
# ─────────────────────────────────────────────────────────────────────────────
def fetch_wdg():
    print("  Fetching report 3618 (WDG)...")
    resp = mars.get(f"{MARS_URL}/3618", params={"allSections": "true", "lastDays": 365}, timeout=60)
    resp.raise_for_status()
    raw = resp.json()

    sections = raw if isinstance(raw, list) else raw.get("results", [])
    detail_rows = []
    for section in sections:
        if isinstance(section, dict) and "detail" in str(section.get("reportSection", "")).lower():
            detail_rows = section.get("results", [])
            break

    if not detail_rows:
        print("  WARNING: WDG detail section not found")
        return []

    by_date = defaultdict(list)
    for row in detail_rows:
        commodity = str(row.get("commodity", "")).lower()
        variety   = str(row.get("variety", "")).lower()
        location  = str(row.get("trade_loc", "")).lower()
        price_str = row.get("price")
        date_raw  = row.get("report_begin_date") or row.get("report_date")

        if "nebraska" not in location:       continue
        if "distillers grain" not in commodity: continue
        if "wet" not in variety:             continue
        if not price_str or not date_raw:    continue
        try:
            price = float(price_str)
            if price < 5: continue
        except (ValueError, TypeError):
            continue

        by_date[normalize_date(date_raw)].append(price)

    result = [{"date": d, "price": round(sum(v)/len(v), 2)} for d, v in sorted(by_date.items())]
    print(f"  WDG data points: {len(result)}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# CME CORN FUTURES — Alpha Vantage TIME_SERIES_DAILY
# Active corn contracts: March(H), May(K), July(N), September(U), December(Z)
# ─────────────────────────────────────────────────────────────────────────────
CORN_MONTH_CODES = [
    (3,  'H', 'Mar'),
    (5,  'K', 'May'),
    (7,  'N', 'Jul'),
    (9,  'U', 'Sep'),
    (12, 'Z', 'Dec'),
]

def active_corn_contracts(n=6):
    """Return the next n active CME corn contract symbols with labels."""
    today = date.today()
    contracts = []
    for yr in [today.year, today.year + 1, today.year + 2]:
        for mo, code, name in CORN_MONTH_CODES:
            # Corn expires ~14th of delivery month; consider expired if past that
            exp = date(yr, mo, 14)
            if exp >= today:
                symbol = f"ZC{code}{str(yr)[2:]}"   # e.g. ZCK26
                label  = f"{name} '{str(yr)[2:]}"    # e.g. May '26
                contracts.append({"symbol": symbol, "label": label, "month": mo, "year": yr})
            if len(contracts) >= n:
                return contracts
    return contracts


def fetch_corn_futures():
    if not AV_KEY:
        print("  No Alpha Vantage key — skipping futures")
        return []

    contracts = active_corn_contracts(6)
    print(f"  Fetching {len(contracts)} CME corn futures contracts...")

    futures = []
    cutoff = (date.today() - timedelta(days=30)).isoformat()

    for i, contract in enumerate(contracts):
        sym = contract["symbol"]
        try:
            resp = requests.get(AV_URL, params={
                "function":   "TIME_SERIES_DAILY",
                "symbol":     sym,
                "outputsize": "compact",   # last 100 trading days
                "apikey":     AV_KEY,
            }, timeout=20)
            resp.raise_for_status()
            data = resp.json()

            ts = data.get("Time Series (Daily)", {})
            if not ts:
                # Alpha Vantage returns Information key on rate limit
                info = data.get("Information") or data.get("Note") or ""
                print(f"    {sym}: no data — {info[:80]}")
                continue

            # Latest close only for the futures curve point
            latest_date = max(ts.keys())
            latest_close = float(ts[latest_date]["4. close"])

            # Last 30 days of history
            history = [
                {"date": d, "price": round(float(ts[d]["4. close"]) / 100, 4)}
                for d in sorted(ts.keys())
                if d >= cutoff
            ]

            futures.append({
                "symbol":      sym,
                "label":       contract["label"],
                "month":       contract["month"],
                "year":        contract["year"],
                "price":       round(latest_close / 100, 4),  # AV returns cents, convert to $/bu
                "date":        latest_date,
                "history":     history,
            })
            print(f"    {sym} ({contract['label']}): ${latest_close/100:.4f}/bu  ({len(history)} days history)")

        except Exception as e:
            print(f"    {sym}: error — {e}")

        # Alpha Vantage free tier: 8 calls/min — wait between calls
        if i < len(contracts) - 1:
            time.sleep(8)

    print(f"  Corn futures fetched: {len(futures)} contracts")
    return futures


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("\nFetching corn cash data (USDA MARS)...")
    corn = fetch_corn()

    print("\nFetching hay data (USDA MARS)...")
    alfa, grass = fetch_hay()

    print("\nFetching WDG data (USDA MARS)...")
    wdg = fetch_wdg()

    print("\nFetching CME corn futures (Alpha Vantage)...")
    corn_futures = fetch_corn_futures()

    market = {
        "generated":    datetime.today().strftime("%Y-%m-%d %H:%M"),
        "corn":         corn,
        "hay_alfalfa":  alfa,
        "hay_grass":    grass,
        "wdg":          wdg,
        "corn_futures": corn_futures,
    }

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(market, f, indent=2)

    print(f"\nWritten {OUT_FILE}")
    print(f"  Corn cash pts:    {len(corn)}")
    print(f"  Alfalfa pts:      {len(alfa)}")
    print(f"  Grass pts:        {len(grass)}")
    print(f"  WDG pts:          {len(wdg)}")
    print(f"  Futures contracts:{len(corn_futures)}")


if __name__ == "__main__":
    main()
