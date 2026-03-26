#!/usr/bin/env python3
"""
build_dashboard.py
==================
Pulls NE corn bids + hay prices from USDA MARS API,
scrapes the Panhandle plain-text report, and generates
a self-contained HTML dashboard.

Used by GitHub Actions on a daily cron, or run manually:
  python build_dashboard.py --api-key YOUR_KEY
"""

import argparse, json, os, re, sys
from datetime import datetime

try:
    import requests
except ImportError:
    sys.exit("pip install requests")

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2/reports"
SLUG_GRAIN = "3225"
SLUG_HAY   = "2935"
TXT_URL    = "https://www.ams.usda.gov/mnreports/to_gr110.txt"
DAYS       = 120  # pull window


# ─── MARS API ────────────────────────────────────────────────────────────────
class MarsAPI:
    def __init__(self, key):
        self.s = requests.Session()
        self.s.auth = (key, "")
        self.s.headers.update({"Accept": "application/json"})

    def get(self, slug, last_days=None):
        url = f"{BASE_URL}/{slug}"
        p = {"allSections": "true"}
        if last_days:
            p["lastDays"] = last_days
        print(f"  → {url}  (lastDays={last_days})")
        r = self.s.get(url, params=p, timeout=120)
        r.raise_for_status()
        data = r.json()
        sections = data["results"] if isinstance(data, dict) and "results" in data else (data if isinstance(data, list) else [data])
        rows = []
        for item in sections:
            if isinstance(item, dict) and "results" in item and isinstance(item["results"], list):
                sec = item.get("reportSection", "")
                for row in item["results"]:
                    row["_sec"] = sec
                rows.extend(item["results"])
        if rows:
            print(f"    ✓ {len(rows)} rows")
        else:
            rows = sections
            print(f"    ✓ {len(rows)} records (flat)")
        return rows


# ─── PULL CORN ───────────────────────────────────────────────────────────────
def pull_corn(api):
    print("\n📊 Pulling corn bids...")
    rows = api.get(SLUG_GRAIN, DAYS)
    detail = [r for r in rows if r.get("_sec", "").lower() != "report header"]
    corn = [r for r in (detail or rows) if r.get("commodity") == "Corn"]
    print(f"    Corn rows: {len(corn)}")
    out = []
    for r in corn:
        try:
            out.append({
                "d": r.get("report_date", ""),
                "loc": r.get("trade_loc", ""),
                "bMid": round((int(r.get("basis Min", 0)) + int(r.get("basis Max", 0))) / 2, 1),
                "price": round(float(r.get("avg_price", 0)), 4),
                "ya": round(float(r["avg_price_year_ago"]), 4) if r.get("avg_price_year_ago") else None,
            })
        except (ValueError, TypeError):
            continue
    # Convert MM/DD/YYYY to YYYY-MM-DD for sorting
    for row in out:
        try:
            parts = row["d"].split("/")
            row["d"] = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
        except:
            pass
    out.sort(key=lambda r: (r["d"], r["loc"]))
    # Thin out to ~weekly samples to keep the HTML small (keep every Mon + most recent 2 weeks)
    if len(out) > 200:
        dates = sorted(set(r["d"] for r in out))
        cutoff = dates[-10] if len(dates) > 10 else dates[0]
        keep_dates = set()
        for d in dates:
            if d >= cutoff:
                keep_dates.add(d)
            else:
                # Keep ~2 per week
                from datetime import datetime as dt
                try:
                    wd = dt.strptime(d, "%Y-%m-%d").weekday()
                    if wd in (0, 3):  # Mon, Thu
                        keep_dates.add(d)
                except:
                    keep_dates.add(d)
        out = [r for r in out if r["d"] in keep_dates]
    print(f"    Final: {len(out)} corn data points")
    return out


# ─── PULL HAY ────────────────────────────────────────────────────────────────
def pull_hay(api):
    print("\n🌾 Pulling hay prices...")
    rows = api.get(SLUG_HAY, DAYS)
    detail = [r for r in rows if r.get("_sec", "").lower() not in ("report header", "volume", "volumes", "")]
    trades = [r for r in (detail or rows) if r.get("sale_Type") == "Trade"]
    print(f"    Trade rows: {len(trades)}")
    out = []
    for r in trades:
        try:
            cls = r.get("class", "") or ""
            if "Alfalfa" in cls and "Grass" in cls:
                cls = "Alf/Grass Mix"
            elif "Alfalfa" in cls:
                cls = "Alfalfa"
            elif "Prairie" in cls or "Meadow" in cls:
                cls = "P/M Grass"
            elif "Grass" in cls:
                cls = "Grass"

            wtd = float(r.get("wtd_Avg_Price", 0))
            if wtd <= 10:
                continue

            # Convert date
            d = r.get("report_begin_date", "")
            try:
                parts = d.split("/")
                d_fmt = f"{parts[0].zfill(2)}/{parts[1].zfill(2)}"
            except:
                d_fmt = d

            out.append({
                "d": d_fmt,
                "cls": cls,
                "q": r.get("quality", "") or "",
                "r": (r.get("region", "") or "")[:8],
                "w": round(wtd, 2),
                "t": int(float(r.get("quantity", 0))) if r.get("quantity") else 0,
            })
        except (ValueError, TypeError):
            continue
    print(f"    Final: {len(out)} hay data points")
    return out


# ─── SCRAPE PANHANDLE TXT ───────────────────────────────────────────────────
def scrape_txt():
    print("\n📄 Scraping Panhandle text report...")
    try:
        resp = requests.get(TXT_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"    ⚠ Failed: {e}")
        return []
    rows = []
    today = datetime.now().strftime("%Y-%m-%d")
    for line in resp.text.split("\n"):
        line = line.strip()
        if not line:
            continue
        m = re.match(r'^([A-Za-z][\w\s\.]+?)\s{2,}([\d\.\-]+(?:\s|$))', line)
        if m:
            loc = m.group(1).strip()
            prices = re.split(r'\s{2,}', line[m.start(2):].strip())
            cols = ["wheat", "corn", "beans", "millet", "sun"]
            row = {"loc": loc}
            for i, c in enumerate(cols):
                row[c] = prices[i].strip() if i < len(prices) else "--"
            if row.get("corn") and row["corn"] != "--":
                rows.append(row)
    print(f"    ✓ {len(rows)} locations with corn prices")
    return rows


# ─── BUILD HTML ──────────────────────────────────────────────────────────────
def build_html(corn, hay, txt, output_path):
    print(f"\n🔨 Building dashboard → {output_path}")
    now = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    latest_date = corn[-1]["d"] if corn else "N/A"

    # Build panhandle ticker
    ticker_html = ""
    if txt:
        items = " · ".join(f"{r['loc']} <b>${r['corn']}</b>" for r in txt[:15])
        ticker_html = f'<div class="ticker"><span>Today\'s Panhandle Corn ($/bu):</span> {items}</div>'

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NE Market Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0f172a;color:#e2e8f0;font-family:'IBM Plex Sans',-apple-system,sans-serif}}
.wrap{{max-width:1140px;margin:0 auto;padding:24px 20px}}
h1{{font-size:28px;font-weight:700;letter-spacing:-.5px}}h1 em{{color:#fbbf24;font-style:normal}}
.meta{{font-size:13px;color:#64748b;font-family:'IBM Plex Mono',monospace;margin-left:12px}}
.sub{{font-size:13px;color:#94a3b8;margin-top:6px;max-width:620px}}
.updated{{font-size:11px;color:#475569;margin-top:4px}}
.ticker{{background:rgba(251,191,36,.08);border:1px solid rgba(251,191,36,.2);border-radius:10px;padding:10px 16px;margin:16px 0;font-size:12px;color:#fbbf24;overflow-x:auto;white-space:nowrap}}
.ticker span{{color:#94a3b8;margin-right:8px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px;margin:20px 0}}
.kpi{{background:rgba(30,41,59,.7);border-radius:12px;padding:16px 18px;border:1px solid rgba(100,116,139,.2)}}
.kpi .l{{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:1px;font-weight:600}}
.kpi .v{{font-size:28px;font-weight:700;font-family:'IBM Plex Mono',monospace;margin-top:4px}}
.kpi .s{{font-size:11px;color:#475569;margin-top:2px}}
.tabs{{display:flex;gap:6px;background:rgba(30,41,59,.5);border-radius:10px;padding:4px;width:fit-content;margin-bottom:16px}}
.tab{{padding:8px 20px;border-radius:8px;border:none;cursor:pointer;font-size:13px;font-weight:600;font-family:inherit;background:transparent;color:#94a3b8;transition:all .2s}}
.tab.on{{background:#fbbf24;color:#0f172a}}
.locs{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:16px}}
.lbtn{{padding:5px 14px;border-radius:20px;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;transition:all .15s}}
.card{{background:rgba(30,41,59,.5);border-radius:16px;padding:20px;border:1px solid rgba(100,116,139,.15);margin-bottom:16px}}
.card h2{{font-size:16px;font-weight:600;margin-bottom:4px}}.card h2 span{{color:#64748b;font-weight:400;font-size:13px}}
.card .note{{font-size:11px;color:#475569;margin-bottom:12px}}
.cw{{position:relative;height:380px}}
.sec{{display:none}}.sec.on{{display:block}}
table{{width:100%;border-collapse:collapse;font-size:12px;font-family:'IBM Plex Mono',monospace}}
th{{padding:8px 10px;text-align:left;color:#64748b;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid rgba(100,116,139,.3)}}
td{{padding:6px 10px;border-bottom:1px solid rgba(100,116,139,.1)}}
tr:nth-child(even){{background:rgba(100,116,139,.05)}}
.alf{{color:#22c55e;font-weight:600}}.grs{{color:#f59e0b;font-weight:600}}.wt{{color:#fbbf24;font-weight:600}}.dm{{color:#64748b}}
.foot{{text-align:center;font-size:11px;color:#475569;margin-top:24px}}
@media(max-width:640px){{h1{{font-size:22px}}.kpi .v{{font-size:22px}}.cw{{height:300px}}}}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;align-items:baseline;flex-wrap:wrap;gap:8px"><h1><em>NE</em> Market Dashboard</h1><span class="meta">USDA MARS API</span></div>
  <p class="sub">Nebraska corn basis & elevator bids by region · Alfalfa & grass hay trade prices</p>
  <p class="updated">Last updated: {now} · Data through {latest_date}</p>
  {ticker_html}
  <div class="kpi-grid" id="kpis"></div>
  <div class="tabs" id="tabs">
    <button class="tab on" data-t="basis">Corn Basis</button>
    <button class="tab" data-t="price">Corn Price</button>
    <button class="tab" data-t="hay">Hay Prices</button>
  </div>
  <div class="locs" id="locs"></div>
  <div class="sec on" id="s-basis"><div class="card"><h2>Corn Basis (Midpoint) by NE Region <span>¢/bu vs CBOT</span></h2><p class="note">Negative = local price below futures. Closer to zero = stronger local demand.</p><div class="cw"><canvas id="c1"></canvas></div></div></div>
  <div class="sec" id="s-price"><div class="card"><h2>Corn Avg Cash Price — Current vs Year Ago <span>$/bu</span></h2><div class="cw"><canvas id="c2"></canvas></div></div><div class="card"><h2>Corn Cash Price by Region <span>$/bu</span></h2><div class="cw"><canvas id="c3"></canvas></div></div></div>
  <div class="sec" id="s-hay"><div class="card"><h2>Alfalfa vs Grass Hay — Weighted Avg <span>$/ton, trades</span></h2><p class="note">Points = weighted avg across all regions & qualities per report period.</p><div class="cw"><canvas id="c4"></canvas></div></div><div class="card"><h2>Hay Trade Detail — Latest Report <span>$/ton</span></h2><div class="cw" style="height:auto"><canvas id="c5"></canvas></div></div><div class="card"><h2>Recent Hay Trades</h2><div style="overflow-x:auto"><table id="htbl"></table></div></div></div>
  <p class="foot">Source: USDA AMS LPGMN · Slug 3225 (Grain) · Slug 2935 (Hay) · Auto-updated daily via GitHub Actions</p>
</div>
<script>
const C={json.dumps(corn)};
const H={json.dumps(hay)};
const LOCS=["Central","Northeast","East","Southeast","South","Southwest","Northwest"];
const LC={{Central:"#3b82f6",Northeast:"#10b981",East:"#f59e0b",Southeast:"#ef4444",South:"#8b5cf6",Southwest:"#ec4899",Northwest:"#06b6d4"}};
const dates=[...new Set(C.map(r=>r.d))].sort();
const fD=d=>{{const p=d.split("-");return p[1]+"/"+p[2]}};
const labels=dates.map(fD);
const byDL=(loc,key)=>dates.map(d=>{{const r=C.find(x=>x.d===d&&x.loc===loc);return r?r[key]:null}});
const avgBy=key=>dates.map(d=>{{const rs=C.filter(x=>x.d===d&&x[key]!=null);return rs.length?+(rs.reduce((a,r)=>a+r[key],0)/rs.length).toFixed(4):null}});

Chart.defaults.color='#94a3b8';Chart.defaults.font.family="'IBM Plex Sans'";Chart.defaults.font.size=11;
Chart.defaults.plugins.legend.labels.usePointStyle=true;Chart.defaults.elements.point.radius=0;Chart.defaults.elements.line.tension=.3;

const lA=avgBy('price'),lY=avgBy('ya'),last=lA[lA.length-1]||0,lastY=lY.filter(v=>v!=null).pop()||0;
const yoy=lastY?((last-lastY)/lastY*100).toFixed(1):0;
const latest=C.filter(r=>r.d===dates[dates.length-1]);
document.getElementById('kpis').innerHTML=[
  {{l:"Avg Corn Price",v:"$"+(last||0).toFixed(2),s:"All NE regions",c:"#fbbf24"}},
  {{l:"YoY Change",v:(yoy>0?"+":"")+yoy+"%",s:"vs year ago",c:yoy<0?"#ef4444":"#22c55e"}},
  {{l:"Tightest Basis",v:Math.max(...latest.map(r=>r.bMid))+"¢",s:"Strongest region",c:"#3b82f6"}},
  {{l:"Widest Basis",v:Math.min(...latest.map(r=>r.bMid))+"¢",s:"Weakest region",c:"#a855f7"}}
].map(k=>`<div class="kpi"><div class="l">${{k.l}}</div><div class="v" style="color:${{k.c}}">${{k.v}}</div><div class="s">${{k.s}}</div></div>`).join('');

let aL=new Set(LOCS);const ld=document.getElementById('locs');
function rT(){{ld.innerHTML=LOCS.map(l=>{{const on=aL.has(l);return`<button class="lbtn" data-l="${{l}}" style="border:2px solid ${{LC[l]}};background:${{on?LC[l]:'transparent'}};color:${{on?'#fff':LC[l]}}">${{l}}</button>`}}).join('')}}
rT();
ld.onclick=e=>{{const l=e.target.dataset.l;if(!l)return;aL.has(l)?aL.delete(l):aL.add(l);rT();[ch1,ch3].forEach(c=>{{c.data.datasets.forEach(ds=>{{ds.hidden=!aL.has(ds.label)}});c.update()}})}};

const ch1=new Chart(document.getElementById('c1'),{{type:'line',data:{{labels,datasets:LOCS.map(l=>({{label:l,data:byDL(l,'bMid'),borderColor:LC[l],borderWidth:2,spanGaps:true}}))}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{tooltip:{{backgroundColor:'rgba(15,23,42,.95)'}}}},scales:{{y:{{title:{{display:true,text:'Basis ¢/bu'}},grid:{{color:'rgba(100,116,139,.12)'}}}},x:{{grid:{{color:'rgba(100,116,139,.08)'}}}}}}}}}});

new Chart(document.getElementById('c2'),{{type:'line',data:{{labels,datasets:[{{label:'Current',data:lA,borderColor:'#fbbf24',borderWidth:2.5,backgroundColor:'rgba(251,191,36,.1)',fill:true}},{{label:'Year Ago',data:lY,borderColor:'#64748b',borderWidth:1.5,borderDash:[5,4],backgroundColor:'rgba(100,116,139,.05)',fill:true,spanGaps:true}}]}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{tooltip:{{backgroundColor:'rgba(15,23,42,.95)',callbacks:{{label:c=>'$'+c.parsed.y.toFixed(4)+' '+c.dataset.label}}}}}},scales:{{y:{{ticks:{{callback:v=>'$'+v.toFixed(2)}},grid:{{color:'rgba(100,116,139,.12)'}}}},x:{{grid:{{color:'rgba(100,116,139,.08)'}}}}}}}}}});

const ch3=new Chart(document.getElementById('c3'),{{type:'line',data:{{labels,datasets:LOCS.map(l=>({{label:l,data:byDL(l,'price'),borderColor:LC[l],borderWidth:2,spanGaps:true}}))}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{tooltip:{{backgroundColor:'rgba(15,23,42,.95)',callbacks:{{label:c=>'$'+c.parsed.y.toFixed(4)+' '+c.dataset.label}}}}}},scales:{{y:{{ticks:{{callback:v=>'$'+v.toFixed(2)}},grid:{{color:'rgba(100,116,139,.12)'}}}},x:{{grid:{{color:'rgba(100,116,139,.08)'}}}}}}}}}});

const hDates=[...new Set(H.map(r=>r.d))];
function hAvg(t){{return hDates.map(d=>{{const rs=H.filter(r=>r.d===d&&(t==='a'?r.cls==='Alfalfa':r.cls.includes('Grass')));return rs.length?+(rs.reduce((a,r)=>a+r.w,0)/rs.length).toFixed(1):null}})}}
new Chart(document.getElementById('c4'),{{type:'line',data:{{labels:hDates,datasets:[{{label:'Alfalfa Wtd Avg',data:hAvg('a'),borderColor:'#22c55e',borderWidth:2.5,backgroundColor:'rgba(34,197,94,.12)',fill:true,pointRadius:5,pointBackgroundColor:'#22c55e'}},{{label:'Grass Wtd Avg',data:hAvg('g'),borderColor:'#f59e0b',borderWidth:2.5,backgroundColor:'rgba(245,158,11,.12)',fill:true,pointRadius:5,pointBackgroundColor:'#f59e0b'}}]}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{tooltip:{{backgroundColor:'rgba(15,23,42,.95)',callbacks:{{label:c=>'$'+c.parsed.y+'/ton'}}}}}},scales:{{y:{{beginAtZero:true,ticks:{{callback:v=>'$'+v}},grid:{{color:'rgba(100,116,139,.12)'}}}},x:{{grid:{{color:'rgba(100,116,139,.08)'}}}}}}}}}});

const lastHayDate=hDates[hDates.length-1]||'';
const lH=H.filter(r=>r.d===lastHayDate);
new Chart(document.getElementById('c5'),{{type:'bar',data:{{labels:lH.map(r=>r.cls+' '+r.q+' ('+r.r+')'),datasets:[{{label:'$/ton',data:lH.map(r=>r.w),backgroundColor:lH.map(r=>r.cls==='Alfalfa'?'rgba(34,197,94,.7)':'rgba(245,158,11,.7)'),borderRadius:4,barThickness:20}}]}},options:{{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{{tooltip:{{backgroundColor:'rgba(15,23,42,.95)',callbacks:{{label:c=>'$'+c.parsed.x+'/ton'}}}},legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:v=>'$'+v}},grid:{{color:'rgba(100,116,139,.12)'}}}},y:{{ticks:{{font:{{size:10}}}},grid:{{display:false}}}}}}}}}});

document.getElementById('htbl').innerHTML='<thead><tr><th>Week</th><th>Class</th><th>Quality</th><th>Region</th><th>Wtd Avg</th><th>Tons</th></tr></thead><tbody>'+H.map(r=>`<tr><td class="dm">${{r.d}}</td><td class="${{r.cls==='Alfalfa'?'alf':'grs'}}">${{r.cls}}</td><td>${{r.q}}</td><td>${{r.r}}</td><td class="wt">${{r.w}}</td><td class="dm">${{r.t?r.t.toLocaleString():'—'}}</td></tr>`).join('')+'</tbody>';

document.getElementById('tabs').onclick=e=>{{const t=e.target.dataset.t;if(!t)return;document.querySelectorAll('.tab').forEach(b=>b.classList.toggle('on',b.dataset.t===t));document.querySelectorAll('.sec').forEach(s=>s.classList.toggle('on',s.id==='s-'+t));ld.style.display=t==='hay'?'none':'flex'}};
</script>
</body>
</html>'''

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    size_kb = os.path.getsize(output_path) / 1024
    print(f"    ✓ {output_path} ({size_kb:.0f} KB)")


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Build NE Market Dashboard")
    parser.add_argument("--api-key", required=True, help="MARS API key")
    parser.add_argument("--output", default="docs/index.html", help="Output HTML path")
    parser.add_argument("--days", type=int, default=DAYS, help="Days of history")
    args = parser.parse_args()

    api = MarsAPI(args.api_key)
    corn = pull_corn(api)
    hay = pull_hay(api)
    txt = scrape_txt()
    build_html(corn, hay, txt, args.output)
    print("\n✅ Done!")

if __name__ == "__main__":
    main()
