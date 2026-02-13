import json
import os
from datetime import datetime, timedelta
import pytz
from collections import defaultdict

# =====================================================
# DATE (IST + 1)
# =====================================================
IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)
DATE_CODE = (NOW_IST + timedelta(days=1)).strftime("%Y%m%d")
LAST_UPDATED = NOW_IST.strftime("%Y-%m-%d %H:%M IST")

BASE_DIR = f"advance/data/{DATE_CODE}"
FINAL_DETAILED = os.path.join(BASE_DIR, "finaldetailed.json")
FINAL_SUMMARY  = os.path.join(BASE_DIR, "finalsummary.json")

print(f"📁 Using directory: {BASE_DIR}")
print(f"⏱ Last updated: {LAST_UPDATED}")

# =====================================================
# HELPERS
# =====================================================
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# =====================================================
# NORMALIZE ROW (🔥 KEY FIX)
# =====================================================
def normalize_row(r):
    r["movie"] = r.get("movie") or "Unknown"
    r["city"] = r.get("city") or "Unknown"
    r["state"] = r.get("state") or "Unknown"
    r["venue"] = r.get("venue") or "Unknown"
    r["address"] = r.get("address") or ""
    r["time"] = r.get("time") or ""
    r["audi"] = r.get("audi") or ""
    r["session_id"] = str(r.get("session_id") or "")
    r["chain"] = r.get("chain") or "Unknown"
    r["source"] = r.get("source") or "Unknown"
    r["date"] = r.get("date") or DATE_CODE

    r["totalSeats"] = int(r.get("totalSeats") or 0)
    r["available"] = int(r.get("available") or 0)
    r["sold"] = int(r.get("sold") or 0)
    r["gross"] = float(r.get("gross") or 0.0)

    occ = r.get("occupancy", "")
    if isinstance(occ, (int, float)):
        r["occupancy"] = f"{round(float(occ), 2)}%"
    elif isinstance(occ, str):
        if not occ.endswith("%"):
            try:
                r["occupancy"] = f"{round(float(occ), 2)}%"
            except:
                r["occupancy"] = "0%"
    else:
        r["occupancy"] = "0%"

    return r

# =====================================================
# DEDUPE
# =====================================================
def dedupe(rows):
    seen = set()
    out = []
    dupes = 0

    for r in rows:
        key = (
            r.get("venue", ""),
            r.get("time", ""),
            r.get("session_id", ""),
            r.get("audi", ""),
        )
        if key in seen:
            dupes += 1
            continue
        seen.add(key)
        out.append(r)

    return out, dupes

# =====================================================
# LOAD + COMBINE SHARDS
# =====================================================
all_rows = []

for i in range(1, 10):
    path = os.path.join(BASE_DIR, f"detailed{i}.json")
    data = load_json(path)
    if data:
        print(f"✅ detailed{i}.json → {len(data)} rows")
        all_rows.extend(data)

print(f"📊 Raw rows: {len(all_rows)}")

# =====================================================
# NORMALIZE ALL ROWS (🔥 IMPORTANT)
# =====================================================
all_rows = [normalize_row(r) for r in all_rows]

# =====================================================
# DEDUPE FINAL
# =====================================================
final_rows, dupes = dedupe(all_rows)
print(f"🧹 Duplicates removed: {dupes}")
print(f"🎯 Final detailed rows: {len(final_rows)}")

# =====================================================
# SORT FINAL DETAILED
# =====================================================
final_rows.sort(
    key=lambda x: (
        x["movie"],
        x["city"],
        x["venue"],
        x["time"],
    )
)

# =====================================================
# SAVE finaldetailed.json
# =====================================================
# compress detailed rows
def compress_detail_row(r):
    r.pop("address", None)
    r.pop("date", None)

    src = r.pop("source", None)
    if src == "BMS":
        r["s"] = "B"
    elif src == "District":
        r["s"] = "D"
    elif src:
        r["s"] = src[0].upper()
    return r

compressed_rows = [compress_detail_row(dict(r)) for r in final_rows]

os.makedirs(os.path.dirname(FINAL_DETAILED), exist_ok=True)
with open(FINAL_DETAILED, "w", encoding="utf-8") as f:
    json.dump(
        {"last_updated": LAST_UPDATED, "data": compressed_rows},
        f,
        ensure_ascii=False,
        separators=(',', ':')
    )


print("🎉 finaldetailed.json saved")

# =====================================================
# BUILD FINAL SUMMARY
# =====================================================
summary = {}

for r in final_rows:
    movie = r["movie"]
    city = r["city"]
    state = r["state"]
    venue = r["venue"]
    chain = r["chain"]

    total = r["totalSeats"]
    sold = r["sold"]
    gross = r["gross"]
    occ = (sold / total * 100) if total else 0

    if movie not in summary:
        summary[movie] = {
            "shows": 0,
            "gross": 0.0,
            "sold": 0,
            "totalSeats": 0,
            "venues": set(),
            "cities": set(),
            "fastfilling": 0,
            "housefull": 0,
            "details": {},
            "Chain_details": {}
        }

    m = summary[movie]
    m["shows"] += 1
    m["gross"] += gross
    m["sold"] += sold
    m["totalSeats"] += total
    m["venues"].add(venue)
    m["cities"].add(city)

    if occ >= 98:
        m["housefull"] += 1
    elif occ >= 50:
        m["fastfilling"] += 1

    ck = (city, state)
    if ck not in m["details"]:
        m["details"][ck] = {
            "city": city,
            "state": state,
            "venues": set(),
            "shows": 0,
            "gross": 0.0,
            "sold": 0,
            "totalSeats": 0,
            "fastfilling": 0,
            "housefull": 0
        }

    d = m["details"][ck]
    d["venues"].add(venue)
    d["shows"] += 1
    d["gross"] += gross
    d["sold"] += sold
    d["totalSeats"] += total
    if occ >= 98:
        d["housefull"] += 1
    elif occ >= 50:
        d["fastfilling"] += 1

    if chain not in m["Chain_details"]:
        m["Chain_details"][chain] = {
            "chain": chain,
            "venues": set(),
            "shows": 0,
            "gross": 0.0,
            "sold": 0,
            "totalSeats": 0,
            "fastfilling": 0,
            "housefull": 0
        }

    c = m["Chain_details"][chain]
    c["venues"].add(venue)
    c["shows"] += 1
    c["gross"] += gross
    c["sold"] += sold
    c["totalSeats"] += total
    if occ >= 98:
        c["housefull"] += 1
    elif occ >= 50:
        c["fastfilling"] += 1

# =====================================================
# FINALIZE SUMMARY
# =====================================================
final_summary = {}

for movie, m in summary.items():
    final_summary[movie] = {
        "shows": m["shows"],
        "gross": round(m["gross"], 2),
        "sold": m["sold"],
        "totalSeats": m["totalSeats"],
        "venues": len(m["venues"]),
        "cities": len(m["cities"]),
        "fastfilling": m["fastfilling"],
        "housefull": m["housefull"],
        "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
        "details": [],
        "Chain_details": []
    }

    for d in m["details"].values():
        final_summary[movie]["details"].append({
            "city": d["city"],
            "state": d["state"],
            "venues": len(d["venues"]),
            "shows": d["shows"],
            "gross": round(d["gross"], 2),
            "sold": d["sold"],
            "totalSeats": d["totalSeats"],
            "fastfilling": d["fastfilling"],
            "housefull": d["housefull"],
            "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0
        })

    for c in m["Chain_details"].values():
        final_summary[movie]["Chain_details"].append({
            "chain": c["chain"],
            "venues": len(c["venues"]),
            "shows": c["shows"],
            "gross": round(c["gross"], 2),
            "sold": c["sold"],
            "totalSeats": c["totalSeats"],
            "fastfilling": c["fastfilling"],
            "housefull": c["housefull"],
            "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0
        })

# =====================================================
# SAVE finalsummary.json
# =====================================================
os.makedirs(os.path.dirname(FINAL_SUMMARY), exist_ok=True)
with open(FINAL_SUMMARY, "w", encoding="utf-8") as f:
    json.dump(
        {"last_updated": LAST_UPDATED, "movies": final_summary},
        f,
        ensure_ascii=False,
        separators=(',', ':')
    )


print("🎉 finalsummary.json created successfully")
print("📄 Files ready:")
print(f"   • {FINAL_DETAILED}")
print(f"   • {FINAL_SUMMARY}")

# =====================================================
# ADVANCE MONTHLY LOGGING (T+1 FUTURE DAY)
# =====================================================
ADV_LOG_ROOT = "advance/logs"
os.makedirs(ADV_LOG_ROOT, exist_ok=True)

# month folder → e.g. advance/logs/2026-01
MONTH_FOLDER = os.path.join(ADV_LOG_ROOT, NOW_IST.strftime("%Y-%m"))
os.makedirs(MONTH_FOLDER, exist_ok=True)

# file → advance/logs/2026-01/monthlylogs.json
ADV_LOG_FILE = os.path.join(MONTH_FOLDER, "monthlylogs.json")

# load existing logs if present
if os.path.exists(ADV_LOG_FILE):
    try:
        adv_logs = json.load(open(ADV_LOG_FILE, "r", encoding="utf-8"))
    except:
        adv_logs = {}
else:
    adv_logs = {}

# TARGET DATE = DATE_CODE converted to dd/mm/yyyy
TARGET_DATE = datetime.strptime(DATE_CODE, "%Y%m%d").strftime("%d/%m/%Y")

# BUCKET = "TIME, TODAY" (e.g. "05PM, 12/01")
TIME_KEY = NOW_IST.strftime("%I%p")
TODAY_KEY = NOW_IST.strftime("%d/%m")
BUCKET = f"{TIME_KEY}, {TODAY_KEY}"

movies_summary = final_summary

# write movie logs
for movie, d in movies_summary.items():
    if movie not in adv_logs:
        adv_logs[movie] = {}

    if TARGET_DATE not in adv_logs[movie]:
        adv_logs[movie][TARGET_DATE] = {}

    adv_logs[movie][TARGET_DATE][BUCKET] = [
        d["shows"],
        d["gross"],
        d["sold"],
        round(d["occupancy"], 2),
        d["venues"],
        d["cities"],
        d["fastfilling"],
        d["housefull"]
    ]

# save compact json
with open(ADV_LOG_FILE, "w", encoding="utf-8") as f:
    json.dump(adv_logs, f, ensure_ascii=False, separators=(',', ':'))

print(f"📦 Advance monthly log updated → {ADV_LOG_FILE}")
print(f"🎯 Target Date: {TARGET_DATE} | Bucket: {BUCKET}")