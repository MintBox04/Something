import asyncio
import json
import os
import re
import math
import sys
import random
from datetime import datetime, timedelta, timezone
from playwright.async_api import async_playwright

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 1
if len(sys.argv) > 1:
    try:
        SHARD_ID = int(sys.argv[1])
    except ValueError:
        pass

API_TIMEOUT = 12
HARD_TIMEOUT = 15
CUTOFF_MINUTES = 200

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = os.path.join("daily", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

SUMMARY_FILE  = os.path.join(BASE_DIR, f"movie_summary{SHARD_ID}.json")
DETAILED_FILE = os.path.join(BASE_DIR, f"detailed{SHARD_ID}.json")
LOG_FILE      = os.path.join(LOG_DIR, f"bms{SHARD_ID}.log")

# =====================================================
# LOGGING
# =====================================================
def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# =====================================================
# HELPERS
# =====================================================
def calc_occupancy(sold, total):
    return round((sold / total) * 100, 2) if total else 0.0

def minutes_left(show_time):
    try:
        now = datetime.now(IST)
        t = datetime.strptime(show_time, "%I:%M %p")
        t = t.replace(year=now.year, month=now.month, day=now.day, tzinfo=IST)
        return 9999 
    except Exception:
        return 9999

# =====================================================
# MAIN ASYNC
# =====================================================
async def run_shard():
    log(f"🚀 STARTING SHARD {SHARD_ID}")
    
    # 1. Read Venues
    venues_file = f"venues{SHARD_ID}.json"
    if not os.path.exists(venues_file):
        log(f"❌ Venues file not found: {venues_file}")
        return

    with open(venues_file, "r", encoding="utf-8") as f:
        venues_data = json.load(f)
        
    log(f"Loaded {len(venues_data)} venues from {venues_file}")

    # 2. Read code.js and Inject Venues
    if not os.path.exists("code.js"):
        log("❌ code.js not found")
        return

    with open("code.js", "r", encoding="utf-8") as f:
        js_code = f.read()

    start_marker = "const venues = {"
    end_marker = "const venueCodes = Object.keys(venues);"
    
    start_idx = js_code.find(start_marker)
    end_idx = js_code.find(end_marker)
    
    if start_idx == -1 or end_idx == -1:
        log("❌ Could not find venues object in code.js")
        return

    new_venues_block = f"const venues = {json.dumps(venues_data, indent=2)};\n\n"
    final_js = js_code[:start_idx] + new_venues_block + js_code[end_idx:]
    
    # Inject DATE_CODE
    date_code_start = js_code.find("const DATE_CODE =")
    if date_code_start != -1:
        date_code_end = js_code.find("})();", date_code_start)
        if date_code_end != -1:
             date_code_end += 5
             final_js = final_js[:date_code_start] + f'const DATE_CODE = "{DATE_CODE}";' + final_js[date_code_end:]
             log(f"📅 Injected DATE_CODE: {DATE_CODE}")

    # 3. Run Playwright with STEALTH
    async with async_playwright() as p:
        # Stealth args
        args = [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-infobars",
            "--window-position=0,0",
            "--ignore-certifcate-errors",
            "--ignore-certifcate-errors-spki-list",
            "--disable-accelerated-2d-canvas",
            "--disable-gpu",
        ]
        
        # Randomize UA
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]
        ua = random.choice(user_agents)

        browser = await p.chromium.launch(
            headless=False, # Headless=False helps with cloudflare often
            channel="chrome", # Use real chrome if available
            args=args
        )
        
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1280, "height": 720},
            permissions=["geolocation"],
        )
        
        # Inject stealth scripts
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        page = await context.new_page()

        log("🌍 Navigating to BMS...")
        try:
            # Go to home first
            await page.goto("https://in.bookmyshow.com", wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2) # Wait for page to settle
        except Exception as e:
            log(f"❌ Navigation failed: {e}")
            await browser.close()
            return

        log("💉 Injecting JS...")
        
        execution_done = asyncio.Event()
        downloaded_data = None
        
        async with page.expect_download(timeout=600000) as download_info: 
            page.on("console", lambda msg: print(f"PAGE LOG: {msg.text}"))
            
            # We can also wrap eval in a retry loop if network fails? 
            # But the JS itself has logic.
            # Let's rely on the JS for now.
            try:
                await page.evaluate(final_js)
            except Exception as e:
                log(f"❌ Script evaluation error: {e}")
            
            log("⏳ Waiting for download...")
            try:
                download = await download_info.value
                temp_path = await download.path()
                
                with open(temp_path, "r", encoding="utf-8") as f:
                    downloaded_data = json.load(f)
                    
                log(f"📥 Downloaded {len(downloaded_data)} raw rows")
            except Exception as e:
                 log(f"❌ Download failed or timed out: {e}")

        await browser.close()

    if not downloaded_data:
        log("❌ No data downloaded")
        return

    # 4. Process & Summary (Matching user logic)
    processed_rows = []
    
    # Just filter/process if needed, mostly pass through
    processed_rows = downloaded_data

    # Summary Generation
    summary = {}

    for r in processed_rows:
        movie = r.get("movie", "Unknown")
        city  = r.get("city", "Unknown")
        state = r.get("state", "Unknown")
        venue = r.get("venue", "Unknown")
        lang  = r.get("language", "Unknown")
        dim   = r.get("dimension", "Unknown")

        sold  = r.get("sold", 0)
        total = r.get("totalSeats", 0)
        gross = r.get("gross", 0)
        occ   = calc_occupancy(sold, total)

        if movie not in summary:
            summary[movie] = {
                "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                "venues": set(), "cities": set(),
                "fastfilling": 0, "housefull": 0,
                "details": {}, # Key: (city, state)
                "Language_details": {}, # Key: lang
                "Format_details": {} # Key: dim
            }

        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)

        if occ >= 98: m["housefull"] += 1
        elif occ >= 50: m["fastfilling"] += 1

        # -------- CITY --------
        ck = (city, state)
        if ck not in m["details"]:
            m["details"][ck] = {
                "city": city, "state": state,
                "venues": set(), "shows": 0,
                "gross": 0.0, "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }
        d = m["details"][ck]
        d["venues"].add(venue)
        d["shows"] += 1
        d["gross"] += gross
        d["sold"] += sold
        d["totalSeats"] += total
        if occ >= 98: d["housefull"] += 1
        elif occ >= 50: d["fastfilling"] += 1

        # -------- LANGUAGE --------
        if lang not in m["Language_details"]:
            m["Language_details"][lang] = {
                "language": lang,
                "venues": set(), "shows": 0,
                "gross": 0.0, "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }
        L = m["Language_details"][lang]
        L["venues"].add(venue)
        L["shows"] += 1
        L["gross"] += gross
        L["sold"] += sold
        L["totalSeats"] += total
        if occ >= 98: L["housefull"] += 1
        elif occ >= 50: L["fastfilling"] += 1

        # -------- FORMAT --------
        if dim not in m["Format_details"]:
            m["Format_details"][dim] = {
                "dimension": dim,
                "venues": set(), "shows": 0,
                "gross": 0.0, "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }
        F = m["Format_details"][dim]
        F["venues"].add(venue)
        F["shows"] += 1
        F["gross"] += gross
        F["sold"] += sold
        F["totalSeats"] += total
        if occ >= 98: F["housefull"] += 1
        elif occ >= 50: F["fastfilling"] += 1

    # Final Summary Structure
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
            "occupancy": calc_occupancy(m["sold"], m["totalSeats"]),
            "City_details": [],
            "Language_details": [],
            "Format_details": []
        }

        # Flatten City Details
        for d in m["details"].values():
            final_summary[movie]["City_details"].append({
                "city": d["city"],
                "state": d["state"],
                "venues": len(d["venues"]),
                "shows": d["shows"],
                "gross": round(d["gross"], 2),
                "sold": d["sold"],
                "totalSeats": d["totalSeats"],
                "fastfilling": d["fastfilling"],
                "housefull": d["housefull"],
                "occupancy": calc_occupancy(d["sold"], d["totalSeats"])
            })

        # Flatten Language Details
        for l in m["Language_details"].values():
            final_summary[movie]["Language_details"].append({
                "language": l["language"],
                "venues": len(l["venues"]),
                "shows": l["shows"],
                "gross": round(l["gross"], 2),
                "sold": l["sold"],
                "totalSeats": l["totalSeats"],
                "fastfilling": l["fastfilling"],
                "housefull": l["housefull"],
                "occupancy": calc_occupancy(l["sold"], l["totalSeats"])
            })

        # Flatten Format Details
        for f in m["Format_details"].values():
            final_summary[movie]["Format_details"].append({
                "dimension": f["dimension"],
                "venues": len(f["venues"]),
                "shows": f["shows"],
                "gross": round(f["gross"], 2),
                "sold": f["sold"],
                "totalSeats": f["totalSeats"],
                "fastfilling": f["fastfilling"],
                "housefull": f["housefull"],
                "occupancy": calc_occupancy(f["sold"], f["totalSeats"])
            })

    # 6. Save
    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(processed_rows, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Shows={len(processed_rows)} | Movies={len(final_summary)}")

if __name__ == "__main__":
    asyncio.run(run_shard())
