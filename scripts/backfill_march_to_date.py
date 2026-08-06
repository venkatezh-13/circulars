"""
backfill_march_to_date.py — Backfill circulars from March 2026 to current date (2026-08-06)
for NSE, BSE, MCX, and SEBI across all segments.
"""

import os
import sys
import json
import time
import random
from datetime import date, datetime, timedelta
from dataclasses import asdict

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(REPO_ROOT, "scrapers"))

from nse_circulars import scrape_nse_circulars
from bse_circulars import fetch_from_xml_feed, make_session
from mcx_circulars import scrape_mcx_circulars
from sebi_circulars import main as sebi_main

START_DATE = date(2026, 3, 1)
END_DATE   = date(2026, 8, 6)


def save_nse_daily(circulars):
    by_date = {}
    for c in circulars:
        item = asdict(c) if hasattr(c, "__dataclass_fields__") else c
        raw_d = item.get("date", "")
        # cirDate (YYYYMMDD) or cirDisplayDate
        date_iso = ""
        import re
        m = re.search(r'(\d{4})(\d{2})(\d{2})', str(raw_d))
        if m:
            date_iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        else:
            try:
                date_iso = datetime.strptime(raw_d, "%B %d, %Y").strftime("%Y-%m-%d")
            except Exception:
                pass
        
        if not date_iso:
            continue

        by_date.setdefault(date_iso, []).append(item)

    raw_dir = os.path.join(REPO_ROOT, "data", "nse", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    total_saved = 0

    for iso, items in by_date.items():
        out_file = os.path.join(raw_dir, f"{iso}.json")
        existing = []
        if os.path.exists(out_file):
            with open(out_file, encoding="utf-8") as f:
                existing = json.load(f)
        
        seen = {x.get("circular_ref", "") or x.get("subject", "") for x in existing}
        new_items = [x for x in items if (x.get("circular_ref", "") or x.get("subject", "")) not in seen]
        combined = existing + new_items

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2, ensure_ascii=False)
        total_saved += len(new_items)

    print(f"  [NSE] Saved {total_saved} new records across {len(by_date)} dates.")


def save_mcx_daily(circulars):
    by_date = {}
    for c in circulars:
        item = asdict(c) if hasattr(c, "__dataclass_fields__") else c
        raw_d = item.get("date", "")
        date_iso = ""
        try:
            date_iso = datetime.strptime(raw_d, "%d %b %Y").strftime("%Y-%m-%d")
        except Exception:
            pass

        if not date_iso:
            continue

        by_date.setdefault(date_iso, []).append(item)

    raw_dir = os.path.join(REPO_ROOT, "data", "mcx", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    total_saved = 0

    for iso, items in by_date.items():
        out_file = os.path.join(raw_dir, f"{iso}.json")
        existing = []
        if os.path.exists(out_file):
            with open(out_file, encoding="utf-8") as f:
                existing = json.load(f)

        seen = {x.get("circular_no", "") or x.get("title", "") for x in existing}
        new_items = [x for x in items if (x.get("circular_no", "") or x.get("title", "")) not in seen]
        combined = existing + new_items

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2, ensure_ascii=False)
        total_saved += len(new_items)

    print(f"  [MCX] Saved {total_saved} new records across {len(by_date)} dates.")


def save_bse_xml_records(records):
    by_date = {}
    for r in records:
        notice_no = r.get("notice_no", "")
        # Notice No format e.g. 20260806-23
        import re
        m = re.match(r'(\d{4})(\d{2})(\d{2})-\d+', notice_no)
        if m:
            date_iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            by_date.setdefault(date_iso, []).append(r)

    raw_dir = os.path.join(REPO_ROOT, "data", "bse", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    total_saved = 0

    for iso, items in by_date.items():
        out_file = os.path.join(raw_dir, f"{iso}.json")
        existing = []
        if os.path.exists(out_file):
            with open(out_file, encoding="utf-8") as f:
                existing = json.load(f)

        seen = {x.get("notice_no", "") for x in existing}
        new_items = [x for x in items if x.get("notice_no", "") not in seen]
        combined = existing + new_items

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2, ensure_ascii=False)
        total_saved += len(new_items)

    print(f"  [BSE] Saved {total_saved} new XML records across {len(by_date)} dates.")


def main():
    print(f"=== Starting Archive Backfill ({START_DATE} to {END_DATE}) ===\n")

    # 1. NSE Backfill in weekly chunks
    print("[1/4] Backfilling NSE Circulars...")
    curr = START_DATE
    while curr <= END_DATE:
        chunk_end = min(curr + timedelta(days=6), END_DATE)
        print(f"  NSE chunk: {curr} to {chunk_end}")
        try:
            results = scrape_nse_circulars(curr, chunk_end, use_cache=False)
            if results:
                save_nse_daily(results)
        except Exception as e:
            print(f"  ERROR scraping NSE {curr} to {chunk_end}: {e}")
        curr = chunk_end + timedelta(days=1)
        time.sleep(random.uniform(2.0, 4.0))

    # 2. MCX Backfill in weekly chunks
    print("\n[2/4] Backfilling MCX Circulars...")
    curr = START_DATE
    while curr <= END_DATE:
        chunk_end = min(curr + timedelta(days=6), END_DATE)
        print(f"  MCX chunk: {curr} to {chunk_end}")
        try:
            results = scrape_mcx_circulars(curr, chunk_end, use_cache=False)
            if results:
                save_mcx_daily(results)
        except Exception as e:
            print(f"  ERROR scraping MCX {curr} to {chunk_end}: {e}")
        curr = chunk_end + timedelta(days=1)
        time.sleep(random.uniform(2.0, 4.0))

    # 3. BSE Feed & Recent Sync
    print("\n[3/4] Syncing BSE Notices...")
    try:
        s = make_session()
        xml_records = fetch_from_xml_feed(s)
        if xml_records:
            save_bse_xml_records(xml_records)
    except Exception as e:
        print(f"  ERROR syncing BSE XML feed: {e}")

    # 4. SEBI Backfill
    print("\n[4/4] Syncing SEBI Circulars...")
    try:
        sys.argv = ["sebi_circulars.py", "--from", START_DATE.strftime("%d/%m/%Y"), "--to", END_DATE.strftime("%d/%m/%Y"), "--out", "sebi_temp.json"]
        sebi_main()
        if os.path.exists("sebi_temp.json"):
            with open("sebi_temp.json", encoding="utf-8") as f:
                sebi_items = json.load(f)
            # Group into data/sebi/raw/YYYY-MM-DD.json
            by_date = {}
            for item in sebi_items:
                raw_iso = item.get("date_iso", "")
                if raw_iso:
                    by_date.setdefault(raw_iso, []).append(item)

            raw_dir = os.path.join(REPO_ROOT, "data", "sebi", "raw")
            os.makedirs(raw_dir, exist_ok=True)
            for iso, items in by_date.items():
                out_file = os.path.join(raw_dir, f"{iso}.json")
                existing = []
                if os.path.exists(out_file):
                    with open(out_file, encoding="utf-8") as f:
                        existing = json.load(f)
                seen = {x.get("notice_no", "") for x in existing}
                new_items = [x for x in items if x.get("notice_no", "") not in seen]
                combined = existing + new_items
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(combined, f, indent=2, ensure_ascii=False)
            os.remove("sebi_temp.json")
    except Exception as e:
        print(f"  ERROR syncing SEBI: {e}")

    print("\n=== Archive Backfill Complete ===")


if __name__ == "__main__":
    main()
