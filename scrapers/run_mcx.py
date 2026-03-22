"""
MCX daily scraper — called by GitHub Actions.
Scrapes today's circulars and saves to data/mcx/raw/YYYY-MM-DD.json
"""
import json, sys, os
from datetime import date
from dataclasses import asdict
sys.path.insert(0, os.path.dirname(__file__))
from mcx_circulars import scrape_mcx_circulars

def main():
    today = date.today()
    circulars = scrape_mcx_circulars(today, today)

    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "mcx", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{today}.json")

    records = []
    for c in circulars:
        records.append(c if isinstance(c, dict) else asdict(c))

    existing = []
    if os.path.exists(out_file):
        with open(out_file, encoding="utf-8") as f:
            existing = json.load(f)

    seen, unique = set(), []
    for item in existing + records:
        key = item.get("circular_no", "") or item.get("title", "")
        if key not in seen:
            seen.add(key)
            unique.append(item)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(unique, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(unique)} MCX circulars to {out_file}")

if __name__ == "__main__":
    main()
