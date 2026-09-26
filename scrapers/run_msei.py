"""
MSEI daily scraper — called by GitHub Actions / local.
Scrapes circulars and saves to data/msei/raw/YYYY-MM-DD.json
"""
import json, sys, os
from datetime import date
sys.path.insert(0, os.path.dirname(__file__))
from msei_circulars import scrape_msei_circulars

def main():
    today = date.today()
    try:
        circulars = scrape_msei_circulars()
    except Exception as e:
        print(f"Warning: MSEI scraper error: {e}")
        circulars = []

    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "msei", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{today}.json")

    existing = []
    if os.path.exists(out_file):
        with open(out_file, encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except Exception:
                existing = []

    seen = {c.get("circular_no", "") or c.get("title", "") for c in existing}
    for item in circulars:
        key = item.get("circular_no", "") or item.get("title", "")
        if key and key not in seen:
            existing.append(item)
            seen.add(key)

    tmp_file = out_file + ".tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)
    os.replace(tmp_file, out_file)

    print(f"Saved {len(existing)} MSEI circulars to {out_file}")

if __name__ == "__main__":
    main()
