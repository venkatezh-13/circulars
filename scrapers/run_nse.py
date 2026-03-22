"""
NSE daily scraper — called by GitHub Actions.
Scrapes today's circulars and saves to data/nse/raw/YYYY-MM-DD.json
"""
import json, sys, os
from datetime import date
sys.path.insert(0, os.path.dirname(__file__))
from nse_circulars import scrape_nse_circulars

def main():
    today = date.today()
    circulars = scrape_nse_circulars(today, today, use_cache=False)

    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "nse", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{today}.json")

    existing = []
    if os.path.exists(out_file):
        with open(out_file, encoding="utf-8") as f:
            existing = json.load(f)

    existing_refs = {c.get("circular_ref") for c in existing}
    new = [c.__dict__ if hasattr(c, "__dict__") else c for c in circulars
           if (c.circular_ref if hasattr(c, "circular_ref") else c.get("circular_ref")) not in existing_refs]

    from dataclasses import asdict
    all_data = existing + [asdict(c) if hasattr(c, "__dataclass_fields__") else c for c in circulars]
    # deduplicate
    seen, unique = set(), []
    for item in all_data:
        key = item.get("circular_ref", "") or item.get("subject", "")
        if key not in seen:
            seen.add(key)
            unique.append(item)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(unique, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(unique)} NSE circulars to {out_file}")

if __name__ == "__main__":
    main()
