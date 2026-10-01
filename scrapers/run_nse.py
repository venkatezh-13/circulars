"""
NSE daily scraper — called by GitHub Actions / local.
Scrapes circulars from last fetch date up to today, grouping and saving per date.
"""
import json, sys, os, glob, re
from datetime import date, datetime, timedelta
from dataclasses import asdict
sys.path.insert(0, os.path.dirname(__file__))
from nse_circulars import scrape_nse_circulars

MONTH_MAP = {
    'jan':'01', 'feb':'02', 'mar':'03', 'apr':'04', 'may':'05', 'jun':'06',
    'jul':'07', 'aug':'08', 'sep':'09', 'oct':'10', 'nov':'11', 'dec':'12'
}

def parse_date_iso(date_str: str, default_iso: str) -> str:
    if not date_str:
        return default_iso
    s = str(date_str).strip().replace(',', ', ').replace('  ', ' ')
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r'(\d{1,2})[\s\-]+([A-Za-z]{3,9})[\s\-]+(\d{4})', s)
    if m:
        day, month_name, year = m.group(1), m.group(2).lower()[:3], m.group(3)
        month = MONTH_MAP.get(month_name, '01')
        return f"{year}-{month}-{day.zfill(2)}"
    m2 = re.search(r'(\d{4})(\d{2})(\d{2})', s)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    return default_iso


def get_start_date(out_dir: str, default_lookback_days: int = 7) -> date:
    today = date.today()
    files = glob.glob(os.path.join(out_dir, "*.json"))
    dates = []
    for f in files:
        b = os.path.basename(f).replace(".json", "")
        try:
            d = datetime.strptime(b, "%Y-%m-%d").date()
            if d <= today:
                dates.append(d)
        except ValueError:
            pass
    if dates:
        most_recent = max(dates)
        start = max(most_recent - timedelta(days=1), today - timedelta(days=default_lookback_days))
        return start
    return today - timedelta(days=default_lookback_days)

def main():
    today = date.today()
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "nse", "raw")
    os.makedirs(out_dir, exist_ok=True)

    from_date = get_start_date(out_dir, default_lookback_days=7)
    print(f"Fetching NSE circulars from {from_date} to {today}...")

    circulars = scrape_nse_circulars(from_date, today, use_cache=False)

    # Group circulars by date
    grouped = {}
    for c in circulars:
        c_dict = asdict(c) if hasattr(c, "__dataclass_fields__") else (c.__dict__ if hasattr(c, "__dict__") else c)
        c_date_str = c_dict.get("date") or ""
        d_iso = parse_date_iso(c_date_str, str(today))
        grouped.setdefault(d_iso, []).append(c_dict)

    if str(today) not in grouped:
        grouped[str(today)] = []

    for d_iso, new_items in grouped.items():
        out_file = os.path.join(out_dir, f"{d_iso}.json")
        existing = []
        if os.path.exists(out_file):
            with open(out_file, encoding="utf-8") as f:
                try:
                    existing = json.load(f)
                except Exception:
                    existing = []

        seen, unique = set(), []
        for item in existing + new_items:
            key = item.get("circular_ref", "") or item.get("subject", "")
            if key not in seen:
                seen.add(key)
                unique.append(item)

        tmp_file = out_file + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(unique, f, indent=2, ensure_ascii=False)
        os.replace(tmp_file, out_file)
        print(f"Saved {len(unique)} NSE circulars to {out_file}")

if __name__ == "__main__":
    main()
