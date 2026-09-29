"""
BSE daily scraper — called by GitHub Actions / local.
Scrapes circulars from last fetch date up to today, grouping and saving per date.
"""
import json, sys, os, glob, re
from datetime import date, datetime, timedelta
sys.path.insert(0, os.path.dirname(__file__))
from bse_circulars import main as bse_main

def fmt_date(d):
    return d.strftime("%d/%m/%Y")

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

def parse_bse_date_iso(item: dict, default_iso: str) -> str:
    link = item.get("pdf_url") or item.get("link") or ""
    m = re.search(r'/Notices/(\d{4})(\d{2})(\d{2})-\d+/', link)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    notice_no = str(item.get("notice_no") or "")
    m2 = re.search(r'^(\d{4})(\d{2})(\d{2})-\d+$', notice_no)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    return default_iso

def main():
    today = date.today()
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "bse", "raw")
    os.makedirs(out_dir, exist_ok=True)

    from_date = get_start_date(out_dir, default_lookback_days=7)
    print(f"Fetching BSE circulars from {from_date} to {today}...")

    temp_file = "bse_temp.json"
    sys.argv = ["bse_circulars.py", "--from", fmt_date(from_date), "--to", fmt_date(today), "--out", temp_file]

    bse_main()

    if os.path.exists(temp_file):
        with open(temp_file, encoding="utf-8") as f:
            try:
                circulars = json.load(f)
            except Exception:
                circulars = []

        grouped = {}
        for c in circulars:
            d_iso = parse_bse_date_iso(c, str(today))
            grouped.setdefault(d_iso, []).append(c)

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

            seen = {c.get("notice_no", "") for c in existing}
            for c in new_items:
                notice_no = c.get("notice_no", "")
                if notice_no and notice_no not in seen:
                    existing.append(c)
                    seen.add(notice_no)

            tmp_file = out_file + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)
            os.replace(tmp_file, out_file)
            print(f"Saved {len(existing)} BSE circulars to {out_file}")

        try:
            os.remove(temp_file)
        except OSError:
            pass
    else:
        print("Warning: BSE scraper did not create temporary output file")

if __name__ == "__main__":
    main()
