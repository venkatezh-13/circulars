"""
NSE daily scraper — called by GitHub Actions.
Scrapes today's circulars and saves to SQLite database.
"""
import json, sys, os, re
from datetime import date
from dataclasses import asdict
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from nse_circulars import scrape_nse_circulars
from db import init_db, get_connection

MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
}

def to_iso(date_str: str) -> str:
    """Convert NSE date (January 01, 2026) to ISO date"""
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", date_str)
    if m:
        mon = MONTH_MAP.get(m.group(1).lower()[:3], 0)
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(2)):02d}"
    return ""

def main():
    # Ensure database is initialized
    init_db()
    
    today = date.today()
    circulars = scrape_nse_circulars(today, today, use_cache=False)

    # Insert into SQLite database
    conn = get_connection()
    inserted = 0
    for c in circulars:
        item = c if isinstance(c, dict) else asdict(c)
        date_iso = to_iso(item.get("date", ""))
        ref = item.get("circular_ref", "")
        if ref and date_iso:
            try:
                conn.execute("""
                    INSERT INTO circulars (exchange, date_iso, ref, subject, category, link, segment, department)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    "NSE",
                    date_iso,
                    ref,
                    item.get("subject", ""),
                    item.get("department", ""),
                    item.get("link", ""),
                    None,
                    item.get("department", ""),
                ))
                inserted += 1
            except Exception:
                pass  # Duplicate
    conn.commit()
    conn.close()

    print(f"Inserted {inserted} new NSE circulars into database")

if __name__ == "__main__":
    main()
