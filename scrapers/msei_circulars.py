"""
MSEI (Metropolitan Stock Exchange of India) Circulars Scraper
============================================================
Scrapes circulars from https://www.msei.in/downloads/circulars/default.aspx
"""

import requests
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MSEI_URL = "https://www.msei.in/downloads/circulars/default.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def parse_msei_date(raw_date: str) -> str:
    """Convert '23-Sep-2026' -> 'YYYY-MM-DD'"""
    try:
        dt = datetime.strptime(raw_date.strip(), "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return raw_date

def scrape_msei_circulars() -> list:
    """Fetch and parse MSEI circulars from default.aspx SSR payload."""
    session = requests.Session()
    session.headers.update(HEADERS)

    print(f"[*] Fetching MSEI circulars from {MSEI_URL}...")
    try:
        resp = session.get(MSEI_URL, verify=False, timeout=30)
        if resp.status_code != 200:
            print(f"  [!] HTTP status {resp.status_code}")
            return []
        
        html = resp.text
        # Clean unescaped quotes/slashes in Next.js stream payload
        clean_html = html.replace('\\"', '"').replace('\\/', '/')

        # Find the start of the "rows" JSON array using bracket counting
        # so nested objects inside rows don't cause truncation
        key_marker = '"rows":'
        key_pos = clean_html.find(key_marker)
        if key_pos == -1:
            print("  [!] Could not locate 'rows' key in MSEI payload")
            return []

        array_start = clean_html.find('[', key_pos + len(key_marker))
        if array_start == -1:
            print("  [!] Could not locate 'rows' array start in MSEI payload")
            return []

        # Walk forward counting brackets to find the matching ']'
        depth = 0
        array_end = array_start
        for i, ch in enumerate(clean_html[array_start:], array_start):
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    array_end = i
                    break
        else:
            print("  [!] Could not find closing bracket for 'rows' array in MSEI payload")
            return []

        try:
            raw_rows = json.loads(clean_html[array_start:array_end + 1])
        except json.JSONDecodeError as e:
            print(f"  [!] JSON parse error for MSEI rows: {e}")
            return []

        circulars = []
        for r in raw_rows:
            circ_no = str(r.get("circularNo", "")).strip()
            date_raw = str(r.get("date", "")).strip()
            date_iso = parse_msei_date(date_raw)
            title = str(r.get("title", "")).strip()
            segments = str(r.get("segments", "")).strip()
            department = str(r.get("department", "")).strip()
            href = str(r.get("href", "")).strip()

            if circ_no or title:
                circulars.append({
                    "exchange": "MSEI",
                    "circular_no": circ_no,
                    "date": date_raw,
                    "date_iso": date_iso,
                    "title": title,
                    "subject": title,
                    "category": segments or department or "General",
                    "department": department,
                    "link": href,
                })

        print(f"  [+] Successfully parsed {len(circulars)} MSEI circulars")
        return circulars

    except Exception as e:
        print(f"  [!] Exception scraping MSEI circulars: {e}")
        return []

if __name__ == "__main__":
    results = scrape_msei_circulars()
    print(f"\nFetched {len(results)} circulars:")
    for c in results[:10]:
        print(f" - [{c['date']}] No. {c['circular_no']} | {c['subject'][:60]} | {c['link']}")
