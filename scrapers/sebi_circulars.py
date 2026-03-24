"""
SEBI Circulars Fetcher
======================
Fetches circulars for a given date range from SEBI India.

Two endpoints exist on SEBI:
  - Recent  : doListing=yes        (active circulars)
  - Archive : doListingCirArchive=yes (archived circulars)

Usage
-----
  python sebi_circulars.py                          # uses FROM_DATE / TO_DATE below
  python sebi_circulars.py --date 24/03/2026        # single date
  python sebi_circulars.py --from 01/01/2026 --to 24/03/2026  # range
  python sebi_circulars.py --date 24/03/2026 --out my_file.json

Date format: DD/MM/YYYY
"""

import re
import sys
import json
import time
import random
import argparse
from datetime import datetime, date, timedelta
from html import unescape
import requests

# ── Date config (edit these) ─────────────────────────────────────────────────
FROM_DATE = "01/02/2026"
TO_DATE   = "28/02/2026"

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do"

# URL parameters for recent and archive
RECENT_PARAMS = {"doListing": "yes", "sid": "1", "ssid": "7", "smid": "0"}
ARCHIVE_PARAMS = {"doListingCirArchive": "yes", "sid": "1", "ssid": "7", "smid": "0"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language":           "en-US,en;q=0.9",
    "Accept-Encoding":           "gzip, deflate, br",
    "Connection":                "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

FMT = "%d/%m/%Y"


# ── Date helpers ──────────────────────────────────────────────────────────────
def parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), FMT).date()

def fmt_date(d: date) -> str:
    return d.strftime(FMT)

def parse_sebi_date(s: str) -> date:
    """Parse SEBI date format like 'Mar 23, 2026' or 'Mar 23,2026'"""
    s = s.strip().replace(",", ", ").replace(",  ", ", ")
    try:
        return datetime.strptime(s, "%b %d, %Y").date()
    except ValueError:
        # Try without space after comma
        return datetime.strptime(s.replace(", ", ","), "%b %d,%Y").date()


# ── HTML helpers ──────────────────────────────────────────────────────────────
def strip_tags(html_frag: str) -> str:
    return unescape(" ".join(re.sub(r"<[^>]+>", " ", html_frag).split()))


def extract_hidden(html: str) -> dict:
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR",
                 "__EVENTVALIDATION", "__VIEWSTATEENCRYPTED"]:
        m = re.search(rf'id="{re.escape(name)}" value="([^"]*)"', html)
        fields[name] = m.group(1) if m else ""
    return fields


# ── Parsing ───────────────────────────────────────────────────────────────────
def _parse_rows(rows: list) -> list:
    results = []
    for row in rows:
        # SEBI HTML has malformed <td> tags (missing closing tags)
        # Use a different approach: find <td> starts and extract content
        td_starts = list(re.finditer(r'<td[^>]*>', row, re.I))

        if len(td_starts) < 2:
            continue

        # Extract content between <td> tags
        # First cell: from first <td> to second <td>
        first_td_end = td_starts[0].end()
        second_td_start = td_starts[1].start()
        date_html = row[first_td_end:second_td_start]

        # Second cell: from second <td> to end of row (before </tr> or end)
        second_td_end = td_starts[1].end()
        # Find the next <td or </tr or end of string
        next_tag = re.search(r'</tr>|<td[^>]*>', row[second_td_end:], re.I)
        if next_tag:
            title_html = row[second_td_end:second_td_end + next_tag.start()]
        else:
            title_html = row[second_td_end:]

        date_str = strip_tags(date_html)
        title = strip_tags(title_html)

        # Look for link in the title cell (SEBI links to HTML detail pages)
        link_m = re.search(r'href="([^"]+)"', title_html, re.I)
        if link_m:
            detail_url = link_m.group(1)
            if detail_url.startswith("/"):
                detail_url = "https://www.sebi.gov.in" + detail_url
            elif not detail_url.startswith("http"):
                detail_url = "https://www.sebi.gov.in/sebiweb/home/" + detail_url
        else:
            detail_url = ""

        # Skip if no valid date or title
        if not date_str or not title:
            continue

        try:
            circular_date = parse_sebi_date(date_str)
        except ValueError:
            circular_date = None

        # Create a shorter, cleaner reference number
        # Format: SEBI/YYYY-MM-DD/XXXX where XXXX is last 4 chars of URL ID
        url_slug = detail_url.split('/')[-1] if detail_url else ""
        url_id_match = re.search(r'_(\d+)\.html$', url_slug)
        url_id = url_id_match.group(1) if url_id_match else "0000"
        
        # Use last 4 digits of URL ID, or pad if shorter
        ref_suffix = url_id[-4:].zfill(4)
        notice_no = f"SEBI/{circular_date.isoformat()}/{ref_suffix}" if circular_date else f"SEBI/{ref_suffix}"

        results.append({
            "notice_no": notice_no,
            "date": date_str,
            "date_iso": circular_date.isoformat() if circular_date else None,
            "subject": title,
            "segment": "General",  # SEBI doesn't categorize by segment
            "category": "Circular",  # All are circulars
            "department": "SEBI",  # Generic department
            "pdf_url": detail_url,  # Using detail_url as pdf_url for consistency
        })
    return results


def parse_html(html: str) -> tuple:
    """
    Parse SEBI circulars table.
    Returns (results, total_records)
    """
    # Find total records
    total_m = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)\s+records', html, re.I)
    total_records = int(total_m.group(3)) if total_m else 0

    # Find all table rows with data (circulars)
    # SEBI uses <tr role='row' class='odd'> for data rows
    rows = re.findall(r"<tr[^>]*role=['\"]row['\"][^>]*>(.*?)</tr>", html, re.DOTALL | re.I)

    if not rows:
        # Fallback: find any table rows with <td> cells
        all_rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.I)
        rows = [r for r in all_rows if re.search(r'<td', r, re.I)]

    results = _parse_rows(rows)

    return results, total_records


def get_pager_pages(html: str) -> list:
    """Find pagination page numbers."""
    from html import unescape as _unescape

    # Unescape HTML entities
    html = _unescape(html)

    # Look for page numbers in pagination
    pages = re.findall(r'[?&;]page=(\d+)["\'&]', html, re.I)
    if not pages:
        # Try JavaScript pagination
        pages = re.findall(r"goToPage\(['\"]?(\d+)['\"]?\)", html, re.I)
    if not pages:
        # Look for page numbers in pager links
        pages = re.findall(r">\s*(\d+)\s*<", html)

    # Deduplicate and sort, exclude page 1 (already loaded)
    return sorted(set(int(p) for p in pages if int(p) > 1))


# ── HTTP ──────────────────────────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def get_page(session: requests.Session, params: dict, page: int = 1) -> str:
    """Fetch a page of circulars."""
    req_params = params.copy()
    if page > 1:
        req_params["page"] = str(page)

    resp = session.get(
        BASE_URL,
        params=req_params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text


def post_filter(
    session: requests.Session,
    params: dict,
    prev_html: str,
    from_date: str,
    to_date: str,
    page: int = 1,
) -> str:
    """Post filter request for date range."""
    hidden = extract_hidden(prev_html)

    data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "__VIEWSTATEENCRYPTED": "",
        "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
        "date": from_date,
        "toDate": to_date,
        "department": "",
        "subSection": "",
        "subSubSection": "",
        "intermediary": "",
    }

    req_params = params.copy()
    if page > 1:
        req_params["page"] = str(page)

    resp = session.post(
        BASE_URL,
        params=req_params,
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": BASE_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items()),
            "Origin": "https://www.sebi.gov.in",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text


# ── Core fetcher ──────────────────────────────────────────────────────────────
def fetch_circulars(
    session: requests.Session,
    params: dict,
    from_date: date,
    to_date: date,
) -> list:
    """Fetch circulars for a date range from a given endpoint."""
    print(f"[*] Fetching from endpoint: {'archive' if 'Archive' in str(params) else 'recent'}")

    all_rows = []
    page = 1
    seen_titles = set()

    while True:
        print(f"    Fetching page {page} ...")

        if page == 1:
            html = get_page(session, params, page)
        else:
            time.sleep(random.uniform(1.0, 2.0))
            html = get_page(session, params, page)

        rows, total_records = parse_html(html)
        print(f"    Page {page}: {len(rows)} circulars (total: {total_records})")

        if not rows:
            break

        # Filter by date range
        for row in rows:
            try:
                row_date = parse_sebi_date(row["date"]) if row.get("date") else None
            except ValueError:
                row_date = None

            if row_date and from_date <= row_date <= to_date:
                # Deduplicate by notice_no
                key = row.get("notice_no", "")
                if key not in seen_titles:
                    seen_titles.add(key)
                    all_rows.append(row)

        # Check if we've reached the end
        if len(rows) < 25:  # SEBI shows 25 per page
            break

        # Check if we should continue
        last_row_date = None
        for row in reversed(rows):
            try:
                last_row_date = parse_sebi_date(row["date"])
                break
            except ValueError:
                continue

        if last_row_date and last_row_date < from_date:
            # All remaining records are before our range
            break

        page += 1

        # Safety limit
        if page > 100:
            print("    Warning: reached page limit")
            break

    return all_rows


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Fetch SEBI Circulars for a date or date range."
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--date",  metavar="DD/MM/YYYY")
    grp.add_argument("--from",  dest="from_date", metavar="DD/MM/YYYY")
    parser.add_argument("--to", dest="to_date",   metavar="DD/MM/YYYY")
    parser.add_argument("--out", metavar="FILE")
    parser.add_argument("--archive", action="store_true", help="Fetch from archive only")
    parser.add_argument("--recent", action="store_true", help="Fetch from recent only")
    args = parser.parse_args()

    if args.date:
        from_dt = to_dt = parse_date(args.date)
    elif args.from_date:
        if not args.to_date:
            parser.error("--to is required when --from is used")
        from_dt = parse_date(args.from_date)
        to_dt   = parse_date(args.to_date)
        if from_dt > to_dt:
            parser.error("--from must be on or before --to")
    else:
        from_dt = parse_date(FROM_DATE)
        to_dt   = parse_date(TO_DATE)

    from_str  = fmt_date(from_dt)
    to_str    = fmt_date(to_dt)
    out_file  = args.out or "sebi_circulars_cache.json"

    print(f"[*] Date range : {from_str}  →  {to_str}")
    print(f"[*] Output     : {out_file}")

    session = make_session()
    all_circulars = []

    # Determine which endpoints to use
    use_recent = args.recent or (not args.archive)
    use_archive = args.archive or (not args.recent)

    if use_recent:
        print("\n[*] Fetching from Recent circulars...")
        rows = fetch_circulars(session, RECENT_PARAMS, from_dt, to_dt)
        all_circulars.extend(rows)

    if use_archive:
        if use_recent and all_circulars:
            time.sleep(random.uniform(1.0, 2.0))
        print("\n[*] Fetching from Archive circulars...")
        rows = fetch_circulars(session, ARCHIVE_PARAMS, from_dt, to_dt)
        all_circulars.extend(rows)

    # Sort by date descending
    def sort_key(c):
        try:
            return parse_sebi_date(c["date"])
        except ValueError:
            return date.min

    all_circulars = sorted(all_circulars, key=sort_key, reverse=True)

    # Display
    sep = "=" * 70
    range_label = from_str if from_str == to_str else f"{from_str} to {to_str}"
    print(f"\n{sep}")
    print(f"  SEBI Circulars  {range_label}  (total: {len(all_circulars)})")
    print(sep)
    for i, c in enumerate(all_circulars, 1):
        print(f"\n[{i:03d}] {c['notice_no']}  |  {c['subject']}")
        print(f"      Segment  : {c['segment']}")
        print(f"      Category : {c['category']}")
        print(f"      Dept     : {c['department']}")
        print(f"      PDF      : {c['pdf_url']}")

    # Merge into cache file
    try:
        with open(out_file, encoding="utf-8") as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cache = []

    existing_ids = {c.get("notice_no") for c in cache}
    added = [c for c in all_circulars if c.get("notice_no") not in existing_ids]
    cache.extend(added)
    cache = sorted(cache, key=lambda x: x.get("date_iso", ""), reverse=True)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
    print(f"\n[+] {len(added)} new  |  {len(cache)} total in cache → {out_file}")


if __name__ == "__main__":
    main()
