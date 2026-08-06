"""
MCX Circulars Scraper - Production Ready
==========================================
Install:
    pip install httpx

Usage:
    python mcx_circulars.py
"""

import httpx
import json
import re
import time
import random
import logging
import os
from datetime import date, datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

FROM_DATE        = date(2026, 3, 18)
TO_DATE          = date(2026, 3, 18)

API_URL          = "https://www.mcxindia.com/circulars/all-circulars/GetFilteredAnnouncements"
HOME_URL         = "https://www.mcxindia.com/circulars/all-circulars"

MAX_RETRIES      = 3
RETRY_DELAY      = 2        # base seconds (multiplied per attempt)
RATE_LIMIT_MIN   = 1.5      # min seconds between requests
RATE_LIMIT_MAX   = 4.0      # max seconds between requests
PAGE_SIZE        = 100      # warn if result count hits this
CACHE_DIR        = "mcx_cache"   # folder to store cached responses
OFF_PEAK_ONLY    = False    # set True to only run between 00:00–07:00 IST


# ── User-Agent pool ───────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class MCXCircular:
    date: str
    category: str
    title: str
    circular_no: str
    link: Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_mcx_date(raw: str) -> str:
    """Convert /Date(1773772200000)/ to DD Mon YYYY."""
    match = re.search(r'/Date\((\d+)\)/', str(raw))
    if match:
        ts = int(match.group(1)) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d %b %Y")
    return str(raw)


def make_headers() -> dict:
    """Build realistic Chrome headers with Akamai bot bypass params."""
    ua = random.choice(USER_AGENTS)
    return {
        "User-Agent":          ua,
        "Accept":              "application/json, text/javascript, */*; q=0.01",
        "Accept-Language":     "en-US,en;q=0.9",
        "Accept-Encoding":     "gzip, deflate, br",
        "Referer":             HOME_URL,
        "X-Requested-With":    "XMLHttpRequest",
        "Connection":          "keep-alive",
        "sec-ch-ua":           '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-mobile":    "?0",
        "sec-ch-ua-platform":  '"Windows"',
        "sec-fetch-dest":      "empty",
        "sec-fetch-mode":      "cors",
        "sec-fetch-site":      "same-origin",
    }


def random_delay():
    """Sleep for a random human-like duration."""
    t = random.uniform(RATE_LIMIT_MIN, RATE_LIMIT_MAX)
    log.debug(f"  Sleeping {t:.1f}s")
    time.sleep(t)


def is_off_peak() -> bool:
    """Check if current IST time is between 00:00 and 07:00."""
    ist_hour = (datetime.utcnow().hour + 5) % 24   # UTC+5:30 approx
    return 0 <= ist_hour < 7


def cache_path(from_date: date, to_date: date, circular_type: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{from_date}_{to_date}_{circular_type}.json")


def load_cache(from_date: date, to_date: date, circular_type: str) -> Optional[list]:
    path = cache_path(from_date, to_date, circular_type)
    if os.path.exists(path):
        log.info(f"  Cache hit: {path}")
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(from_date: date, to_date: date, circular_type: str, data: list):
    path = cache_path(from_date, to_date, circular_type)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info(f"  Cached to: {path}")


# ── Session warm-up ───────────────────────────────────────────────────────────

def warm_session(client: httpx.Client):
    """Visit home page and circulars page to get cookies."""
    try:
        doc_headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
        }
        r0 = client.get("https://www.mcxindia.com", headers=doc_headers, timeout=30)
        time.sleep(1.0)
        r = client.get(HOME_URL, headers=doc_headers, timeout=30)
        log.info(f"  Session warm-up: HTTP {r.status_code}")
        time.sleep(random.uniform(1.0, 2.0))
    except Exception as e:
        log.warning(f"  Session warm-up failed (continuing anyway): {e}")


# ── Core fetch with retry ─────────────────────────────────────────────────────

def fetch_page(
    client: httpx.Client,
    from_str: str,
    to_str: str,
    page: int = 1,
) -> tuple[list[dict], int]:
    params = {
        "CircularsCategory": "CircularsCategory",
        "CircularCategory":  "",
        "CircularNo":        "",
        "fromdate":          from_str,
        "todate":            to_str,
        "searchText":        "",
        "page":              str(page),
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.get(API_URL, headers=make_headers(), params=params, timeout=30)

            if r.status_code in (401, 403):
                raise ValueError(f"HTTP {r.status_code} — session may be required. Re-warming...")

            if r.status_code != 200:
                raise ValueError(f"HTTP {r.status_code}: {r.text[:300]}")

            data = r.json()

            if isinstance(data, dict) and data.get("success") is False:
                err = data.get("Error", "Unknown API error")
                log.warning(f"  API returned success=false: {err}")
                return [], 0

            items = data.get("AllResult", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            total_pages = data.get("TotalPages", 1) if isinstance(data, dict) else 1

            log.info(f"  Fetched {len(items)} items on page {page}/{total_pages} (attempt {attempt})")
            return items, int(total_pages or 1)

        except ValueError as e:
            last_error = e
            log.warning(f"  Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if "403" in str(e) and attempt < MAX_RETRIES:
                log.info("  Re-warming session after 403...")
                warm_session(client)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)

        except (httpx.TimeoutException, httpx.NetworkError) as e:
            last_error = e
            log.warning(f"  Network error attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)

    log.error(f"All {MAX_RETRIES} attempts failed. Last: {last_error}")
    return [], 0


# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape_mcx_circulars(
    from_date: date,
    to_date: date,
    circular_type: str = "ALL",
    use_cache: bool = True,
) -> list[MCXCircular]:

    # Off-peak guard
    if OFF_PEAK_ONLY and not is_off_peak():
        log.warning("Not running — OFF_PEAK_ONLY is enabled and it's business hours (IST)")
        return []

    # Cache check
    if use_cache:
        cached = load_cache(from_date, to_date, circular_type)
        if cached is not None:
            return [MCXCircular(**c) for c in cached]

    from_str = from_date.strftime("%d/%m/%Y")
    to_str   = to_date.strftime("%d/%m/%Y")
    log.info(f"Fetching MCX circulars: {from_date} -> {to_date} | type={circular_type}")

    all_circulars = []
    with httpx.Client(follow_redirects=True, timeout=30) as client:
        warm_session(client)
        page = 1
        seen_refs = set()
        while True:
            items, total_pages = fetch_page(client, from_str, to_str, page=page)
            if not items:
                break

            for item in items:
                try:
                    disp_date = item.get("DisplayDate") or item.get("DisplayCircularDate") or parse_mcx_date(item.get("CircularDate", ""))
                    link      = item.get("CircularFile") or item.get("Documents") or ""
                    if link and not link.startswith("http"):
                        link = "https://www.mcxindia.com" + link

                    circ_no = str(item.get("CircularNo", ""))
                    key = (circ_no, item.get("Title", ""))
                    if key in seen_refs:
                        continue
                    seen_refs.add(key)

                    all_circulars.append(MCXCircular(
                        date=disp_date,
                        category=item.get("CircularsCategory") or item.get("CircularTypesName") or "",
                        title=item.get("Title", ""),
                        circular_no=circ_no,
                        link=link or None,
                    ))
                except Exception as e:
                    log.warning(f"  Skipping malformed item: {e} | {item}")

            if page >= total_pages:
                break
            page += 1

    if not all_circulars:
        log.warning("No items returned — check date range or API status")
        return []

    # Save to cache
    if use_cache and all_circulars:
        save_cache(from_date, to_date, circular_type, [asdict(c) for c in all_circulars])

    log.info(f"Done — {len(all_circulars)} circulars parsed")
    return all_circulars
    return circulars


# ── Bulk fetch (multiple date ranges) ────────────────────────────────────────

def scrape_multiple_dates(
    date_ranges: list[tuple[date, date]],
    circular_type: str = "ALL",
    use_cache: bool = True,
) -> list[MCXCircular]:
    """
    Fetch circulars for multiple date ranges with rate limiting.
    Example:
        results = scrape_multiple_dates([
            (date(2026, 3, 1),  date(2026, 3, 7)),
            (date(2026, 3, 8),  date(2026, 3, 14)),
            (date(2026, 3, 15), date(2026, 3, 18)),
        ])
    """
    all_circulars = []
    for i, (from_d, to_d) in enumerate(date_ranges):
        if i > 0:
            random_delay()
        results = scrape_mcx_circulars(from_d, to_d, circular_type, use_cache)
        all_circulars.extend(results)
    return all_circulars


# ── Output helpers ────────────────────────────────────────────────────────────

def print_circulars(circulars: list[MCXCircular]) -> None:
    if not circulars:
        print("\nNo circulars found.")
        return
    print(f"\n{'─'*70}")
    print(f"  Found {len(circulars)} circular(s)")
    print(f"{'─'*70}\n")
    for c in circulars:
        print(f"  Date       : {c.date}")
        print(f"  Category   : {c.category}")
        print(f"  Circular No: {c.circular_no}")
        print(f"  Title      : {c.title}")
        if c.link:
            print(f"  Link       : {c.link}")
        print()


def save_json(circulars: list[MCXCircular], filename="mcx_circulars.json") -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump([asdict(c) for c in circulars], f, indent=2, ensure_ascii=False)
    log.info(f"Saved {len(circulars)} circulars → {filename}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    circulars = scrape_mcx_circulars(FROM_DATE, TO_DATE)
    print_circulars(circulars)
    save_json(circulars)
