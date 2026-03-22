"""
NSE Exchange Communication Circulars Scraper - Production Ready
================================================================
Scrapes from https://www.nseindia.com/resources/exchange-communication-circulars

Install:
    pip install httpx

Usage:
    python nse_circulars.py
"""

import httpx
import json
import time
import random
import logging
import os
from datetime import date, datetime
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

FROM_DATE       = date(2026, 3, 18)
TO_DATE         = date(2026, 3, 18)

NSE_HOME        = "https://www.nseindia.com"
CIRCULARS_PAGE  = "https://www.nseindia.com/resources/exchange-communication-circulars"
API_URL         = "https://www.nseindia.com/api/circulars"

MAX_RETRIES     = 3
RETRY_DELAY     = 2
RATE_LIMIT_MIN  = 2.0
RATE_LIMIT_MAX  = 5.0
PAGE_SIZE       = 10
CACHE_DIR       = "nse_cache"
OFF_PEAK_ONLY   = False


# ── User-Agent pool ───────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class NSECircular:
    date: str
    department: str
    subject: str
    circular_ref: str
    link: Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_page_headers() -> dict:
    return {
        "User-Agent":                random.choice(USER_AGENTS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def make_api_headers() -> dict:
    ua = random.choice(USER_AGENTS)
    is_mac = "Macintosh" in ua
    return {
        "User-Agent":         ua,
        "Accept":             "application/json, text/plain, */*",
        "Accept-Language":    "en-US,en;q=0.9",
        # Do NOT send Accept-Encoding here — let httpx handle decompression
        # automatically via its built-in decoder instead of raw bytes
        "Referer":            CIRCULARS_PAGE,
        "sec-ch-ua":          '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"macOS"' if is_mac else '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-origin",
        "Connection":         "keep-alive",
    }


def random_delay():
    t = random.uniform(RATE_LIMIT_MIN, RATE_LIMIT_MAX)
    log.debug(f"  Sleeping {t:.1f}s")
    time.sleep(t)


def is_off_peak() -> bool:
    ist_hour = (datetime.utcnow().hour + 5) % 24
    return 0 <= ist_hour < 7


def cache_path(from_date: date, to_date: date, page: int) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"nse_{from_date}_{to_date}_p{page}.json")


def load_cache(from_date: date, to_date: date, page: int) -> Optional[dict]:
    path = cache_path(from_date, to_date, page)
    if os.path.exists(path):
        log.info(f"  Cache hit: {path}")
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(from_date: date, to_date: date, page: int, data: dict):
    path = cache_path(from_date, to_date, page)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info(f"  Cached: {path}")


# ── Session warm-up ───────────────────────────────────────────────────────────

def warm_session(client: httpx.Client) -> bool:
    """
    NSE Akamai requires cookies set by the circulars page before the API works.
    Homepage returns 403 but still sets AKA_A2 cookie — that's fine and expected.
    Circulars page returns 200 and sets _abck, bm_sz, nsit cookies — these are needed.
    """
    try:
        log.info("  Warming session (step 1/2): NSE homepage...")
        r1 = client.get(NSE_HOME, headers=make_page_headers(), timeout=30)
        log.info(f"    Homepage: HTTP {r1.status_code} | cookies: {list(client.cookies.keys())}")
        time.sleep(random.uniform(1.5, 3.0))

        log.info("  Warming session (step 2/2): Circulars page...")
        r2 = client.get(CIRCULARS_PAGE, headers=make_page_headers(), timeout=30)
        log.info(f"    Circulars page: HTTP {r2.status_code} | cookies: {list(client.cookies.keys())}")
        time.sleep(random.uniform(1.0, 2.0))

        return True
    except Exception as e:
        log.warning(f"  Session warm-up failed: {e}")
        return False


# ── Fetch single page ─────────────────────────────────────────────────────────

def fetch_page(
    client: httpx.Client,
    from_str: str,
    to_str: str,
    page: int = 1,
) -> Optional[dict]:
    params = {
        "fromDate": from_str,
        "toDate":   to_str,
        "page":     page,
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.get(
                API_URL,
                params=params,
                headers=make_api_headers(),
                timeout=30,
            )

            if r.status_code in (401, 403):
                log.warning(f"  HTTP {r.status_code} — re-warming session...")
                warm_session(client)
                time.sleep(RETRY_DELAY)
                continue

            if r.status_code != 200:
                raise ValueError(f"HTTP {r.status_code}: {r.text[:300]}")

            # ── Decode response safely ────────────────────────────────────────
            # NSE returns gzip/deflate-compressed JSON.
            # r.content gives raw bytes; r.text auto-decodes using detected charset.
            # Use r.content + explicit decode fallback chain to handle edge cases.
            try:
                text = r.text   # httpx auto-decompresses and decodes
            except Exception:
                # Fallback: try common encodings manually
                for enc in ("utf-8", "latin-1", "cp1252"):
                    try:
                        text = r.content.decode(enc)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError("Could not decode response with any encoding")

            data = json.loads(text)
            log.info(f"  Page {page} fetched (attempt {attempt}) — keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            return data

        except (httpx.TimeoutException, httpx.NetworkError) as e:
            last_error = e
            log.warning(f"  Network error attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)

        except (ValueError, json.JSONDecodeError) as e:
            last_error = e
            log.warning(f"  Parse error attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    log.error(f"  All {MAX_RETRIES} attempts failed. Last: {last_error}")
    return None


# ── Parse items ───────────────────────────────────────────────────────────────

def parse_items(items: list) -> list[NSECircular]:
    circulars = []
    for item in items:
        try:
            # Field names confirmed from NSE API response:
            # cirDisplayDate, cirDate, sub, circDisplayNo, circDepartment, circFilelink
            circ_date = (
                item.get("cirDisplayDate") or item.get("cirDate") or
                item.get("circularDate") or item.get("date") or ""
            )
            department = (
                item.get("circDepartment") or item.get("circCategory") or
                item.get("department") or item.get("issuingDepartment") or ""
            )
            subject = (
                item.get("sub") or item.get("subject") or
                item.get("Subject") or item.get("title") or ""
            )
            ref = (
                item.get("circDisplayNo") or item.get("circularRefNumber") or
                item.get("circNumber") or item.get("refNo") or ""
            )
            link = (
                item.get("circFilelink") or item.get("url") or
                item.get("link") or item.get("pdfLink") or ""
            )
            if link and not link.startswith("http"):
                link = NSE_HOME + link

            circulars.append(NSECircular(
                date=circ_date,
                department=department,
                subject=subject,
                circular_ref=ref,
                link=link or None,
            ))
        except Exception as e:
            log.warning(f"  Skipping malformed item: {e} | {item}")
    return circulars


# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape_nse_circulars(
    from_date: date,
    to_date: date,
    use_cache: bool = True,
) -> list[NSECircular]:

    if OFF_PEAK_ONLY and not is_off_peak():
        log.warning("OFF_PEAK_ONLY enabled — skipping (not 00:00–07:00 IST)")
        return []

    from_str = from_date.strftime("%d-%m-%Y")
    to_str   = to_date.strftime("%d-%m-%Y")
    log.info(f"Fetching NSE circulars: {from_date} → {to_date}")

    all_circulars: list[NSECircular] = []

    with httpx.Client(follow_redirects=True, timeout=30) as client:
        warm_session(client)

        page = 1
        total_pages = 1

        while page <= total_pages:
            if use_cache:
                cached = load_cache(from_date, to_date, page)
                if cached is not None:
                    items = cached.get("data", cached.get("circulars", []))
                    all_circulars.extend(parse_items(items if isinstance(items, list) else []))
                    page += 1
                    continue

            if page > 1:
                random_delay()

            data = fetch_page(client, from_str, to_str, page)

            if data is None:
                log.error(f"  Failed to fetch page {page} — stopping")
                break

            # Print sample on first page for field discovery
            if page == 1:
                items_sample = data.get("data", data.get("circulars", []))
                if items_sample:
                    log.info(f"  Sample item keys: {list(items_sample[0].keys())}")
                    log.info(f"  Sample: {json.dumps(items_sample[0], indent=2)}")

            items_raw = (
                data.get("data") or data.get("circulars") or
                data.get("results") or
                (data if isinstance(data, list) else [])
            )

            if not items_raw:
                log.info(f"  No items on page {page} — done")
                break

            # Calculate pagination
            total_records = data.get("total") or data.get("totalRecords") or data.get("count") or 0
            per_page      = data.get("noofrecords") or data.get("perPage") or PAGE_SIZE
            if total_records and per_page:
                total_pages = -(-int(total_records) // int(per_page))
                log.info(f"  Page {page}/{total_pages} | total: {total_records} records")

            if use_cache:
                save_cache(from_date, to_date, page, data)

            circulars = parse_items(items_raw)
            all_circulars.extend(circulars)
            log.info(f"  Parsed {len(circulars)} circulars from page {page}")

            if len(items_raw) < int(per_page):
                break

            page += 1

    log.info(f"Done — {len(all_circulars)} total circulars")
    return all_circulars


# ── Bulk fetch ────────────────────────────────────────────────────────────────

def scrape_multiple_dates(
    date_ranges: list[tuple[date, date]],
    use_cache: bool = True,
) -> list[NSECircular]:
    all_circulars = []
    for i, (from_d, to_d) in enumerate(date_ranges):
        if i > 0:
            random_delay()
        results = scrape_nse_circulars(from_d, to_d, use_cache)
        all_circulars.extend(results)
    return all_circulars


# ── Output ────────────────────────────────────────────────────────────────────

def print_circulars(circulars: list[NSECircular]) -> None:
    if not circulars:
        print("\nNo circulars found.")
        return
    print(f"\n{'─'*70}")
    print(f"  Found {len(circulars)} NSE circular(s)")
    print(f"{'─'*70}\n")
    for c in circulars:
        print(f"  Date       : {c.date}")
        print(f"  Department : {c.department}")
        print(f"  Ref        : {c.circular_ref}")
        print(f"  Subject    : {c.subject}")
        if c.link:
            print(f"  Link       : {c.link}")
        print()


def save_json(circulars: list[NSECircular], filename="nse_circulars.json") -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump([asdict(c) for c in circulars], f, indent=2, ensure_ascii=False)
    log.info(f"Saved {len(circulars)} circulars → {filename}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    circulars = scrape_nse_circulars(FROM_DATE, TO_DATE)
    print_circulars(circulars)
    save_json(circulars)
