"""
backfill.py — Safe historical data scraper.

Run once per day by GitHub Actions. Each run scrapes ONE WEEK of historical
data per exchange, then stops. Over time this builds up the full archive
without triggering rate limits or IP bans.

State is tracked in scripts/backfill_state.json so each run picks up
where the last one left off.

Usage:
    python scripts/backfill.py              # auto mode (reads state file)
    python scripts/backfill.py --exchange nse --from 2025-01-01 --to 2025-01-07
"""

import os, sys, json, time, random, argparse
from datetime import date, datetime, timedelta
from dataclasses import asdict

REPO_ROOT   = os.path.join(os.path.dirname(__file__), "..")
STATE_FILE  = os.path.join(os.path.dirname(__file__), "backfill_state.json")
BATCH_DAYS  = 7     # days scraped per run per exchange
MIN_YEAR    = 2020  # how far back to go

sys.path.insert(0, os.path.join(REPO_ROOT, "scrapers"))
sys.path.insert(0, os.path.dirname(__file__))


# ── State management ──────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    # Default: start from yesterday, work backwards
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    return {
        "nse": {"next_to_date": yesterday, "done": False},
        "bse": {"next_to_date": yesterday, "done": False},
        "mcx": {"next_to_date": yesterday, "done": False},
    }

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Per-exchange scrape ───────────────────────────────────────────────────────

def scrape_nse_batch(from_date: date, to_date: date) -> list:
    from nse_circulars import scrape_nse_circulars
    circulars = scrape_nse_circulars(from_date, to_date, use_cache=False)
    return [asdict(c) if hasattr(c, "__dataclass_fields__") else c for c in circulars]

def scrape_bse_batch(from_date: date, to_date: date) -> list:
    from bse_circulars import main as bse_main, fetch_for_url, make_session
    from bse_circulars import ARCHIVE_URL, RECENT_URL, fmt_date, parse_date
    session = make_session()
    # Decide which URL to use
    days_ago = (date.today() - to_date).days
    url = ARCHIVE_URL if days_ago > 7 else RECENT_URL
    rows = fetch_for_url(session, url, fmt_date(from_date), fmt_date(to_date))
    return rows

def scrape_mcx_batch(from_date: date, to_date: date) -> list:
    from mcx_circulars import scrape_mcx_circulars
    circulars = scrape_mcx_circulars(from_date, to_date)
    return [asdict(c) if hasattr(c, "__dataclass_fields__") else c for c in circulars]

SCRAPERS = {"nse": scrape_nse_batch, "bse": scrape_bse_batch, "mcx": scrape_mcx_batch}


# ── Save batch ────────────────────────────────────────────────────────────────

MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
}

def to_iso(date_str: str) -> str:
    """Convert any date string to YYYY-MM-DD for sorting."""
    import re
    s = date_str.strip()
    if re.match(r"\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", s)
    if m:
        mon = MONTH_MAP.get(m.group(2).lower()[:3], 0)
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(1)):02d}"
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", s)
    if m:
        mon = MONTH_MAP.get(m.group(1).lower()[:3], 0)
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(2)):02d}"
    m = re.match(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


def save_batch(exchange: str, from_date: date, to_date: date, records: list):
    """Save records split by their individual dates into daily files."""

    # Group by date
    by_date: dict[str, list] = {}
    for r in records:
        # Extract date from record
        if exchange == "bse":
            raw_date = r.get("notice_no", "") or r.get("date", "")
        elif exchange == "nse":
            raw_date = r.get("date", "")
        else:
            raw_date = r.get("date", "")

        iso = to_iso(raw_date)
        if not iso:
            iso = from_date.isoformat()
        by_date.setdefault(iso, []).append(r)

    raw_dir = os.path.join(REPO_ROOT, "data", exchange, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    total_saved = 0

    for iso, day_records in by_date.items():
        out_file = os.path.join(raw_dir, f"{iso}.json")
        existing = []
        if os.path.exists(out_file):
            with open(out_file, encoding="utf-8") as f:
                existing = json.load(f)

        # Dedup
        if exchange == "bse":
            key_fn = lambda r: r.get("notice_no", "") or r.get("subject", "")
        elif exchange == "nse":
            key_fn = lambda r: r.get("circular_ref", "") or r.get("subject", "")
        else:
            key_fn = lambda r: r.get("circular_no", "") or r.get("title", "")

        seen = {key_fn(r) for r in existing}
        new  = [r for r in day_records if key_fn(r) not in seen]
        all_data = existing + new

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)

        total_saved += len(new)

    print(f"  Saved {total_saved} new records across {len(by_date)} dates")
    return total_saved


# ── Main ──────────────────────────────────────────────────────────────────────

def run_batch(exchange: str, to_date: date) -> date:
    """Scrape one batch ending at to_date. Returns the new to_date for next run."""
    from_date = to_date - timedelta(days=BATCH_DAYS - 1)
    min_date  = date(MIN_YEAR, 1, 1)

    if from_date < min_date:
        from_date = min_date

    print(f"  Scraping {exchange.upper()}: {from_date} → {to_date}")

    try:
        records = SCRAPERS[exchange](from_date, to_date)
        print(f"  Fetched {len(records)} records")
        save_batch(exchange, from_date, to_date, records)
    except Exception as e:
        print(f"  ERROR scraping {exchange}: {e}")

    # Sleep between exchanges to be polite
    time.sleep(random.uniform(3.0, 6.0))

    # Next run should go further back
    next_to = from_date - timedelta(days=1)
    return next_to


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exchange", choices=["nse","bse","mcx","all"], default="all")
    parser.add_argument("--from", dest="from_date")
    parser.add_argument("--to",   dest="to_date")
    args = parser.parse_args()

    state = load_state()
    min_date = date(MIN_YEAR, 1, 1)

    if args.from_date and args.to_date:
        # Manual mode
        from_d = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        to_d   = datetime.strptime(args.to_date,   "%Y-%m-%d").date()
        exchanges = [args.exchange] if args.exchange != "all" else ["nse","bse","mcx"]
        for exc in exchanges:
            records = SCRAPERS[exc](from_d, to_d)
            save_batch(exc, from_d, to_d, records)
        return

    # Auto mode — run one batch per exchange
    exchanges = [args.exchange] if args.exchange != "all" else ["nse","bse","mcx"]

    for exc in exchanges:
        exc_state = state.get(exc, {})
        if exc_state.get("done"):
            print(f"  [{exc.upper()}] backfill already complete, skipping")
            continue

        next_to_str = exc_state.get("next_to_date")
        to_d = datetime.strptime(next_to_str, "%Y-%m-%d").date() if next_to_str else date.today() - timedelta(days=1)

        if to_d < min_date:
            print(f"  [{exc.upper()}] reached {MIN_YEAR}, marking done")
            state[exc]["done"] = True
            save_state(state)
            continue

        next_to = run_batch(exc, to_d)
        state[exc]["next_to_date"] = next_to.isoformat()
        state[exc]["done"] = next_to < min_date
        save_state(state)
        print(f"  [{exc.upper()}] next run will fetch up to {next_to}")

    print("\nBackfill batch complete. State saved.")


if __name__ == "__main__":
    main()
