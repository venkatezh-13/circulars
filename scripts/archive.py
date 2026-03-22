"""
archive.py — Daily archival job (run by GitHub Actions).

Logic:
  - Scans data/{exchange}/raw/ for JSON files older than 30 days
  - Appends their records to data/{exchange}/archive/YYYY-MM.txt
  - Deletes the raw JSON after archiving to keep the repo lean
  - Each line in the .txt is a pipe-delimited record for fast grep/search
  - Also removes raw files older than 365 days from archive consideration
    (they should already be archived)

Safe to re-run — checks for duplicates before appending.
"""

import os
import json
import glob
from datetime import date, datetime, timedelta

REPO_ROOT  = os.path.join(os.path.dirname(__file__), "..")
EXCHANGES  = ["nse", "bse", "mcx"]
KEEP_DAYS  = 30    # keep daily JSONs for this many days before archiving


# ── Field extractors per exchange ─────────────────────────────────────────────

def nse_to_line(r: dict) -> str:
    return "|".join([
        r.get("date", ""),
        r.get("circular_ref", ""),
        r.get("department", ""),
        r.get("subject", "").replace("|", " "),
        r.get("link", "") or "",
    ])

def bse_to_line(r: dict) -> str:
    return "|".join([
        r.get("date", ""),
        r.get("notice_no", ""),
        r.get("segment", ""),
        r.get("category", ""),
        r.get("department", ""),
        r.get("subject", "").replace("|", " "),
        r.get("pdf_url", "") or "",
    ])

def mcx_to_line(r: dict) -> str:
    return "|".join([
        r.get("date", ""),
        r.get("circular_no", ""),
        r.get("category", ""),
        r.get("title", "").replace("|", " "),
        r.get("link", "") or "",
    ])

CONVERTERS = {"nse": nse_to_line, "bse": bse_to_line, "mcx": mcx_to_line}

# Archive file headers
HEADERS = {
    "nse": "# date|circular_ref|department|subject|link",
    "bse": "# date|notice_no|segment|category|department|subject|attachment",
    "mcx": "# date|circular_no|category|title|link",
}


# ── Unique key per record (for dedup) ────────────────────────────────────────

def record_key(exchange: str, r: dict) -> str:
    if exchange == "nse":
        return r.get("circular_ref", "") or r.get("subject", "")
    if exchange == "bse":
        return r.get("notice_no", "") or r.get("subject", "")
    if exchange == "mcx":
        return r.get("circular_no", "") or r.get("title", "")
    return ""


# ── Main ──────────────────────────────────────────────────────────────────────

def archive_exchange(exchange: str):
    raw_dir     = os.path.join(REPO_ROOT, "data", exchange, "raw")
    archive_dir = os.path.join(REPO_ROOT, "data", exchange, "archive")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    converter = CONVERTERS[exchange]
    cutoff    = date.today() - timedelta(days=KEEP_DAYS)
    archived  = 0
    deleted   = 0

    for json_file in sorted(glob.glob(os.path.join(raw_dir, "*.json"))):
        fname = os.path.basename(json_file)
        try:
            file_date = datetime.strptime(fname.replace(".json", ""), "%Y-%m-%d").date()
        except ValueError:
            continue

        if file_date >= cutoff:
            continue   # keep recent files as-is

        # Determine target archive file: YYYY-MM.txt
        month_key    = file_date.strftime("%Y-%m")
        archive_file = os.path.join(archive_dir, f"{month_key}.txt")

        # Load existing archive keys to avoid duplicates
        existing_keys: set[str] = set()
        if os.path.exists(archive_file):
            with open(archive_file, encoding="utf-8") as f:
                for line in f:
                    if not line.startswith("#") and "|" in line:
                        existing_keys.add(line.split("|")[1].strip())

        # Load raw JSON
        try:
            with open(json_file, encoding="utf-8") as f:
                records = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  Skipping {fname}: {e}")
            continue

        # Write new records to archive
        new_lines = []
        for r in records:
            key = record_key(exchange, r)
            if key and key not in existing_keys:
                new_lines.append(converter(r))
                existing_keys.add(key)

        if new_lines:
            # Write header if file is new
            write_header = not os.path.exists(archive_file)
            with open(archive_file, "a", encoding="utf-8") as f:
                if write_header:
                    f.write(HEADERS[exchange] + "\n")
                for line in new_lines:
                    f.write(line + "\n")
            archived += len(new_lines)
            print(f"  Archived {len(new_lines)} records from {fname} → {month_key}.txt")

        # Delete raw file after archiving
        os.remove(json_file)
        deleted += 1

    print(f"  [{exchange.upper()}] Archived {archived} records, deleted {deleted} raw files")


def main():
    print(f"Starting archive job — cutoff: {date.today() - timedelta(days=KEEP_DAYS)}")
    for exchange in EXCHANGES:
        print(f"\nProcessing {exchange.upper()}...")
        archive_exchange(exchange)
    print("\nArchive job complete.")


if __name__ == "__main__":
    main()
