"""
migrate_to_sqlite.py — Migrate existing JSON data to SQLite database.

Reads all JSON files from data/{exchange}/raw/ and imports them into
the SQLite database. Also reads archive TXT files.
"""

import os
import json
import glob
import re
import sqlite3
from datetime import datetime
from db import init_db, get_connection

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

# Date parsing
MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
}

def to_iso(date_str: str) -> str:
    """Convert any date string to YYYY-MM-DD."""
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


def parse_nse_record(r: dict) -> dict:
    """Parse NSE JSON record."""
    iso = to_iso(r.get("date", ""))
    return {
        "exchange": "NSE",
        "date_iso": iso,
        "ref": r.get("circular_ref", ""),
        "subject": r.get("subject", ""),
        "category": r.get("department", ""),
        "link": r.get("link", "") or "",
        "segment": None,
        "department": r.get("department", ""),
    }


def parse_bse_record(r: dict) -> dict:
    """Parse BSE JSON record."""
    notice = r.get("notice_no", "")
    iso = to_iso(notice) or to_iso(r.get("date", ""))
    return {
        "exchange": "BSE",
        "date_iso": iso,
        "ref": notice,
        "subject": r.get("subject", ""),
        "category": f"{r.get('segment','')} / {r.get('category','')}".strip(" /"),
        "link": r.get("pdf_url", "") or "",
        "segment": r.get("segment", ""),
        "department": r.get("department", ""),
    }


def parse_mcx_record(r: dict) -> dict:
    """Parse MCX JSON record."""
    iso = to_iso(r.get("date", ""))
    return {
        "exchange": "MCX",
        "date_iso": iso,
        "ref": str(r.get("circular_no", "")),
        "subject": r.get("title", ""),
        "category": r.get("category", ""),
        "link": r.get("link", "") or "",
        "segment": None,
        "department": None,
    }


# Archive line parsers
def parse_nse_line(line: str) -> dict:
    p = line.strip().split("|")
    if len(p) < 5: return {}
    iso = to_iso(p[0])
    return {
        "exchange": "NSE", "date_iso": iso, "ref": p[1],
        "category": p[2], "subject": p[3], "link": p[4],
        "segment": None, "department": p[2],
    }


def parse_bse_line(line: str) -> dict:
    p = line.strip().split("|")
    if len(p) < 7: return {}
    iso = to_iso(p[0]) or to_iso(p[1])
    return {
        "exchange": "BSE", "date_iso": iso, "ref": p[1],
        "category": f"{p[2]} / {p[3]}".strip(" /"),
        "subject": p[5], "link": p[6],
        "segment": p[2], "department": p[4] if len(p) > 4 else None,
    }


def parse_mcx_line(line: str) -> dict:
    p = line.strip().split("|")
    if len(p) < 5: return {}
    iso = to_iso(p[0])
    return {
        "exchange": "MCX", "date_iso": iso, "ref": p[1],
        "category": p[2], "subject": p[3], "link": p[4],
        "segment": None, "department": None,
    }


RECORD_PARSERS = {"nse": parse_nse_record, "bse": parse_bse_record, "mcx": parse_mcx_record}
LINE_PARSERS = {"nse": parse_nse_line, "bse": parse_bse_line, "mcx": parse_mcx_line}


def migrate():
    """Migrate all data to SQLite."""
    print("Initializing database...")
    init_db()
    
    conn = get_connection()
    total_inserted = 0
    stats = {"NSE": 0, "BSE": 0, "MCX": 0}

    for exchange in ["nse", "bse", "mcx"]:
        raw_dir = os.path.join(REPO_ROOT, "data", exchange, "raw")
        archive_dir = os.path.join(REPO_ROOT, "data", exchange, "archive")
        rp = RECORD_PARSERS[exchange]
        lp = LINE_PARSERS[exchange]

        # Raw JSON files
        json_files = sorted(glob.glob(os.path.join(raw_dir, "*.json")))
        for f in json_files:
            try:
                with open(f, encoding="utf-8") as fh:
                    for r in json.load(fh):
                        rec = rp(r)
                        if rec and rec.get("date_iso") and rec.get("ref"):
                            try:
                                conn.execute("""
                                    INSERT INTO circulars (exchange, date_iso, ref, subject, category, link, segment, department)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """, (rec["exchange"], rec["date_iso"], rec["ref"], rec["subject"],
                                      rec["category"], rec["link"], rec.get("segment"), rec.get("department")))
                                stats[rec["exchange"]] += 1
                                total_inserted += 1
                            except sqlite3.IntegrityError:
                                pass  # Duplicate
            except Exception as e:
                print(f"  Warning: {f}: {e}")

        # Archive TXT files
        txt_files = sorted(glob.glob(os.path.join(archive_dir, "*.txt")))
        for f in txt_files:
            try:
                with open(f, encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            rec = lp(line)
                            if rec and rec.get("date_iso") and rec.get("ref"):
                                try:
                                    conn.execute("""
                                        INSERT INTO circulars (exchange, date_iso, ref, subject, category, link, segment, department)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (rec["exchange"], rec["date_iso"], rec["ref"], rec["subject"],
                                          rec["category"], rec["link"], rec.get("segment"), rec.get("department")))
                                    stats[rec["exchange"]] += 1
                                    total_inserted += 1
                                except sqlite3.IntegrityError:
                                    pass
            except Exception as e:
                print(f"  Warning: {f}: {e}")

        print(f"  [{exchange.upper()}] {stats[exchange.upper()]:,} records")

    conn.commit()
    conn.close()

    print(f"\nMigration complete: {total_inserted:,} total records")
    return total_inserted


if __name__ == "__main__":
    migrate()
