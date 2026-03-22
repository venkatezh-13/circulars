"""
build_index.py — Rebuilds docs/search_index.json from SQLite database.

Reads from:
  - data/circulars.db (SQLite database with all circulars)

Writes:
  - docs/search_index.json (flat list, used by the frontend)

Each record in the index:
  {
    "exchange": "NSE" | "BSE" | "MCX",
    "date":     "18 Mar 2026",
    "date_iso": "2026-03-18",
    "ref":      "NSE/CML/73363",
    "subject":  "Listing of further issues...",
    "category": "Listing",
    "link":     "https://..."
  }
"""

import os
import json
from datetime import datetime
from generate_rss import generate_rss
from db import get_all_circulars, get_db_size, get_stats

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
OUT_FILE = os.path.join(REPO_ROOT, "docs", "search_index.json")


def to_display(iso: str) -> str:
    """YYYY-MM-DD → DD Mon YYYY"""
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        return iso


def main():
    # Read all circulars from SQLite
    records = get_all_circulars()
    
    # Transform to frontend format
    all_records = []
    for r in records:
        all_records.append({
            "exchange": r["exchange"],
            "date": to_display(r["date_iso"]) if r["date_iso"] else "",
            "date_iso": r["date_iso"] or "",
            "ref": r["ref"] or "",
            "subject": r["subject"] or "",
            "category": r["category"] or "",
            "link": r["link"] or "",
        })

    # Write index
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_records, f, separators=(",", ":"), ensure_ascii=False)

    # Print stats
    stats = get_stats()
    print(f"\nIndex built: {len(all_records):,} total records → {OUT_FILE}")
    size_kb = os.path.getsize(OUT_FILE) / 1024
    print(f"Index size: {size_kb:.1f} KB")
    print(f"Database size: {get_db_size() / 1024:.1f} KB")
    for ex, count in stats['by_exchange'].items():
        print(f"  [{ex}] {count:,} records")

    generate_rss()


if __name__ == "__main__":
    main()
