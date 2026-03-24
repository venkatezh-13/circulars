"""
build_index.py — Rebuilds docs/search_index.json from JSON files.

Reads from:
  - data/nse/raw/*.json
  - data/bse/raw/*.json
  - data/mcx/raw/*.json
  - data/sebi/raw/*.json

Writes:
  - docs/search_index.json (flat list, used by the frontend)

Each record in the index:
  {
    "exchange": "NSE" | "BSE" | "MCX" | "SEBI",
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
import glob
from datetime import datetime
from generate_rss import generate_rss

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
OUT_FILE = os.path.join(REPO_ROOT, "docs", "search_index.json")


def to_display(iso: str) -> str:
    """YYYY-MM-DD → DD Mon YYYY"""
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        return iso


def load_exchange_json(exchange: str):
    """Load all JSON files for an exchange."""
    records = []
    raw_dir = os.path.join(REPO_ROOT, "data", exchange.lower(), "raw")
    
    for json_file in glob.glob(os.path.join(raw_dir, "*.json")):
        with open(json_file, encoding="utf-8") as f:
            data = json.load(f)
            for item in data:
                # Extract date from file name
                filename = os.path.basename(json_file)
                date_iso = filename.replace(".json", "")
                
                if exchange == "NSE":
                    records.append({
                        "exchange": "NSE",
                        "date_iso": date_iso,
                        "ref": item.get("circular_ref", ""),
                        "subject": item.get("subject", ""),
                        "category": item.get("department", ""),
                        "link": item.get("link", ""),
                    })
                elif exchange == "BSE":
                    records.append({
                        "exchange": "BSE",
                        "date_iso": date_iso,
                        "ref": item.get("notice_no", ""),
                        "subject": item.get("subject", ""),
                        "category": f"{item.get('segment','')} / {item.get('category','')}".strip(" /"),
                        "link": item.get("pdf_url", ""),
                    })
                elif exchange == "MCX":
                    records.append({
                        "exchange": "MCX",
                        "date_iso": date_iso,
                        "ref": str(item.get("circular_no", "")),
                        "subject": item.get("title", ""),
                        "category": item.get("category", ""),
                        "link": item.get("link", ""),
                    })
                elif exchange == "SEBI":
                    # SEBI data has date_iso in the item itself, not from filename
                    item_date_iso = item.get("date_iso", date_iso)
                    records.append({
                        "exchange": "SEBI",
                        "date": to_display(item_date_iso),
                        "date_iso": item_date_iso,
                        "ref": item.get("notice_no", ""),
                        "subject": item.get("subject", ""),
                        "category": item.get("category", "Circular"),
                        "link": item.get("pdf_url", ""),
                    })
    
    return records


def main():
    # Load all circulars from JSON files
    all_records = []
    
    for exchange in ["NSE", "BSE", "MCX", "SEBI"]:
        records = load_exchange_json(exchange)
        all_records.extend(records)
    
    # Transform to frontend format
    formatted_records = []
    for r in all_records:
        formatted_records.append({
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
        json.dump(formatted_records, f, separators=(",", ":"), ensure_ascii=False)

    # Print stats
    print(f"\nIndex built: {len(formatted_records):,} total records → {OUT_FILE}")
    size_kb = os.path.getsize(OUT_FILE) / 1024
    print(f"Index size: {size_kb:.1f} KB")
    
    by_exchange = {}
    for r in formatted_records:
        ex = r["exchange"]
        by_exchange[ex] = by_exchange.get(ex, 0) + 1
    
    for ex, count in sorted(by_exchange.items()):
        print(f"  [{ex}] {count:,} records")

    generate_rss()


if __name__ == "__main__":
    main()
