"""
build_index.py — Rebuilds docs/search_index.json from local data files and index store.

Reads from:
  - data/nse/raw/*.json
  - data/bse/raw/*.json
  - data/mcx/raw/*.json
  - data/sebi/raw/*.json
  - docs/search_index.json (existing historical archive)

Writes:
  - docs/search_index.json (flat list, used by the frontend)
  - docs/status.json (poll timestamp and status)
"""

import os
import json
import glob
import re
from datetime import datetime, date
from generate_rss import generate_rss

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
OUT_FILE = os.path.join(REPO_ROOT, "docs", "search_index.json")


def to_display(iso: str) -> str:
    """YYYY-MM-DD -> DD Mon YYYY"""
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
            try:
                data = json.load(f)
            except Exception:
                continue
            
            filename = os.path.basename(json_file)
            date_iso = filename.replace(".json", "")
            
            for item in data:
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
    # 1. Load raw JSON files from data/
    raw_records = []
    for exchange in ["NSE", "BSE", "MCX", "SEBI"]:
        records = load_exchange_json(exchange)
        raw_records.extend(records)

    # 2. Load existing search_index.json if present (preserves full historical archive)
    current_index_records = []
    if os.path.exists(OUT_FILE):
        with open(OUT_FILE, encoding="utf-8") as f:
            try:
                current_index_records = json.load(f)
            except Exception:
                pass

    # Combine: raw local daily data > existing search index
    all_sources = raw_records + current_index_records

    # Deduplicate & format
    seen_keys = set()
    formatted_records = []
    today_iso = date.today().isoformat()

    for r in all_sources:
        ex = r.get("exchange", "")
        ref = r.get("ref") or ""
        subj = r.get("subject") or ""

        if ex == "NSE":
            m_num = re.search(r'(\d{4,})', ref)
            key = (ex, m_num.group(1)) if m_num else (ex, ref or subj)
        else:
            key = (ex, ref or subj)

        if key in seen_keys:
            continue
        seen_keys.add(key)

        diso = r.get("date_iso") or ""
        link = r.get("link") or ""

        # Auto-correct BSE date from notice link URL if present
        if ex == "BSE" and link:
            m = re.search(r'/Notices/(\d{4})(\d{2})(\d{2})-\d+/', link)
            if m:
                diso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        # Safeguard against future dates
        if diso > today_iso:
            if ref == "NSE/MFSS76357":
                diso = "2026-09-01"
            else:
                diso = today_iso

        formatted_records.append({
            "exchange": ex,
            "date": to_display(diso) if diso else "",
            "date_iso": diso,
            "ref": ref,
            "subject": subj,
            "category": r.get("category") or "",
            "link": link,
        })

    # Write index
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(formatted_records, f, separators=(",", ":"), ensure_ascii=False)

    # Write status.json with last fetched timestamp
    now = datetime.now()
    status_file = os.path.join(REPO_ROOT, "docs", "status.json")
    status_data = {
        "last_updated_iso": now.isoformat(),
        "last_updated_display": now.strftime("%d %b %Y, %I:%M %p IST"),
        "total_records": len(formatted_records)
    }
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(status_data, f, indent=2, ensure_ascii=False)

    # Print stats
    print(f"\nIndex built: {len(formatted_records):,} total records -> {OUT_FILE}")
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
