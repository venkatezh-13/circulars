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


from concurrent.futures import ThreadPoolExecutor
import re

def parse_single_md(entry_path, exchange):
    try:
        with open(entry_path, "rb") as file:
            raw = file.read(4096)

        p1 = raw.find(b"---")
        if p1 == -1: return None
        p2 = raw.find(b"---", p1 + 3)
        if p2 == -1: return None
        fm = raw[p1+3:p2].decode("utf-8", "ignore")

        guid = ""
        date_iso = ""
        description = ""
        category = "Circular"
        circular_id = ""

        for line in fm.splitlines():
            if line.startswith("guid:"):
                guid = line[5:].strip().strip("'\"")
            elif line.startswith("date:"):
                date_iso = line[5:].strip().strip("'\"")
            elif line.startswith("description:"):
                description = line[12:].strip().strip("'\"")
            elif line.startswith("category:"):
                category = line[9:].strip().strip("'\"")
            elif line.startswith("circular_id:"):
                circular_id = line[12:].strip().strip("'\"")

        notice_no = ""
        if exchange == "BSE":
            idx_no = guid.find("noticeno=")
            if idx_no != -1:
                end = guid.find("&", idx_no)
                notice_no = guid[idx_no+9:end] if end != -1 else guid[idx_no+9:]
            else:
                m_fn = re.search(r'bse-(\d{4}-\d{2}-\d{2})-(.*)\.md$', os.path.basename(entry_path))
                if m_fn: notice_no = m_fn.group(2)[:30]
        elif exchange == "NSE":
            m_nse = re.search(r'/([A-Z0-9_-]+)\.(pdf|zip|xls|xlsx|csv)', guid, re.IGNORECASE)
            if m_nse:
                notice_no = f"NSE/{m_nse.group(1)}"
            else:
                notice_no = circular_id or os.path.basename(entry_path).replace(".md", "")
        elif exchange == "SEBI":
            m_sebi = re.search(r'/([a-z0-9_-]+)_(\d+)\.html', guid, re.IGNORECASE)
            if m_sebi:
                notice_no = f"SEBI/{m_sebi.group(2)}"
            else:
                notice_no = circular_id or os.path.basename(entry_path).replace(".md", "")
        else:
            notice_no = circular_id or os.path.basename(entry_path).replace(".md", "")

        if not date_iso:
            m_fn = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(entry_path))
            if m_fn:
                date_iso = m_fn.group(1)

        if not date_iso: return None

        subject = description or os.path.basename(entry_path).replace(".md", "").replace("-", " ").title()
        link = guid
        if exchange == "BSE" and re.match(r'^\d{8}-\d+$', notice_no):
            link = f"https://www.bseindia.com/downloads/UploadDocs/Notices/{notice_no}/{notice_no}.pdf"

        return {
            "exchange": exchange,
            "date": to_display(date_iso),
            "date_iso": date_iso,
            "ref": notice_no,
            "subject": subject,
            "category": category,
            "link": link,
        }
    except Exception:
        return None


def load_rhnvrm_records():
    rhnvrm_dir = os.path.join(REPO_ROOT, "scratch", "rhnvrm_repo", "hugo-site", "content", "circulars")
    if not os.path.exists(rhnvrm_dir):
        print("Cloning Rohan's stock-market-circulars repository (live data fetch)...")
        target_clone = os.path.join(REPO_ROOT, "scratch", "rhnvrm_repo")
        os.makedirs(os.path.dirname(target_clone), exist_ok=True)
        try:
            import subprocess
            subprocess.run(["git", "clone", "--depth", "1", "https://github.com/rhnvrm/stock-market-circulars.git", target_clone], check=True)
        except Exception as e:
            print(f"Error cloning Rohan's repo: {e}")
            return []

    print("Reading Rohan's stock-market-circulars dataset directly across NSE, BSE, SEBI...")
    
    tasks = []
    for ex in ["nse", "bse", "sebi"]:
        ex_dir = os.path.join(rhnvrm_dir, ex)
        if not os.path.exists(ex_dir):
            continue
        for root, dirs, files in os.walk(ex_dir):
            for file in files:
                if file.endswith(".md"):
                    tasks.append((os.path.join(root, file), ex.upper()))

    records = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        results = executor.map(lambda t: parse_single_md(t[0], t[1]), tasks)
        records = [r for r in results if r is not None]

    print(f"Loaded {len(records):,} records directly from Rohan's dataset.")
    return records


def main():
    # Load all circulars from JSON files
    all_records = []
    
    for exchange in ["NSE", "BSE", "MCX", "SEBI"]:
        records = load_exchange_json(exchange)
        all_records.extend(records)

    # Load Rohan's dataset directly without storing raw files in git data/
    rhn_records = load_rhnvrm_records()
    
    # Deduplicate
    seen_keys = set()
    formatted_records = []

    for r in all_records + rhn_records:
        key = (r.get("exchange"), r.get("ref") or r.get("subject"))
        if key in seen_keys:
            continue
        seen_keys.add(key)

        formatted_records.append({
            "exchange": r["exchange"],
            "date": to_display(r["date_iso"]) if r.get("date_iso") else "",
            "date_iso": r.get("date_iso") or "",
            "ref": r.get("ref") or "",
            "subject": r.get("subject") or "",
            "category": r.get("category") or "",
            "link": r.get("link") or "",
        })

    # Write index
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(formatted_records, f, separators=(",", ":"), ensure_ascii=False)

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
