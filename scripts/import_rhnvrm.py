"""
scripts/import_rhnvrm.py — Importer for rhnvrm/stock-market-circulars dataset.
Converts 26,780 historical BSE circular markdown files into monthly JSON archives
(data/bse/archive/YYYY-MM.json) to keep the repository lightweight.
"""

import os
import re
import json
from datetime import datetime

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
RHNVRM_DIR = os.path.join(REPO_ROOT, "scratch", "rhnvrm_repo", "hugo-site", "content", "circulars")


def parse_md_file(file_path):
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            content = f.read(2048)

        m_fm = re.search(r'^---\s*\n(.*?)\n---', content, re.DOTALL)
        if not m_fm:
            return None

        fm_text = m_fm.group(1)

        guid = ""
        date_iso = ""
        description = ""
        category = "Circular"

        for line in fm_text.splitlines():
            if line.startswith("guid:"):
                guid = line.split(":", 1)[1].strip().strip("'\"")
            elif line.startswith("date:"):
                date_iso = line.split(":", 1)[1].strip().strip("'\"")
            elif line.startswith("description:"):
                description = line.split(":", 1)[1].strip().strip("'\"")
            elif line.startswith("category:"):
                category = line.split(":", 1)[1].strip().strip("'\"")

        notice_no = ""
        m_noticeno = re.search(r'noticeno=([0-9\-]+)', guid)
        if m_noticeno:
            notice_no = m_noticeno.group(1)
        else:
            fname = os.path.basename(file_path)
            m_fn = re.search(r'bse-(\d{4}-\d{2}-\d{2})-(.*)\.md$', fname)
            if m_fn:
                notice_no = m_fn.group(2)[:30]

        if not notice_no:
            return None

        if not date_iso:
            m_d = re.search(r'dt=(\d{2}/\d{2}/\d{4})', guid)
            if m_d:
                try:
                    date_iso = datetime.strptime(m_d.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                except Exception:
                    pass

        if not date_iso:
            m_fn = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(file_path))
            if m_fn:
                date_iso = m_fn.group(1)

        if not date_iso:
            return None

        subject = description or os.path.basename(file_path).replace(".md", "")
        pdf_url = f"https://www.bseindia.com/downloads/UploadDocs/Notices/{notice_no}/{notice_no}.pdf" if re.match(r'^\d{8}-\d+$', notice_no) else guid

        date_disp = date_iso
        try:
            date_disp = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d %b %Y")
        except Exception:
            pass

        return {
            "exchange": "BSE",
            "date": date_disp,
            "date_iso": date_iso,
            "ref": notice_no,
            "subject": subject,
            "category": category,
            "link": pdf_url,
        }
    except Exception:
        return None


def main():
    print("=== Scanning rhnvrm BSE Markdown Files ===")
    md_files = []
    for root, dirs, files in os.walk(RHNVRM_DIR):
        for file in files:
            if file.endswith(".md"):
                md_files.append(os.path.join(root, file))

    print(f"Found {len(md_files):,} markdown files.")

    by_month = {}
    valid_count = 0

    for fpath in md_files:
        parsed = parse_md_file(fpath)
        if not parsed or not parsed["date_iso"]:
            continue

        valid_count += 1
        month_key = parsed["date_iso"][:7]
        by_month.setdefault(month_key, []).append(parsed)

    print(f"Parsed {valid_count:,} valid circulars across {len(by_month)} months.")

    archive_dir = os.path.join(REPO_ROOT, "data", "bse", "archive")
    os.makedirs(archive_dir, exist_ok=True)

    new_total = 0
    for month_key, items in sorted(by_month.items()):
        out_file = os.path.join(archive_dir, f"{month_key}.json")
        existing = []
        if os.path.exists(out_file):
            with open(out_file, encoding="utf-8") as f:
                existing = json.load(f)

        seen_refs = {x.get("ref", "") or x.get("subject", "") for x in existing}
        new_items = [x for x in items if (x.get("ref", "") or x.get("subject", "")) not in seen_refs]
        combined = existing + new_items

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2, ensure_ascii=False)

        new_total += len(new_items)
        print(f"  [{month_key}] Saved {len(combined):,} circulars ({len(new_items):,} new) -> {os.path.basename(out_file)}")

    print(f"\nImported {new_total:,} new BSE records into lightweight monthly archive files.")


if __name__ == "__main__":
    main()
