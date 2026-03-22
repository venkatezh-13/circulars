"""
BSE daily scraper — called by GitHub Actions.
Scrapes today's circulars and saves to SQLite database.
"""
import json
import sys
import os
from datetime import date
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from bse_circulars import main as bse_main  # Import main function
from db import init_db, get_connection

def fmt_date(d):
    """Format date as DD/MM/YYYY for BSE"""
    return d.strftime("%d/%m/%Y")

def to_iso(date_str: str) -> str:
    """Convert BSE notice_no (YYYYMMDD-N) to ISO date"""
    import re
    m = re.match(r"(\d{4})(\d{2})(\d{2})", date_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""

def main():
    # Ensure database is initialized
    init_db()
    
    today = date.today()
    date_str = fmt_date(today)
    
    # Set up command-line arguments for bse_circulars.py
    temp_file = "bse_temp.json"
    sys.argv = ["bse_circulars.py", "--date", date_str, "--out", temp_file]
    
    # Run the BSE scraper's main function
    bse_main()
    
    # Read the temporary output
    if os.path.exists(temp_file):
        with open(temp_file, encoding="utf-8") as f:
            circulars = json.load(f)
        
        # Insert into SQLite database
        conn = get_connection()
        inserted = 0
        for c in circulars:
            notice_no = c.get("notice_no", "")
            date_iso = to_iso(notice_no)
            if notice_no and date_iso:
                try:
                    conn.execute("""
                        INSERT INTO circulars (exchange, date_iso, ref, subject, category, link, segment, department)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        "BSE",
                        date_iso,
                        notice_no,
                        c.get("subject", ""),
                        f"{c.get('segment','')} / {c.get('category','')}".strip(" /"),
                        c.get("pdf_url", ""),
                        c.get("segment", ""),
                        c.get("department", ""),
                    ))
                    inserted += 1
                except Exception:
                    pass  # Duplicate
        conn.commit()
        conn.close()
        
        print(f"Inserted {inserted} new BSE circulars into database")
        
        # Clean up temp file
        os.remove(temp_file)
    else:
        print("Error: BSE scraper did not create output file")
        sys.exit(1)

if __name__ == "__main__":
    main()
