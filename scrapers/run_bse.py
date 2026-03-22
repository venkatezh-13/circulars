"""
BSE daily scraper — called by GitHub Actions.
Scrapes today's circulars and saves to data/bse/raw/YYYY-MM-DD.json
"""
import json
import sys
import os
from datetime import date
sys.path.insert(0, os.path.dirname(__file__))
from bse_circulars import main as bse_main  # Import main function

def fmt_date(d):
    """Format date as DD/MM/YYYY for BSE"""
    return d.strftime("%d/%m/%Y")

def main():
    today = date.today()
    date_str = fmt_date(today)
    
    # Set up command-line arguments for bse_circulars.py
    # Save to a temporary file first
    temp_file = "bse_temp.json"
    sys.argv = ["bse_circulars.py", "--date", date_str, "--out", temp_file]
    
    # Run the BSE scraper's main function
    bse_main()
    
    # Read the temporary output
    if os.path.exists(temp_file):
        with open(temp_file, encoding="utf-8") as f:
            circulars = json.load(f)
        
        # Save to data/bse/raw/ directory
        out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "bse", "raw")
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f"{today}.json")
        
        # Load existing if any
        existing = []
        if os.path.exists(out_file):
            with open(out_file, encoding="utf-8") as f:
                existing = json.load(f)
        
        # Merge and deduplicate
        seen = {c.get("notice_no", "") for c in existing}
        for c in circulars:
            notice_no = c.get("notice_no", "")
            if notice_no and notice_no not in seen:
                existing.append(c)
                seen.add(notice_no)
        
        # Save merged results
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(existing)} BSE circulars to {out_file}")
        
        # Clean up temp file
        os.remove(temp_file)
    else:
        print("Error: BSE scraper did not create output file")
        sys.exit(1)

if __name__ == "__main__":
    main()
