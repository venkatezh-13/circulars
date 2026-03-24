"""
Split monthly SEBI JSON files into daily files.
Converts data/sebi/raw/YYYY-MM.json → data/sebi/raw/YYYY-MM-DD.json
"""
import json
import os
from collections import defaultdict

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.join(REPO_ROOT, "data", "sebi", "raw")

def split_monthly_to_daily():
    """Split monthly SEBI files into daily files."""
    
    # Find all monthly files (YYYY-MM.json pattern, not YYYY-MM-DD.json)
    # YYYY-MM.json = 12 chars (e.g., "2026-03.json"), YYYY-MM-DD.json = 15 chars
    monthly_files = [
        f for f in os.listdir(RAW_DIR) 
        if f.endswith('.json') and '-' in f and len(f) == 12
    ]
    
    if not monthly_files:
        print("No monthly files found to split.")
        return
    
    print(f"Found {len(monthly_files)} monthly file(s) to split:")
    for f in monthly_files:
        print(f"  - {f}")
    
    for filename in monthly_files:
        filepath = os.path.join(RAW_DIR, filename)
        
        # Read monthly data
        with open(filepath, encoding='utf-8') as f:
            circulars = json.load(f)
        
        # Group by date_iso
        by_date = defaultdict(list)
        for circular in circulars:
            date_iso = circular.get('date_iso', '')
            if date_iso:
                by_date[date_iso].append(circular)
        
        # Save daily files
        daily_count = 0
        for date_iso, items in sorted(by_date.items()):
            # Convert YYYY-MM-DD to date object for filename
            daily_filename = f"{date_iso}.json"
            daily_filepath = os.path.join(RAW_DIR, daily_filename)
            
            # Load existing if any
            existing = []
            if os.path.exists(daily_filepath):
                with open(daily_filepath, encoding='utf-8') as f:
                    existing = json.load(f)
            
            # Merge and deduplicate
            seen = {c.get('notice_no', '') for c in existing}
            for item in items:
                notice_no = item.get('notice_no', '')
                if notice_no and notice_no not in seen:
                    existing.append(item)
                    seen.add(notice_no)
            
            # Save
            with open(daily_filepath, 'w', encoding='utf-8') as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)
            
            daily_count += 1
            print(f"  Created {daily_filename} with {len(existing)} circulars")
        
        # Archive the monthly file (rename with .backup extension)
        backup_path = filepath + '.backup'
        os.rename(filepath, backup_path)
        print(f"  Backed up {filename} → {filename}.backup\n")
    
    print(f"✓ Split complete! Created {daily_count} daily files.")


if __name__ == "__main__":
    split_monthly_to_daily()
