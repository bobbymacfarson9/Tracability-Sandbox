"""
Rename 2026 loading slip files from slip numbers (56-61) to calendar-week naming.

NOTE: Slip 56 = week 4 of 2026 (not week 1). Use fix_2026_slip_week_offset.py if files
were incorrectly renamed as Week 1-6. Correct mapping: 56->4, 57->5, 58->6, 59->7, 60->8, 61->9.

This script renames raw slip numbers to calendar weeks (for NEW files arriving as 56-61):
  Week 56 ... -> Week 4 Loading Slip 2026.xlsx  (slip 56 = week 4)
  Week 57 ... -> Week 5 Loading Slip 2026.xlsx
  Week 58 ... -> Week 6 Loading Slip 2026.xlsx
  Week 59 ... -> Week 7 Loading Slip 2026.xlsx
  Week 60 ... -> Week 8 Loading Slip 2026.xlsx
  Week 61 ... -> Week 9 Loading Slip 2026.xlsx

Also fixes "Slipp" -> "Slip" and normalizes casing.

Run from repo root:
  python Scripts/rename_2026_loading_slips.py        # dry run (print only)
  python Scripts/rename_2026_loading_slips.py --apply   # perform renames
"""

import json
import re
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
REF_DATA_DIR = BASE_DIR / "Reference_Data"

# Slip number (56-61) -> calendar week in 2026. Slip 56 = week 4, not week 1.
ISO_TO_2026_CALENDAR = {56: 4, 57: 5, 58: 6, 59: 7, 60: 8, 61: 9}


def get_2026_loading_slip_dir():
    """Return the 2026 Loading slip folder from paths.json, or None."""
    paths_file = REF_DATA_DIR / "paths.json"
    if not paths_file.exists():
        return None
    with open(paths_file, encoding="utf-8") as f:
        config = json.load(f)
    for path in config.get("LoadingSlipsPaths") or []:
        p = Path(path)
        if not p.is_absolute():
            p = BASE_DIR / p
        if p.exists() and "2026" in p.name and ("loading" in p.name.lower() or "slip" in p.name.lower()):
            return p
    return None


def main(dry_run=True):
    folder = get_2026_loading_slip_dir()
    if not folder:
        print("2026 Loading slip folder not found (check Reference_Data/paths.json LoadingSlipsPaths).")
        return 1
    print(f"Folder: {folder}")
    renames = []
    for f in sorted(folder.glob("*.xlsx")):
        name = f.name
        name_lower = name.lower()
        if "palletlines" in name_lower or "traceability" in name_lower or "copy" in name_lower:
            continue
        if "loading" not in name_lower and "slip" not in name_lower:
            continue
        # Parse week number from filename (first number after "week")
        parts = re.split(r"\bweek\b", name_lower, 1, flags=re.I)
        if len(parts) < 2:
            continue
        rest = parts[1].strip(" .")
        m = re.match(r"^(\d{1,2})", rest)
        if not m:
            continue
        week_num = int(m.group(1))
        if week_num not in ISO_TO_2026_CALENDAR:
            continue
        cal = ISO_TO_2026_CALENDAR[week_num]
        target_name = f"Week {cal} Loading Slip 2026.xlsx"
        if name == target_name:
            continue
        renames.append((f, target_name))
    if not renames:
        print("No files to rename (already correct or no Week 56–61 slips found).")
        return 0
    for path, new_name in renames:
        new_path = path.parent / new_name
        if new_path.exists() and new_path != path:
            print(f"  SKIP (target exists): {path.name} -> {new_name}")
            continue
        print(f"  {path.name}  ->  {new_name}")
        if not dry_run:
            path.rename(new_path)
    if dry_run and renames:
        print("\n[DRY RUN] Run with --apply to perform renames.")
    return 0


if __name__ == "__main__":
    import sys
    dry = "--apply" not in sys.argv
    if not dry:
        print("Applying renames...")
    sys.exit(main(dry_run=dry))
