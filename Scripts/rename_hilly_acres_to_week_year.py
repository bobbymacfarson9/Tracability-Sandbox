"""
Rename Hilly Acres workbooks to: Week N YEAR_ Hilly Acres Farm Ltd.xlsx
so the traceability code picks them up consistently.

Reads week number from Inputs!C2 (row 1, col 2). Year from Inputs!C1 (row 0) or folder name (2025/2026).

Usage:
  python Scripts/rename_hilly_acres_to_week_year.py              # dry run (print only)
  python Scripts/rename_hilly_acres_to_week_year.py --do-it      # actually rename
"""

import argparse
import json
import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
REF_DATA_DIR = BASE_DIR / "Reference_Data"
INPUTS_SHEET = "Inputs"


def get_folders():
    """Folders from paths.json HillyAcresPaths, or default under Hilly Acres Slips For Barn Production."""
    paths_file = REF_DATA_DIR / "paths.json"
    if paths_file.exists():
        try:
            with open(paths_file, encoding="utf-8") as f:
                config = json.load(f)
            for path in config.get("HillyAcresPaths") or []:
                p = Path(path)
                if not p.is_absolute():
                    p = BASE_DIR / p
                if p.exists():
                    yield p
        except Exception:
            pass
    default = BASE_DIR / "Hilly Acres Slips For Barn Production"
    if default.exists():
        for sub in ["2025 Reports EF", "2026 EFC Reports"]:
            p = default / sub
            if p.exists():
                yield p


def get_week_and_year(file_path):
    """Return (week_int, year_int) from Inputs sheet, or (None, None)."""
    try:
        df = pd.read_excel(file_path, sheet_name=INPUTS_SHEET, header=None)
        if df.shape[0] < 2 or df.shape[1] < 3:
            return None, None
        week_val = df.iloc[1, 2]
        year_val = df.iloc[0, 2] if df.shape[1] > 2 else None
        if week_val is None or str(week_val).strip() == "":
            return None, None
        week_str = str(week_val).strip().replace(".", "")
        if not week_str.isdigit():
            return None, None
        week = int(week_str)
        year = None
        if year_val is not None:
            ystr = str(year_val).strip()
            if len(ystr) >= 4 and ystr[:4].isdigit():
                year = int(ystr[:4])
        if year is None and "2026" in str(file_path):
            year = 2026
        if year is None and "2025" in str(file_path):
            year = 2025
        if year is None:
            year = 2025
        return week, year
    except Exception:
        return None, None


def target_name(week: int, year: int) -> str:
    return f"Week {week} {year}_ Hilly Acres Farm Ltd.xlsx"


def main():
    ap = argparse.ArgumentParser(description="Rename Hilly Acres workbooks to Week N YEAR_ Hilly Acres Farm Ltd.xlsx")
    ap.add_argument("--do-it", action="store_true", help="Actually rename files (default: dry run)")
    args = ap.parse_args()
    do_it = args.do_it
    if not do_it:
        print("DRY RUN (use --do-it to rename)\n")
    folders = list(get_folders())
    if not folders:
        print("No Hilly Acres folders found in paths.json or Hilly Acres Slips For Barn Production.")
        return 1
    renamed = 0
    skipped = 0
    for folder in folders:
        print(f"Folder: {folder}")
        for f in sorted(folder.glob("*.xlsx")):
            if "copy of" in f.name.lower() or f.name.lower().startswith("copy "):
                skipped += 1
                continue
            week, year = get_week_and_year(f)
            if week is None:
                print(f"  SKIP (no week in Inputs): {f.name}")
                skipped += 1
                continue
            new_name = target_name(week, year)
            if f.name == new_name:
                print(f"  OK (already named): {f.name}")
                continue
            new_path = f.parent / new_name
            if new_path.exists() and new_path != f:
                print(f"  SKIP (target exists): {f.name} -> {new_name}")
                skipped += 1
                continue
            print(f"  RENAME: {f.name}")
            print(f"       -> {new_name}")
            if do_it:
                f.rename(new_path)
                renamed += 1
        print()
    if do_it:
        print(f"Renamed {renamed} file(s).")
    else:
        print("No files renamed (dry run). Run with --do-it to apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)
