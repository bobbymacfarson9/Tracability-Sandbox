"""
Fix 2026 loading slip filenames: slip 56 = week 4, not week 1.

Convention: Slip 53 = week 1, 54 = week 2, 55 = week 3, 56 = week 4, etc.
So the files that were originally Week 56-61 contain weeks 4-9 of 2026.

Current (wrong): Week 1-6 (from rename that assumed 56=week 1)
Target:          Week 4-9 (56=week 4, 57=week 5, 58=week 6, 59=week 7, 60=week 8, 61=week 9)

Re-rename in reverse order to avoid overwriting:
  Week 6 -> Week 9
  Week 5 -> Week 8
  Week 4 -> Week 7
  Week 3 -> Week 6
  Week 2 -> Week 5
  Week 1 -> Week 4

Run:
  python Scripts/fix_2026_slip_week_offset.py        # dry run
  python Scripts/fix_2026_slip_week_offset.py --apply   # perform renames
"""

import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
REF_DATA_DIR = BASE_DIR / "Reference_Data"

# Current filename -> target (add 3 to week number: 1->4, 2->5, etc.)
CURRENT_TO_TARGET = {
    1: 4,
    2: 5,
    3: 6,
    4: 7,
    5: 8,
    6: 9,
}


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
    print("Slip 56 = week 4 of 2026. Re-renaming Week 1->4, 2->5, 3->6, 4->7, 5->8, 6->9")
    print()

    renames = []
    for f in sorted(folder.glob("*.xlsx")):
        name = f.name
        name_lower = name.lower()
        if "palletlines" in name_lower or "traceability" in name_lower or "copy" in name_lower:
            continue
        if "loading" not in name_lower and "slip" not in name_lower:
            continue
        # Parse week number from "Week N Loading Slip 2026.xlsx"
        if not name_lower.startswith("week "):
            continue
        rest = name_lower[5:].strip()
        if not rest[0].isdigit():
            continue
        num_str = ""
        for c in rest:
            if c.isdigit():
                num_str += c
            else:
                break
        if not num_str:
            continue
        week_num = int(num_str)
        if week_num not in CURRENT_TO_TARGET:
            continue
        target_week = CURRENT_TO_TARGET[week_num]
        target_name = f"Week {target_week} Loading Slip 2026.xlsx"
        if name == target_name:
            continue
        renames.append((f, target_name))

    if not renames:
        print("No files to rename (already correct or no Week 1-6 slips found).")
        return 0

    # Sort in reverse order (6->9, 5->8, ...) so we don't overwrite
    renames.sort(key=lambda x: -int(x[0].name.split()[1]))

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
