"""
Run PalletLines processing for every week that has loading slip(s) in Reference_Data and paths.json LoadingSlipsPaths (e.g. 2024 Reports).
Older weeks use Week_42 mapping (no OD). Weeks 59/60 use Week_60 mapping.
Output: weekly consolidated report + master inventory in Traceability_Exports (no daily files).
"""

import json
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
REF_DATA_DIR = BASE_DIR / "Reference_Data"


def _get_loading_slip_dirs():
    """Same as process_weekly_loading_slip: Reference_Data + LoadingSlipsPaths from paths.json."""
    dirs = [REF_DATA_DIR]
    try:
        paths_file = REF_DATA_DIR / "paths.json"
        if paths_file.exists():
            with open(paths_file, encoding="utf-8") as f:
                config = json.load(f)
            for path in config.get("LoadingSlipsPaths") or []:
                p = Path(path)
                if not p.is_absolute():
                    p = BASE_DIR / p
                if p.exists() and p.is_dir() and p not in dirs:
                    dirs.append(p)
    except Exception:
        pass
    return dirs


def _get_2026_calendar_to_iso():
    """Load 2026 calendar week (1-6) -> ISO week (56-61) from paths.json."""
    try:
        paths_file = REF_DATA_DIR / "paths.json"
        if paths_file.exists():
            with open(paths_file, encoding="utf-8") as f:
                config = json.load(f)
            m = config.get("LoadingSlip2026CalendarToISO") or {}
            return {int(k): int(v) for k, v in m.items()}
    except Exception:
        pass
    return {}


def _is_2026_loading_slip_folder(path):
    """True if this path is the 2026 loading slip folder."""
    name = path.name if hasattr(path, "name") else str(path)
    return "2026" in name and ("loading" in name.lower() or "slip" in name.lower())


def find_week_numbers_and_files():
    """Find all (week_number, file_path) for loading slip files. One file per week preferred (multi-sheet).
    In the 2026 Loading slip folder, files named 'Week 1 Loading Slip 2026.xlsx' etc. map to ISO weeks 56-61.
    """
    exclude = {'palletlines', 'traceability', 'master', 'inventory', 'pallet', 'sku_final', 'mapping', 'polished'}
    cal_to_iso_2026 = _get_2026_calendar_to_iso()
    week_to_file = {}
    for data_dir in _get_loading_slip_dirs():
        for path in data_dir.rglob("*.xlsx"):
            name_lower = path.name.lower()
            if any(x in name_lower for x in exclude):
                continue
            if 'copy of' in name_lower or name_lower.startswith('copy '):
                continue
            if 'loading' not in name_lower and 'slip' not in name_lower:
                continue
            # Parse week number: "Week 35 Loading Slip 2025.xlsx" or "Week 2 Loading Slip 2026.xlsx"
            rest = name_lower.replace("week", " ", 1)
            parts = rest.split()
            if not parts:
                continue
            num = parts[0].strip(" .")
            if not num.isdigit():
                continue
            week_num = int(num)
            # 2026 folder with calendar-week naming: Week 1-6 2026 -> ISO 56-61
            if _is_2026_loading_slip_folder(path.parent) and "2026" in name_lower and week_num in cal_to_iso_2026:
                week_num = cal_to_iso_2026[week_num]
            # Prefer a file that has no day name (multi-sheet = one file for whole week)
            has_day = any(d in name_lower for d in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'mon', 'tues', 'wed', 'thurs', 'fri'])
            if week_num not in week_to_file or (not has_day and 'day' in str(week_to_file[week_num]).lower()):
                week_to_file[week_num] = path
            elif not has_day:
                week_to_file[week_num] = path  # prefer no-day (multi-sheet)
    return sorted(week_to_file.items())


def main():
    items = find_week_numbers_and_files()
    if not items:
        print("No loading slip files found in Reference_Data.")
        return 1
    print(f"Found {len(items)} weeks to process: {[w for w, _ in items]}")
    for week_num, file_path in items:
        try:
            rel_str = str(file_path.relative_to(BASE_DIR))
        except Exception:
            rel_str = str(file_path)
        rel_str = rel_str.replace("\\", "/")
        print(f"\n--- Week {week_num}: {file_path.name} ---")
        cmd = [
            sys.executable,
            str(SCRIPT_DIR / "process_weekly_loading_slip.py"),
            "--week", str(week_num),
            "--file", rel_str,
        ]
        ret = subprocess.run(cmd, cwd=str(BASE_DIR))
        if ret.returncode != 0:
            print(f"  [FAILED] Week {week_num}")
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
