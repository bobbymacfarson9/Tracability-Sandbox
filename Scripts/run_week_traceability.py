"""
Run the full traceability pipeline for one week (newer weeks 59+):
  1. Process the loading slip → PalletLines (process_weekly_loading_slip.py --week N)
  2. Run traceability for that week (sqf_traceability.py --slip-week --week N)

Usage (from repo root):
  python Scripts/run_week_traceability.py 59
  python Scripts/run_week_traceability.py 60
"""

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent


def main():
    if len(sys.argv) < 2:
        print("Usage: python Scripts/run_week_traceability.py <week_number>")
        print("Example: python Scripts/run_week_traceability.py 59")
        return 1
    week = sys.argv[1].strip()
    if not week.isdigit():
        print(f"Invalid week: {week}")
        return 1
    py = sys.executable
    # 1. Build PalletLines for this week
    print(f"\n{'='*60}")
    print(f"Step 1: Processing loading slip for Week {week}")
    print("="*60)
    r1 = subprocess.run(
        [py, str(SCRIPT_DIR / "process_weekly_loading_slip.py"), "--week", week],
        cwd=str(BASE_DIR),
    )
    if r1.returncode != 0:
        print(f"\nProcessor failed for week {week}. Fix errors and re-run.")
        return r1.returncode
    # 2. Run traceability for this week
    print(f"\n{'='*60}")
    print(f"Step 2: Traceability report for Week {week}")
    print("="*60)
    r2 = subprocess.run(
        [py, str(SCRIPT_DIR / "sqf_traceability.py"), "--slip-week", "--week", week],
        cwd=str(BASE_DIR),
    )
    return r2.returncode


if __name__ == "__main__":
    sys.exit(main() or 0)
