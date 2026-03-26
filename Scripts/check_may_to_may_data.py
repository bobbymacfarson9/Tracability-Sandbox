"""
Report what data exists vs missing for May-to-May traceability (last year).
Run from repo root: python Scripts/check_may_to_may_data.py

Checks:
- SQF_Traceability_Inputs.csv: which weeks have a row
- Traceability_Exports: which weeks have a PalletLines file
- Reference_Data: paths.json (HillyAcresPath, GradeOutsPath), mapping files, loading slip files
- Summarizes gaps for May 2024 -> May 2025 (weeks 18-52 of 2024, 1-22 of 2025)
"""

import json
import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
REF_DATA_DIR = BASE_DIR / "Reference_Data"
EXPORTS_DIR = BASE_DIR / "Traceability_Exports"
INPUT_CSV = REF_DATA_DIR / "SQF_Traceability_Inputs.csv"

# May-to-May: approximate week range (ISO). May 1 ~ week 18, May 31 ~ week 22.
# "Last year May to May" = 2024 W18-W52 + 2025 W1-W22 (we use simple week numbers 1-52)
WEEKS_YEAR1 = list(range(18, 53))   # 18..52
WEEKS_YEAR2 = list(range(1, 23))   # 1..22
MAY_TO_MAY_WEEKS = set(WEEKS_YEAR1 + WEEKS_YEAR2)


def main():
    print("=" * 70)
    print("MAY-TO-MAY TRACEABILITY — DATA CHECK")
    print("(May 2024 to May 2025 = weeks 18-52 and 1-22)")
    print("=" * 70)

    # 1. Paths
    paths_file = REF_DATA_DIR / "paths.json"
    hilly_path = ""
    grade_path = ""
    hilly_paths = False
    if paths_file.exists():
        try:
            with open(paths_file, encoding="utf-8") as f:
                cfg = json.load(f)
            hilly_path = (cfg.get("HillyAcresPath") or "").strip()
            grade_path = (cfg.get("GradeOutsPath") or "").strip()
            hilly_paths = isinstance(cfg.get("HillyAcresPaths"), list) and len(cfg.get("HillyAcresPaths", [])) > 0
        except Exception:
            pass
    print("\n1. PATHS (Reference_Data/paths.json)")
    print("   HillyAcresPath:  ", "SET" if hilly_path else ("(single path not set)" if hilly_paths else "MISSING (Production will be 0 unless filled in CSV)"))
    if hilly_paths and not hilly_path:
        print("   HillyAcresPaths: SET (2024/2025/2026 Reports) - Production will be read from these folders")
    print("   GradeOutsPath:   ", "SET" if grade_path else "MISSING (OffGrades from CSV or 1.2% default)")

    # 2. CSV weeks
    csv_weeks = set()
    if INPUT_CSV.exists():
        try:
            df = pd.read_csv(INPUT_CSV)
            if "Week" in df.columns:
                for w in df["Week"].dropna():
                    try:
                        csv_weeks.add(int(w))
                    except (ValueError, TypeError):
                        pass
        except Exception as e:
            print("   Error reading CSV:", e)
    print("\n2. SQF_Traceability_Inputs.csv")
    print("   Weeks with a row:", len(csv_weeks), "—", sorted(csv_weeks)[:20], "..." if len(csv_weeks) > 20 else "")
    in_range = csv_weeks & MAY_TO_MAY_WEEKS
    missing_csv = MAY_TO_MAY_WEEKS - csv_weeks
    print("   Weeks in May-May range that have a row:", len(in_range))
    if missing_csv:
        print("   Weeks in May-May range MISSING a row:", sorted(missing_csv))

    # 3. PalletLines files
    pallet_weeks = set()
    if EXPORTS_DIR.exists():
        for f in EXPORTS_DIR.glob("Week*_AllDays_PalletLines_*.xlsx"):
            name = f.stem  # e.g. Week44_AllDays_PalletLines_20250220_131306
            parts = name.replace("Week", " ").split("_")
            if parts and parts[0].strip().isdigit():
                pallet_weeks.add(int(parts[0].strip()))
    print("\n3. Traceability_Exports (PalletLines)")
    print("   Weeks with a PalletLines file:", len(pallet_weeks), "—", sorted(pallet_weeks)[:20], "..." if len(pallet_weeks) > 20 else "")
    missing_pallet = MAY_TO_MAY_WEEKS - pallet_weeks
    if missing_pallet and len(MAY_TO_MAY_WEEKS) > 0:
        print("   Weeks in May-May range MISSING PalletLines (run processor):", sorted(missing_pallet)[:25], "..." if len(missing_pallet) > 25 else "")

    # 4. Mapping files
    w42 = (REF_DATA_DIR / "Week_42_Stop_SKU_Final_POLISHED.xlsx").exists()
    w60 = (REF_DATA_DIR / "Week_60_Stop_SKU_Final_POLISHED.xlsx").exists()
    w59 = (REF_DATA_DIR / "Week_59_Stop_SKU_Final_POLISHED.xlsx").exists()
    print("\n4. Reference_Data — Mapping")
    print("   Week_42_Stop_SKU_Final_POLISHED.xlsx (weeks < 59):", "YES" if w42 else "MISSING")
    print("   Week_60_Stop_SKU_Final_POLISHED.xlsx (weeks 59+): ", "YES" if w60 else "MISSING")
    if w59:
        print("   Week_59_Stop_SKU_Final_POLISHED.xlsx:             YES")

    # 5. Loading slips (Reference_Data + paths.json LoadingSlipsPaths e.g. 2024 Reports)
    slip_weeks = set()
    slip_dirs = [REF_DATA_DIR]
    try:
        with open(REF_DATA_DIR / "paths.json", encoding="utf-8") as f:
            cfg = json.load(f)
        for p in cfg.get("LoadingSlipsPaths") or []:
            path = Path(p)
            if not path.is_absolute():
                path = BASE_DIR / path
            if path.exists() and path.is_dir():
                slip_dirs.append(path)
    except Exception:
        pass
    for data_dir in slip_dirs:
        if not data_dir.exists():
            continue
        for f in data_dir.rglob("*.xlsx"):
            name = f.name.lower()
            if "loading" not in name and "slip" not in name:
                continue
            if "palletlines" in name or "mapping" in name or "polished" in name or "traceability" in name or "template" in name or "copy of" in name:
                continue
            rest = name.replace("week", " ", 1)
            parts = rest.split()
            if parts and parts[0].strip().replace(".", "").isdigit():
                slip_weeks.add(int(parts[0].strip().replace(".", "")))
    print("\n5. Loading slip workbooks (Reference_Data + 2024 Reports etc.)")
    print("   Weeks with a loading slip file:", len(slip_weeks), "—", sorted(slip_weeks)[:20], "..." if len(slip_weeks) > 20 else "")
    missing_slips = MAY_TO_MAY_WEEKS - slip_weeks
    if missing_slips:
        print("   Weeks in May-May range MISSING a loading slip:", sorted(missing_slips)[:25], "..." if len(missing_slips) > 25 else "")

    # 6. Summary
    print("\n" + "=" * 70)
    print("SUMMARY - To finish May-to-May you need:")
    if not hilly_path:
        print("   • Set HillyAcresPath in paths.json OR fill Production in CSV for each week")
    if not grade_path:
        print("   • (Optional) Set GradeOutsPath for grade-out CSV, or use 1.2% default")
    if missing_csv:
        print("   • Add CSV rows for weeks:", sorted(missing_csv)[:15], "..." if len(missing_csv) > 15 else "")
    if missing_slips:
        print("   • Add loading slip(s) for weeks missing slips (see list above)")
    if missing_pallet:
        print("   • Run process_weekly_loading_slip.py for each week that has a slip (or run_all_weeks_palletlines.py)")
    print("   • Then run: python Scripts/sqf_traceability.py --slip-week")
    print("=" * 70)


if __name__ == "__main__":
    main()
