"""
Analyze Week_42 mapping to compare Wednesday and Thursday vs Monday.
Run from repo root: python Scripts/analyze_mapping_wed_thu.py
Output: difference in Day rows, stop names, and cell addresses so you can fix mapping or slip.
"""

import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
REF_DATA_DIR = BASE_DIR / "Reference_Data"
MAPPING_FILE = REF_DATA_DIR / "Week_42_Stop_SKU_Final_POLISHED.xlsx"


def main():
    if not MAPPING_FILE.exists():
        print(f"Mapping file not found: {MAPPING_FILE}")
        return
    df = pd.read_excel(MAPPING_FILE, sheet_name=0)
    print("=" * 70)
    print("WEEK_42 MAPPING ANALYSIS — Wednesday / Thursday vs Monday")
    print("=" * 70)

    # Find Day column (first column with "DAY" in name)
    day_col = None
    stop_col = None
    qty_col = None
    sku_col = None
    for c in df.columns:
        cu = str(c).upper().replace(" ", "")
        if "DAY" in cu and day_col is None:
            day_col = c
        if ("STOP" in cu and "NAME" in cu) or cu == "STOP":
            stop_col = c
        if qty_col is None and ("QTYCELL" in cu or "QUANTITYCELL" in cu or "QTYCELLADDR" in cu):
            qty_col = c
        if sku_col is None and ("SKUCELL" in cu or "SKUCELLADDR" in cu):
            sku_col = c

    if not day_col or not stop_col:
        print("Could not find Day or Stop column. Columns:", list(df.columns))
        return

    df["_day_norm"] = df[day_col].astype(str).str.strip().str.upper().str[:3]

    # Unique days and counts
    print("\n1. DAY VALUES IN MAPPING (normalized to first 3 chars for matching)")
    print("-" * 70)
    day_counts = df.groupby("_day_norm").size()
    for d in ["MON", "TUE", "WED", "THU", "FRI", "NFL"]:
        count = day_counts.get(d, 0)
        raw_vals = df.loc[df["_day_norm"] == d, day_col].dropna().unique() if d in day_counts.index else []
        print(f"   {d} (matches 'Wednesday' for WED, 'Thursday' for THU): {count} rows   raw values: {list(raw_vals)[:5]}")
    other = [x for x in day_counts.index if x not in ["MON", "TUE", "WED", "THU", "FRI", "NFL"]]
    if other:
        print(f"   Other day codes in file: {other}")

    # Compare Mon vs Wed vs Thu: row counts and sample stops/cells
    print("\n2. ROW COUNTS BY DAY (Mon / Wed / Thu)")
    print("-" * 70)
    for day_name, code in [("Monday", "MON"), ("Wednesday", "WED"), ("Thursday", "THU")]:
        sub = df[df["_day_norm"] == code]
        print(f"   {day_name} ({code}): {len(sub)} rows")
        if len(sub) > 0 and stop_col and qty_col and sku_col:
            if qty_col in df.columns and sku_col in df.columns:
                sample = sub[[stop_col, qty_col, sku_col]].head(3)
                print(f"      Sample (Stop, QtyCell, SKUCell):")
                for _, r in sample.iterrows():
                    print(f"        {r[stop_col]}  ->  Qty: {r[qty_col]}  SKU: {r[sku_col]}")

    # Stops that appear in Monday but not in Wednesday or Thursday
    print("\n3. STOP OVERLAP (stops in Monday vs Wednesday vs Thursday)")
    print("-" * 70)
    mon_stops = set(df.loc[df["_day_norm"] == "MON", stop_col].dropna().astype(str).str.strip())
    wed_stops = set(df.loc[df["_day_norm"] == "WED", stop_col].dropna().astype(str).str.strip())
    thu_stops = set(df.loc[df["_day_norm"] == "THU", stop_col].dropna().astype(str).str.strip())
    print(f"   Monday:    {len(mon_stops)} unique stops")
    print(f"   Wednesday: {len(wed_stops)} unique stops")
    print(f"   Thursday:  {len(thu_stops)} unique stops")
    in_mon_not_wed = mon_stops - wed_stops
    in_mon_not_thu = mon_stops - thu_stops
    if in_mon_not_wed:
        print(f"   Stops in Monday but NOT in Wednesday ({len(in_mon_not_wed)}): {sorted(in_mon_not_wed)[:8]}{'...' if len(in_mon_not_wed) > 8 else ''}")
    else:
        print("   All Monday stops appear in Wednesday.")
    if in_mon_not_thu:
        print(f"   Stops in Monday but NOT in Thursday ({len(in_mon_not_thu)}): {sorted(in_mon_not_thu)[:8]}{'...' if len(in_mon_not_thu) > 8 else ''}")
    else:
        print("   All Monday stops appear in Thursday.")

    # Cell address pattern: are Wed/Thu using same sheet layout (same row numbers)?
    print("\n4. CELL ADDRESS PATTERN (first 5 rows per day)")
    print("-" * 70)
    if qty_col and sku_col and qty_col in df.columns and sku_col in df.columns:
        for day_name, code in [("Monday", "MON"), ("Wednesday", "WED"), ("Thursday", "THU")]:
            sub = df[df["_day_norm"] == code].head(5)
            cells = list(sub[qty_col].dropna().astype(str)) if qty_col in sub.columns else []
            print(f"   {day_name}: {cells}")
    print("\n" + "=" * 70)
    print("If Wednesday or Thursday have 0 rows in (1), add rows with Day = Wed/Thursday in the mapping.")
    print("If row counts are low or cell refs look wrong, the mapping may have been built from a different slip layout for Wed/Thu.")
    print("=" * 70)


if __name__ == "__main__":
    main()
