import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
EXPORTS_DIR = BASE_DIR / "Traceability_Exports"

WEEKS = [42, 45]

TROUBLE_EXACT = {
    "Lrg 30 pack Nova",
    "XLrg 30 pack Nova",
    "Lob 30",
    "Lob 30 Lg",
    "OC 30 Lrg",
    "OC 30 Lg",
}

TROUBLE_EXTRA = {"ED 18 XL", "ED 18 LG", "Nova Jumbo", "ED Jumbo"}


def is_trouble_sku(s: str) -> bool:
    if not isinstance(s, str):
        return False
    ss = s.strip()
    if ss in TROUBLE_EXACT or ss in TROUBLE_EXTRA:
        return True
    low = ss.lower()
    if "30" in low and ("nova" in low or "lob" in low or low.startswith("oc 30")):
        return True
    return False


def main() -> None:
    for week in WEEKS:
        pattern = f"Week{week}_AllDays_PalletLines*.xlsx"
        files = sorted(EXPORTS_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            print(f"Week {week}: no PalletLines file found for pattern {pattern}")
            continue
        path = files[0]
        print(f"\n=== Week {week} — PalletLines file: {path.name} ===")
        df = pd.read_excel(path, sheet_name="PalletLines")
        if "DayName" not in df.columns and "Day" in df.columns:
            df["DayName"] = df["Day"]
        if "QtyBoxes" not in df.columns or "DayName" not in df.columns or "SKU" not in df.columns:
            print("  Missing expected columns in PalletLines sheet")
            continue
        df = df[["DayName", "SKU", "QtyBoxes"]].copy()
        df["DayName"] = df["DayName"].astype(str).str.strip()
        df["SKU"] = df["SKU"].astype(str).str.strip()
        df["QtyBoxes"] = pd.to_numeric(df["QtyBoxes"], errors="coerce").fillna(0)
        df_trouble = df[df["SKU"].apply(is_trouble_sku)].copy()
        if df_trouble.empty:
            print("  No trouble SKUs found in PalletLines for this week.")
            continue
        grouped = df_trouble.groupby(["DayName", "SKU"])["QtyBoxes"].sum().reset_index()
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "NFLD", "Saturday", "Sunday"]
        grouped["DayOrder"] = grouped["DayName"].apply(lambda d: day_order.index(d) if d in day_order else 99)
        grouped = grouped.sort_values(["DayOrder", "DayName", "SKU"])
        print("\nDay-by-day totals for trouble SKUs (QtyBoxes):")
        for day in grouped["DayName"].unique():
            sub = grouped[grouped["DayName"] == day]
            print(f"\n  {day}:")
            for _, r in sub.iterrows():
                print(f"    {r['SKU']}: {int(r['QtyBoxes'])} boxes")


if __name__ == "__main__":
    main()

