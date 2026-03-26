"""
Export BB-back balancing breakdown tables from sandbox adjusted PalletLines logs.

Outputs:
  - Excel workbook with:
      1) Week_By_Adjustment_Category
      2) Week_To_Week_SKU_Back_Moves
      3) Raw_Back_Adjustments
  - CSV copies for easy filtering
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _load_adjustment_logs(balanced_dir: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(balanced_dir.glob("Week*_AllDays_PalletLines_Adjusted_*.xlsx")):
        try:
            df = pd.read_excel(path, sheet_name="Adjustments_Log")
        except Exception:
            continue
        if df is None or df.empty:
            continue
        df["Adjusted_File"] = path.name
        rows.append(df)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["Moved_CaseEquiv"] = pd.to_numeric(out.get("Moved_CaseEquiv"), errors="coerce").fillna(0.0)
    out["Moved_QtyBoxes"] = pd.to_numeric(out.get("Moved_QtyBoxes"), errors="coerce").fillna(0.0)
    out["Current_ReportWeek"] = pd.to_numeric(out.get("Current_ReportWeek"), errors="coerce").fillna(0).astype(int)
    out["Target_ReportWeek"] = pd.to_numeric(out.get("Target_ReportWeek"), errors="coerce").fillna(0).astype(int)
    out["Candidate_Type"] = out.get("Candidate_Type", "").astype(str)
    out["Adjustment_Action"] = out.get("Adjustment_Action", "").astype(str)
    out["SKU"] = out.get("SKU", "").astype(str)
    return out


def _category_name(candidate_type: str) -> str:
    ct = (candidate_type or "").strip().lower()
    if ct == "existing_bb_alignment_back":
        return "BBBack_ExistingAlignment"
    if ct == "nfld_bb_shift_back":
        return "BBBack_NFLDMinus7"
    if "back" in ct:
        return "BBBack_Other"
    return "Other"


def export_breakdown(sandbox_root: Path) -> Path:
    balanced_dir = sandbox_root / "Traceability_Exports" / "BalancedWorking"
    if not balanced_dir.exists():
        raise SystemExit(f"Missing {balanced_dir}")
    logs = _load_adjustment_logs(balanced_dir)
    if logs.empty:
        raise SystemExit("No Adjustments_Log sheets found in adjusted PalletLines workbooks.")

    # Keep only moves that are explicitly back-week logic.
    back = logs[logs["Candidate_Type"].str.contains("back", case=False, na=False)].copy()
    if back.empty:
        raise SystemExit("No back-week balancing rows found.")

    back["Adjustment_Category"] = back["Candidate_Type"].apply(_category_name)
    back["Week"] = back["Target_ReportWeek"]
    back["From_Week"] = back["Current_ReportWeek"]
    back["To_Week"] = back["Target_ReportWeek"]

    week_cat = (
        back.groupby(["Week", "Adjustment_Category"], as_index=False)
        .agg(
            Cases=("Moved_CaseEquiv", "sum"),
            Boxes=("Moved_QtyBoxes", "sum"),
            Move_Rows=("Moved_CaseEquiv", "size"),
        )
        .sort_values(["Week", "Adjustment_Category"], ascending=[True, True])
    )

    sku_week_pair = (
        back.groupby(["From_Week", "To_Week", "SKU", "Adjustment_Category"], as_index=False)
        .agg(
            QtyBoxes_Moved=("Moved_QtyBoxes", "sum"),
            CaseEquiv_Moved=("Moved_CaseEquiv", "sum"),
            Move_Rows=("Moved_CaseEquiv", "size"),
        )
        .sort_values(["From_Week", "To_Week", "SKU"], ascending=[True, True, True])
    )

    out_xlsx = balanced_dir / "BB_Back_Adjustment_Breakdown.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        week_cat.to_excel(writer, sheet_name="Week_By_Adjustment_Category", index=False)
        sku_week_pair.to_excel(writer, sheet_name="Week_To_Week_SKU_Back_Moves", index=False)
        back.to_excel(writer, sheet_name="Raw_Back_Adjustments", index=False)

    week_cat.to_csv(balanced_dir / "BB_Back_Week_By_Adjustment_Category.csv", index=False)
    sku_week_pair.to_csv(balanced_dir / "BB_Back_Week_To_Week_SKU.csv", index=False)
    return out_xlsx


def main() -> int:
    ap = argparse.ArgumentParser(description="Export BB-back week/category and SKU week->week breakdown.")
    ap.add_argument(
        "--sandbox",
        type=Path,
        default=Path("Sandbox_Traceability"),
        help="Sandbox root folder (default: Sandbox_Traceability)",
    )
    args = ap.parse_args()
    out = export_breakdown(args.sandbox.resolve())
    print(f"Wrote: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

