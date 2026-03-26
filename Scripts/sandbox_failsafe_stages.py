"""
Post–pallet-line stages for sandbox --go: production greedy fill, family take-home, failsafe rows.
Never touches nest run (no SQF input edits except traceability CSVs under Reference_Data).
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

EVIDENCE_PRODUCTION = "sandbox-go-production"
EVIDENCE_FAMILY = "sandbox-go-family"
EVIDENCE_FAILSAFE = "sandbox-go-residual authorized top-up"
FAILSAFE_NOTE_TAG = "auto-added sandbox go"

FAMILY_CATEGORY = "FamilyEmployeeTakehome"


def strip_sandbox_go_rows(ref_dir: Path) -> dict:
    """Remove prior sandbox-go rows from production adjustments, manual adjustments, failsafe CSV."""
    summary = {"production_rows_removed": 0, "manual_rows_removed": 0, "failsafe_rows_removed": 0}
    prod_csv = ref_dir / "Traceability_Production_Adjustments.csv"
    if prod_csv.exists():
        df = pd.read_csv(prod_csv)
        if not df.empty and "Evidence" in df.columns:
            mask = df["Evidence"].astype(str).str.strip() != EVIDENCE_PRODUCTION
            summary["production_rows_removed"] = int((~mask).sum())
            df = df.loc[mask].reset_index(drop=True)
            df.to_csv(prod_csv, index=False)
    adj_csv = ref_dir / "Traceability_Adjustments.csv"
    if adj_csv.exists():
        df = pd.read_csv(adj_csv)
        if not df.empty and "Evidence" in df.columns:
            mask = df["Evidence"].astype(str).str.strip() != EVIDENCE_FAMILY
            summary["manual_rows_removed"] = int((~mask).sum())
            df = df.loc[mask].reset_index(drop=True)
            df.to_csv(adj_csv, index=False)
    fail_csv = ref_dir / "Traceability_Failsafe_Evidence.csv"
    if fail_csv.exists():
        df = pd.read_csv(fail_csv)
        if not df.empty:
            ev = df.get("Evidence", pd.Series(dtype=str)).astype(str).str.strip()
            note = df.get("Note", pd.Series(dtype=str)).astype(str)
            mask = (ev != EVIDENCE_FAILSAFE) & (~note.str.contains(FAILSAFE_NOTE_TAG, case=False, na=False))
            summary["failsafe_rows_removed"] = int((~mask).sum())
            df = df.loc[mask].reset_index(drop=True)
            df.to_csv(fail_csv, index=False)
    return summary


def clear_balanced_working_adjusted(sandbox_root: Path) -> int:
    """Remove prior *_palletlines_adjusted_* workbooks so a new --go balance starts clean."""
    bw = sandbox_root / "Traceability_Exports" / "BalancedWorking"
    if not bw.is_dir():
        return 0
    n = 0
    for p in bw.glob("Week*_AllDays_PalletLines*.xlsx"):
        if "_palletlines_adjusted_" in p.name.lower():
            try:
                p.unlink()
                n += 1
            except OSError:
                pass
    return n


def snapshot_traceability_json(
    sandbox_root: Path,
    barn_day_max_abs: int | None = None,
    *,
    with_failsafe: bool = False,
) -> list[dict]:
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "sandbox_traceability_snapshot.py"),
        str(sandbox_root),
        str(PROJECT_ROOT),
    ]
    cmd.append(str(int(barn_day_max_abs)) if barn_day_max_abs is not None else "-")
    if with_failsafe:
        cmd.append("1")
    r = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr or "snapshot failed")
    return json.loads(r.stdout)


def _reload_hilly_acres(sandbox_root: Path):
    os.environ["EGGROOM_TRACEABILITY_SANDBOX"] = str(sandbox_root.resolve())
    os.environ["EGGROOM_TRACEABILITY_DATA_BASE"] = str(sandbox_root.resolve())
    import hilly_acres_production as ha

    importlib.reload(ha)
    return ha


def _aggregate_production_usage(prod_df: pd.DataFrame) -> dict[tuple, float]:
    usage: dict[tuple, float] = defaultdict(float)
    if prod_df.empty:
        return usage
    for _, row in prod_df.iterrows():
        try:
            w = int(float(row["Week"]))
            barn = int(float(row["Barn"]))
        except (TypeError, ValueError, KeyError):
            continue
        day = str(row.get("DayName") or "").strip()
        try:
            d = float(row["Delta_Stacks"])
        except (TypeError, ValueError, KeyError):
            continue
        usage[(w, day, barn)] += d
    return usage


def apply_production_greedy(
    sandbox_root: Path,
    snapshot: list[dict],
    *,
    max_abs_per_barn_day: int,
    audit: dict,
) -> int:
    """
    Write Traceability_Production_Adjustments rows to close Production vs Accounted gaps using
    negative deltas when under 100% and positive when over 100%, respecting aggregate |sum| per (week,day,barn).
    """
    ref = sandbox_root / "Reference_Data"
    prod_csv = ref / "Traceability_Production_Adjustments.csv"
    ha = _reload_hilly_acres(sandbox_root)

    if prod_csv.exists():
        df_exist = pd.read_csv(prod_csv)
    else:
        df_exist = pd.DataFrame(columns=["Week", "DayName", "Barn", "Delta_Stacks", "Evidence", "Note"])

    new_rows: list[dict] = []
    per_week_detail: list[dict] = []
    usage: dict[tuple, float] = defaultdict(float)
    usage.update(_aggregate_production_usage(df_exist))

    for row in snapshot:
        try:
            week = int(row["Week"])
        except (TypeError, ValueError, KeyError):
            continue
        prod = int(row.get("Production") or 0)
        acc = int(row.get("Accounted") or 0)
        if prod <= 0:
            continue
        gap = prod - acc  # >0 => reduce production; <0 => increase production
        if gap == 0:
            continue
        barn_stacks = ha.get_barn_stacks_for_week(week)
        if not barn_stacks:
            per_week_detail.append({"Week": week, "skipped": True, "reason": "no barn stacks"})
            continue
        barns = sorted(barn_stacks.keys())
        need = abs(int(gap))
        direction_neg = gap > 0
        moved = 0
        day_cycle = list(ha.DAY_NAMES)
        bi = 0
        di = 0
        safety = 0
        max_iter = need * max(len(barns), 1) * 8 + 100
        while need > 0 and safety < max_iter:
            safety += 1
            barn = barns[bi % len(barns)]
            day = day_cycle[di % len(day_cycle)]
            bi += 1
            di += 1
            key = (week, day, barn)
            cur = usage.get(key, 0.0)
            max_abs = float(max_abs_per_barn_day)
            if direction_neg:
                step = min(need, int(max(0.0, cur + max_abs)))
                if step <= 0:
                    continue
                delta = float(-step)
            else:
                step = min(need, int(max(0.0, max_abs - cur)))
                if step <= 0:
                    continue
                delta = float(step)
            new_rows.append({
                "Week": week,
                "DayName": day,
                "Barn": barn,
                "Delta_Stacks": delta,
                "Evidence": EVIDENCE_PRODUCTION,
                "Note": "sandbox go: barn/day guestimate slack",
            })
            usage[key] = cur + delta
            need -= step
            moved += step

        per_week_detail.append({
            "Week": week,
            "gap_cases_before": int(gap),
            "production_delta_cases_attempted": moved,
            "direction": "reduce_production" if gap > 0 else "increase_production",
        })

    if not new_rows:
        audit["production_greedy"] = {"rows_added": 0, "weeks": per_week_detail}
        return 0

    add_df = pd.DataFrame(new_rows)
    out = pd.concat([df_exist, add_df], ignore_index=True)
    out.to_csv(prod_csv, index=False)
    audit["production_greedy"] = {"rows_added": len(new_rows), "weeks": per_week_detail}
    return len(new_rows)


def apply_family_takehome(
    sandbox_root: Path,
    snapshot: list[dict],
    *,
    max_cases_per_week: int,
    tolerance_pct: float,
    audit: dict,
) -> int:
    """Add FamilyEmployeeTakehome manual rows up to max_cases_per_week where traceability is below ~100%."""
    if max_cases_per_week <= 0:
        audit["family_takehome"] = {"rows_added": 0}
        return 0
    ref = sandbox_root / "Reference_Data"
    adj_csv = ref / "Traceability_Adjustments.csv"
    cols = ["Week", "Category", "Cases", "Evidence", "Note"]
    if adj_csv.exists():
        df = pd.read_csv(adj_csv)
    else:
        df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    added = 0
    details = []
    tol = tolerance_pct / 100.0 if tolerance_pct else 0.0

    for row in snapshot:
        try:
            week = int(row["Week"])
        except (TypeError, ValueError, KeyError):
            continue
        prod = int(row.get("Production") or 0)
        acc = int(row.get("Accounted") or 0)
        if prod <= 0:
            continue
        ratio = acc / prod if prod else 0.0
        if ratio >= (1.0 - tol):
            continue
        gap = prod - acc
        if gap <= 0:
            continue
        take = min(int(gap), max_cases_per_week)
        if take <= 0:
            continue
        df.loc[len(df)] = {
            "Week": week,
            "Category": FAMILY_CATEGORY,
            "Cases": float(take),
            "Evidence": EVIDENCE_FAMILY,
            "Note": "sandbox go: family/employee graded eggs take-home (capped)",
        }
        added += 1
        details.append({"Week": week, "cases": take, "gap_before": int(gap)})

    if added:
        df = df.sort_values(["Week", "Category"]).reset_index(drop=True)
        df.to_csv(adj_csv, index=False)
    audit["family_takehome"] = {"rows_added": added, "weeks": details}
    return added


def ensure_failsafe_rows(
    sandbox_root: Path,
    snapshot: list[dict],
    *,
    tolerance_pct: float,
    skip_if_gap_le: int = 0,
    audit: dict,
) -> int:
    """Append Traceability_Failsafe_Evidence rows for weeks still below 100% (after prior stages)."""
    ref = sandbox_root / "Reference_Data"
    fail_csv = ref / "Traceability_Failsafe_Evidence.csv"
    cols = ["Week", "Evidence", "Evidence_File", "Case_Cap", "Note"]
    if fail_csv.exists():
        df = pd.read_csv(fail_csv)
    else:
        df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    tol = tolerance_pct / 100.0 if tolerance_pct else 0.0
    added = 0
    details = []

    for row in snapshot:
        try:
            week = int(row["Week"])
        except (TypeError, ValueError, KeyError):
            continue
        prod = int(row.get("Production") or 0)
        acc = int(row.get("Accounted") or 0)
        if prod <= 0:
            continue
        ratio = acc / prod if prod else 0.0
        if ratio >= (1.0 - tol):
            continue
        gap = prod - acc
        if gap <= 0:
            continue
        if gap <= skip_if_gap_le:
            continue
        if not df.empty and "Week" in df.columns:
            wmatch = pd.to_numeric(df["Week"], errors="coerce") == week
            if wmatch.any():
                continue
        df.loc[len(df)] = {
            "Week": week,
            "Evidence": EVIDENCE_FAILSAFE,
            "Evidence_File": "",
            "Case_Cap": "",
            "Note": f"{FAILSAFE_NOTE_TAG}; residual gap to 100%",
        }
        added += 1
        details.append({"Week": week, "gap_cases": int(prod - acc)})

    if added:
        df = df.sort_values(["Week"]).reset_index(drop=True)
        df.to_csv(fail_csv, index=False)
    audit["failsafe_evidence"] = {"rows_appended": added, "weeks": details}
    return added
