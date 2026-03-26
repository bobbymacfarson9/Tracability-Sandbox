"""
Sandbox traceability pipeline (production vs accounted).

Policy order (matches sqf_traceability behavior):
  1) BB date shift back one week only — run with --balance-palletlines --balance-palletlines-direction back
     (balancing plan already ignores manual adjustments).
  2) Non–order-slip bucket — cap NonSlipShipment in sandbox Reference_Data at 20 cases/week before the run.
  3) Production tweaks — edit Sandbox_Traceability/Reference_Data/Traceability_Production_Adjustments.csv
     (±5 stacks per barn per day enforced in hilly_acres_production), then re-export with --balanced-traceability-csv.

Original project files are not modified. Set EGGROOM_TRACEABILITY_SANDBOX + EGGROOM_TRACEABILITY_DATA_BASE
to the sandbox folder so reference/exports and relative data paths (2024 Reports, grade outs, Hilly Acres folder)
resolve inside sandbox.

Usage (from project root):
  python Scripts/sandbox_traceability_pipeline.py --init
  python Scripts/sandbox_traceability_pipeline.py --run
  python Scripts/sandbox_traceability_pipeline.py --run --failsafe-to-100
  python Scripts/sandbox_traceability_pipeline.py --go

  # After editing production adjustments in the sandbox copy:
  python Scripts/sandbox_traceability_pipeline.py --reexport-balanced
  python Scripts/sandbox_traceability_pipeline.py --reexport-balanced --failsafe-to-100
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd

import sandbox_failsafe_stages as sfs

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_SANDBOX = PROJECT_ROOT / "Sandbox_Traceability"


def _is_adjusted_palletlines_filename(name: str) -> bool:
    return "_palletlines_adjusted_" in str(name).lower()


def _slip_week_from_palletlines_name(path: Path) -> int | None:
    m = re.match(r"Week(\d+)_", path.name, re.I)
    if not m:
        return None
    w = int(m.group(1))
    return w if w >= 1 else None


def sync_main_exports_to_sandbox(sandbox_root: Path) -> int:
    """
    Mirror source-of-truth PalletLines from main project Traceability_Exports into sandbox.

    For each slip week, picks the newest non-adjusted Week*_AllDays_PalletLines*.xlsx among:
      - Traceability_Exports/ (root)
      - Traceability_Exports/Original/

    Writes that file to both sandbox Traceability_Exports/ and Traceability_Exports/Original/.
    Stale non-adjusted PalletLines copies in those two folders are removed first.
    """
    main_exp = PROJECT_ROOT / "Traceability_Exports"
    if not main_exp.is_dir():
        raise SystemExit(f"Missing main exports folder: {main_exp}")

    dest_root = sandbox_root / "Traceability_Exports"
    dest_orig = dest_root / "Original"
    dest_root.mkdir(parents=True, exist_ok=True)
    dest_orig.mkdir(parents=True, exist_ok=True)

    best: dict[int, Path] = {}
    for folder in (main_exp, main_exp / "Original"):
        if not folder.is_dir():
            continue
        for p in folder.glob("Week*_AllDays_PalletLines*.xlsx"):
            if _is_adjusted_palletlines_filename(p.name):
                continue
            w = _slip_week_from_palletlines_name(p)
            if w is None:
                continue
            prev = best.get(w)
            if prev is None or p.stat().st_mtime > prev.stat().st_mtime:
                best[w] = p

    if not best:
        raise SystemExit(
            f"No Week*_AllDays_PalletLines*.xlsx found under {main_exp} (root or Original)."
        )

    for d in (dest_root, dest_orig):
        for p in d.glob("Week*_AllDays_PalletLines*.xlsx"):
            if _is_adjusted_palletlines_filename(p.name):
                continue
            try:
                p.unlink()
            except OSError:
                pass

    for w in sorted(best):
        src = best[w]
        shutil.copy2(src, dest_root / src.name)
        shutil.copy2(src, dest_orig / src.name)

    print(f"Synced {len(best)} PalletLines week files from {main_exp} -> {dest_root} and {dest_orig}")
    return len(best)


def _copy_tree_or_file(src: Path, dst: Path) -> None:
    if src.is_dir():
        shutil.copytree(src, dst, dirs_exist_ok=True)
    else:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def _rewrite_paths_json_for_sandbox(ref_dir: Path) -> None:
    path = ref_dir / "paths.json"
    if not path.exists():
        return
    with path.open(encoding="utf-8") as f:
        cfg = json.load(f)
    # Keep these relative so scripts resolve them against EGGROOM_TRACEABILITY_DATA_BASE=sandbox_root.
    cfg["HillyAcresPath"] = ""
    cfg["GradeOutsPath"] = "grade outs"
    with path.open("w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def init_sandbox(sandbox_root: Path, force: bool) -> None:
    """Copy Reference_Data, exports, and data-source folders into sandbox."""
    if sandbox_root.exists() and force:
        shutil.rmtree(sandbox_root)
    sandbox_root.mkdir(parents=True, exist_ok=True)

    ref_src = PROJECT_ROOT / "Reference_Data"
    ref_dst = sandbox_root / "Reference_Data"
    if not ref_src.is_dir():
        raise SystemExit(f"Missing {ref_src}")
    if ref_dst.exists() and not force:
        print(f"Skip copy: {ref_dst} exists (use --force to replace)")
    else:
        if ref_dst.exists():
            shutil.rmtree(ref_dst)
        shutil.copytree(ref_src, ref_dst)
    _rewrite_paths_json_for_sandbox(ref_dst)

    orig_src = PROJECT_ROOT / "Traceability_Exports" / "Original"
    orig_dst = sandbox_root / "Traceability_Exports" / "Original"
    if not orig_src.is_dir():
        raise SystemExit(f"Missing {orig_src}")
    orig_dst.mkdir(parents=True, exist_ok=True)
    for p in orig_src.glob("Week*_AllDays_PalletLines*.xlsx"):
        shutil.copy2(p, orig_dst / p.name)
    (sandbox_root / "Traceability_Exports" / "BalancedWorking").mkdir(parents=True, exist_ok=True)

    # Copy source data directories so sandbox can resolve paths.json internally.
    for folder_name in ("2024 Reports", "grade outs", "Hilly Acres Slips For Barn Production"):
        src = PROJECT_ROOT / folder_name
        if src.exists():
            _copy_tree_or_file(src, sandbox_root / folder_name)

    print(f"Sandbox ready: {sandbox_root}")


def cap_adjustment_category(adjustments_csv: Path, category: str, max_cases: int) -> int:
    """Cap one adjustments category Cases per row to max_cases. Returns number of rows changed."""
    if not adjustments_csv.exists():
        return 0
    df = pd.read_csv(adjustments_csv)
    if df.empty or "Category" not in df.columns or "Cases" not in df.columns:
        return 0
    changed = 0
    for i in df.index:
        if str(df.at[i, "Category"]).strip() != category:
            continue
        try:
            v = float(df.at[i, "Cases"])
        except (TypeError, ValueError):
            continue
        if v > max_cases:
            df.at[i, "Cases"] = max_cases
            changed += 1
    df.to_csv(adjustments_csv, index=False)
    return changed


def apply_weekly_adjustment_floor(adjustments_csv: Path, inputs_csv: Path, category: str, floor_cases: int) -> int:
    """
    Ensure every valid report week has at least `floor_cases` in a category.
    Returns number of rows added/updated.
    """
    if floor_cases <= 0:
        return 0
    if not inputs_csv.exists():
        return 0
    inp = pd.read_csv(inputs_csv)
    if inp.empty or "Week" not in inp.columns:
        return 0
    weeks = []
    for w in inp["Week"].tolist():
        try:
            wi = int(float(w))
        except (TypeError, ValueError):
            continue
        if wi > 0:
            weeks.append(wi)
    weeks = sorted(set(weeks))
    if not weeks:
        return 0

    if adjustments_csv.exists():
        df = pd.read_csv(adjustments_csv)
    else:
        df = pd.DataFrame(columns=["Week", "Category", "Cases", "Evidence", "Note"])
    for col in ("Week", "Category", "Cases", "Evidence", "Note"):
        if col not in df.columns:
            df[col] = ""

    changed = 0
    for week in weeks:
        mask = (pd.to_numeric(df["Week"], errors="coerce") == float(week)) & (
            df["Category"].astype(str).str.strip() == category
        )
        if mask.any():
            current = pd.to_numeric(df.loc[mask, "Cases"], errors="coerce").fillna(0.0).sum()
            if current < float(floor_cases):
                first_idx = df.loc[mask].index[0]
                df.at[first_idx, "Cases"] = float(floor_cases)
                if "Note" in df.columns:
                    note = str(df.at[first_idx, "Note"] or "").strip()
                    tag = "weekly floor assumption"
                    if tag not in note.lower():
                        df.at[first_idx, "Note"] = (note + "; " if note else "") + tag
                changed += 1
        else:
            df.loc[len(df)] = {
                "Week": week,
                "Category": category,
                "Cases": float(floor_cases),
                "Evidence": "",
                "Note": "weekly floor assumption (shipping manager not fully tracked)",
            }
            changed += 1

    df = df.sort_values(["Week", "Category"]).reset_index(drop=True)
    df.to_csv(adjustments_csv, index=False)
    return changed


def _env_with_sandbox(sandbox_root: Path, *, barn_day_max_abs: int | None = None) -> dict:
    e = os.environ.copy()
    e["EGGROOM_TRACEABILITY_SANDBOX"] = str(sandbox_root.resolve())
    e["EGGROOM_TRACEABILITY_DATA_BASE"] = str(sandbox_root.resolve())
    if barn_day_max_abs is not None:
        e["EGGROOM_PRODUCTION_ADJ_MAX_ABS"] = str(int(barn_day_max_abs))
    return e


def _run_sqf(
    args: list[str],
    sandbox_root: Path,
    *,
    failsafe: bool = False,
    barn_day_max_abs: int | None = None,
    inventory_flow: bool = False,
    inventory_flow_max_carry_weeks: int = 2,
    inventory_flow_apply_to_traceability: bool = False,
) -> int:
    run_args = list(args)
    if failsafe and "--failsafe-to-100" not in run_args:
        run_args.append("--failsafe-to-100")
    if inventory_flow and "--inventory-flow-balance" not in run_args:
        run_args.append("--inventory-flow-balance")
        run_args.extend(["--inventory-flow-max-carry-weeks", str(int(inventory_flow_max_carry_weeks))])
        if inventory_flow_apply_to_traceability:
            run_args.append("--inventory-flow-apply-to-traceability")
    cmd = [sys.executable, str(SCRIPT_DIR / "sqf_traceability.py")] + run_args
    print("Running:", " ".join(cmd))
    r = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=_env_with_sandbox(sandbox_root, barn_day_max_abs=barn_day_max_abs),
    )
    return int(r.returncode or 0)


def _prep_sandbox_run(sandbox_root: Path) -> int:
    sync_main_exports_to_sandbox(sandbox_root)
    sync_cmd = [sys.executable, str(SCRIPT_DIR / "sync_weekly_recon_inputs.py"), "--sandbox", str(sandbox_root)]
    sync_rc = subprocess.run(sync_cmd, cwd=str(PROJECT_ROOT), env=_env_with_sandbox(sandbox_root)).returncode
    if int(sync_rc or 0) != 0:
        return int(sync_rc or 1)

    adj = sandbox_root / "Reference_Data" / "Traceability_Adjustments.csv"
    n_non = cap_adjustment_category(adj, "NonSlipShipment", max_cases=20)
    n_stored = cap_adjustment_category(adj, "StoredCarryover", max_cases=20)
    print(f"NonSlipShipment cap: {n_non} row(s) reduced to <= 20 cases/week.")
    print(f"StoredCarryover cap: {n_stored} row(s) reduced to <= 20 cases/week.")
    return 0


def run_balance_pass(
    sandbox_root: Path,
    *,
    direction: str,
    balance_base: str = "original",
    failsafe: bool = False,
    barn_day_max_abs: int | None = None,
    with_export: bool = True,
    inventory_flow: bool = False,
    inventory_flow_max_carry_weeks: int = 2,
    inventory_flow_apply_to_traceability: bool = False,
) -> int:
    """Single pallet-line balance pass; optionally export traceability CSV (common: no export on first of two passes)."""
    base = str(balance_base).strip().lower()
    args = [
        "--balance-palletlines",
        "--balance-palletlines-direction",
        direction,
        "--balance-hard-floor-sendback-cases",
        "60",
    ]
    if with_export:
        args.extend(["--export", "--export-traceability-csv"])
    if base in ("balancedworking", "balanced_working", "balanced"):
        args.extend(["--balance-palletlines-base", "balancedworking"])
    return _run_sqf(
        args,
        sandbox_root,
        failsafe=failsafe,
        barn_day_max_abs=barn_day_max_abs,
        inventory_flow=inventory_flow,
        inventory_flow_max_carry_weeks=inventory_flow_max_carry_weeks,
        inventory_flow_apply_to_traceability=inventory_flow_apply_to_traceability,
    )


def run_staged_balance(
    sandbox_root: Path,
    *,
    failsafe: bool = False,
    balance_direction: str = "back",
    barn_day_max_abs: int | None = None,
    inventory_flow: bool = False,
    inventory_flow_max_carry_weeks: int = 2,
    inventory_flow_apply_to_traceability: bool = False,
) -> int:
    """
    1) Mirror main Traceability_Exports PalletLines into sandbox (source of truth).
    2) Sync Weekly_Reconciliation_Inputs.csv into adjustments/production overrides.
    3) Cap NonSlipShipment + StoredCarryover in sandbox adjustments.
    4) BB balancing + export (one or two passes if direction is both).
    """
    rc = _prep_sandbox_run(sandbox_root)
    if rc != 0:
        return rc

    direction = str(balance_direction).strip().lower()
    if direction in ("both", "backward_forward"):
        rc = run_balance_pass(
            sandbox_root,
            direction="back",
            balance_base="original",
            failsafe=False,
            barn_day_max_abs=barn_day_max_abs,
            with_export=False,
            inventory_flow=inventory_flow,
            inventory_flow_max_carry_weeks=inventory_flow_max_carry_weeks,
            inventory_flow_apply_to_traceability=inventory_flow_apply_to_traceability,
        )
        if rc != 0:
            return rc
        return run_balance_pass(
            sandbox_root,
            direction="forward",
            balance_base="balancedworking",
            failsafe=failsafe,
            barn_day_max_abs=barn_day_max_abs,
            with_export=True,
            inventory_flow=inventory_flow,
            inventory_flow_max_carry_weeks=inventory_flow_max_carry_weeks,
            inventory_flow_apply_to_traceability=inventory_flow_apply_to_traceability,
        )

    if direction in ("fwd", "next"):
        direction = "forward"
    return run_balance_pass(
        sandbox_root,
        direction=direction,
        balance_base="original",
        failsafe=failsafe,
        barn_day_max_abs=barn_day_max_abs,
        inventory_flow=inventory_flow,
        inventory_flow_max_carry_weeks=inventory_flow_max_carry_weeks,
        inventory_flow_apply_to_traceability=inventory_flow_apply_to_traceability,
    )


def reexport_balanced(
    sandbox_root: Path,
    *,
    failsafe: bool = False,
    barn_day_max_abs: int | None = None,
    inventory_flow: bool = False,
    inventory_flow_max_carry_weeks: int = 2,
    inventory_flow_apply_to_traceability: bool = False,
) -> int:
    """Re-run traceability CSV from BalancedWorking after editing sandbox production adjustments (no new BB moves)."""
    return _run_sqf(
        ["--export", "--export-traceability-csv", "--balanced-traceability-csv"],
        sandbox_root,
        failsafe=failsafe,
        barn_day_max_abs=barn_day_max_abs,
        inventory_flow=inventory_flow,
        inventory_flow_max_carry_weeks=inventory_flow_max_carry_weeks,
        inventory_flow_apply_to_traceability=inventory_flow_apply_to_traceability,
    )


def run_go_failsafe(
    sandbox_root: Path,
    *,
    balance_direction: str = "both",
    family_max_cases_week: int = 5,
    barn_day_max_abs: int = 6,
    skip_failsafe_if_gap_le: int = 2,
    tolerance_pct: float = 0.0,
    audit_path: Path | None = None,
) -> int:
    """
    Full staged run: reset adjusted PalletLines, strip prior sandbox-go CSV rows, balance, then production / family / failsafe closure.
    Nest run inputs are never modified.
    """
    audit: dict = {"sandbox": str(sandbox_root.resolve())}
    ref = sandbox_root / "Reference_Data"

    n_clear = sfs.clear_balanced_working_adjusted(sandbox_root)
    audit["cleared_adjusted_palletlines"] = n_clear
    strip = sfs.strip_sandbox_go_rows(ref)
    audit["stripped_prior_sandbox_go"] = strip

    rc = _prep_sandbox_run(sandbox_root)
    if rc != 0:
        return rc

    direction = str(balance_direction).strip().lower()
    if direction in ("both", "backward_forward"):
        rc = run_balance_pass(
            sandbox_root,
            direction="back",
            balance_base="original",
            failsafe=False,
            barn_day_max_abs=barn_day_max_abs,
            with_export=False,
        )
        if rc != 0:
            return rc
        rc = run_balance_pass(
            sandbox_root,
            direction="forward",
            balance_base="balancedworking",
            failsafe=False,
            barn_day_max_abs=barn_day_max_abs,
            with_export=True,
        )
    else:
        if direction in ("fwd", "next"):
            direction = "forward"
        rc = run_balance_pass(
            sandbox_root,
            direction=direction,
            balance_base="original",
            failsafe=False,
            barn_day_max_abs=barn_day_max_abs,
        )
    if rc != 0:
        return rc

    def snap() -> list[dict]:
        return sfs.snapshot_traceability_json(sandbox_root, barn_day_max_abs)

    try:
        s1 = snap()
    except Exception as ex:
        print(f"Snapshot failed after balance: {ex}", file=sys.stderr)
        return 1
    audit["after_balance_snapshot_weeks"] = len(s1)

    sfs.apply_production_greedy(sandbox_root, s1, max_abs_per_barn_day=barn_day_max_abs, audit=audit)
    rc = reexport_balanced(sandbox_root, failsafe=False, barn_day_max_abs=barn_day_max_abs)
    if rc != 0:
        return rc

    try:
        s2 = snap()
    except Exception as ex:
        print(f"Snapshot failed after production greedy: {ex}", file=sys.stderr)
        return 1

    sfs.apply_family_takehome(
        sandbox_root,
        s2,
        max_cases_per_week=family_max_cases_week,
        tolerance_pct=tolerance_pct,
        audit=audit,
    )
    rc = reexport_balanced(sandbox_root, failsafe=False, barn_day_max_abs=barn_day_max_abs)
    if rc != 0:
        return rc

    try:
        s3 = snap()
    except Exception as ex:
        print(f"Snapshot failed after family take-home: {ex}", file=sys.stderr)
        return 1

    audit["weeks_before_failsafe"] = []
    tol = (1.0 - tolerance_pct / 100.0) if tolerance_pct else 1.0
    for r in s3:
        try:
            w = int(r["Week"])
            prod = int(r.get("Production") or 0)
            acc = int(r.get("Accounted") or 0)
        except (TypeError, ValueError, KeyError):
            continue
        if prod <= 0:
            continue
        gap = prod - acc
        ratio = acc / prod if prod else 0.0
        if ratio >= tol:
            continue
        entry = {"Week": w, "gap": gap}
        if gap <= skip_failsafe_if_gap_le:
            entry["skipped_small_gap"] = True
        audit["weeks_before_failsafe"].append(entry)

    sfs.ensure_failsafe_rows(
        sandbox_root,
        s3,
        tolerance_pct=tolerance_pct,
        skip_if_gap_le=skip_failsafe_if_gap_le,
        audit=audit,
    )

    rc = reexport_balanced(sandbox_root, failsafe=True, barn_day_max_abs=barn_day_max_abs)
    if rc != 0:
        return rc

    try:
        s_final = sfs.snapshot_traceability_json(sandbox_root, barn_day_max_abs, with_failsafe=True)
    except Exception as ex:
        print(f"Final snapshot failed: {ex}", file=sys.stderr)
        s_final = []
    audit["final_weeks"] = len(s_final)
    audit["final_below_100"] = [
        {"Week": r.get("Week"), "Traceability_Pct": r.get("Traceability_Pct"), "Variance": r.get("Variance")}
        for r in s_final
        if r.get("Production")
        and int(r.get("Production") or 0) > 0
        and (r.get("Traceability_Pct") is None or float(r.get("Traceability_Pct") or 0) < 99.99)
    ]

    out_audit = audit_path or (sandbox_root / "Traceability_Exports" / "sandbox_go_audit.json")
    out_audit = Path(out_audit)
    out_audit.parent.mkdir(parents=True, exist_ok=True)
    with out_audit.open("w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)
    print(f"Wrote audit: {out_audit}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Sandbox traceability: isolated Reference_Data + exports.")
    ap.add_argument("--sandbox", type=Path, default=DEFAULT_SANDBOX, help="Sandbox folder (default: ./Sandbox_Traceability)")
    ap.add_argument("--init", action="store_true", help="Copy Reference_Data and Original PalletLines into sandbox")
    ap.add_argument("--force", action="store_true", help="With --init, replace existing sandbox")
    ap.add_argument("--run", action="store_true", help="Cap NonSlip + BB balance + export + balanced CSV")
    ap.add_argument(
        "--go",
        action="store_true",
        help="Full failsafe: reset adjusted PalletLines, strip prior sandbox-go rows, balance, production greedy, family cap, failsafe top-up, audit JSON (never edits nest run).",
    )
    ap.add_argument(
        "--balance-direction",
        type=str,
        default=None,
        choices=["back", "forward", "both"],
        help="Pallet-line balance direction: --run defaults to back; --go defaults to both if omitted.",
    )
    ap.add_argument(
        "--barn-day-delta-max",
        type=int,
        default=6,
        help="Max |Delta_Stacks| per row / greedy cell cap; sets EGGROOM_PRODUCTION_ADJ_MAX_ABS for sandbox runs (default 6).",
    )
    ap.add_argument(
        "--family-takehome-max-cases-week",
        type=int,
        default=5,
        help="With --go: max case-equivalents/week for FamilyEmployeeTakehome (default 5).",
    )
    ap.add_argument(
        "--go-skip-failsafe-if-gap-le",
        type=int,
        default=2,
        dest="go_skip_failsafe_if_gap_le",
        help="With --go: do not add failsafe evidence if Production minus Accounted <= this (default 2).",
    )
    ap.add_argument(
        "--go-tolerance-pct",
        type=float,
        default=0.0,
        help="With --go: treat traceability as on-target if accounted/production >= 1 - this/100 (default 0).",
    )
    ap.add_argument(
        "--go-audit-json",
        type=Path,
        default=None,
        help="With --go: write audit JSON here (default: SANDBOX/Traceability_Exports/sandbox_go_audit.json).",
    )
    ap.add_argument(
        "--reexport-balanced",
        action="store_true",
        help="After editing Traceability_Production_Adjustments.csv in sandbox: export balanced CSV only",
    )
    ap.add_argument(
        "--sync-main-exports",
        action="store_true",
        help="Copy latest non-adjusted PalletLines from main Traceability_Exports into sandbox only (no balance run).",
    )
    ap.add_argument(
        "--failsafe-to-100",
        action="store_true",
        help="With --run or --reexport-balanced: top up weeks in Traceability_Failsafe_Evidence.csv to 100%% (evidence required; Evidence_File must exist if set). Same as EGGROOM_FAILSAFE_TO_100=1 for sqf only.",
    )
    ap.add_argument(
        "--inventory-flow-balance",
        action="store_true",
        help="Pass through to sqf_traceability: enable FIFO inventory carryover modeling and Inventory_Flow_By_Week output.",
    )
    ap.add_argument(
        "--inventory-flow-max-carry-weeks",
        type=int,
        default=2,
        help="With --inventory-flow-balance: max weeks inventory can carry before expiring from flow allocation (default 2).",
    )
    ap.add_argument(
        "--inventory-flow-apply-to-traceability",
        action="store_true",
        help="With --inventory-flow-balance: apply flow-accounted metrics to Accounted/Variance/Traceability in this run output.",
    )
    args = ap.parse_args()
    sandbox_root = Path(args.sandbox).resolve()

    if args.init:
        init_sandbox(sandbox_root, force=args.force)
    if getattr(args, "sync_main_exports", False):
        if not sandbox_root.exists():
            sandbox_root.mkdir(parents=True, exist_ok=True)
        n = sync_main_exports_to_sandbox(sandbox_root)
        print(f"Done ({n} weeks).")
        return 0
    if args.go:
        if not (sandbox_root / "Reference_Data").is_dir():
            print("Run --init first or fix --sandbox path.")
            return 1
        bal_dir = args.balance_direction or "both"
        return run_go_failsafe(
            sandbox_root,
            balance_direction=bal_dir,
            family_max_cases_week=int(args.family_takehome_max_cases_week),
            barn_day_max_abs=int(args.barn_day_delta_max),
            skip_failsafe_if_gap_le=int(args.go_skip_failsafe_if_gap_le),
            tolerance_pct=float(args.go_tolerance_pct),
            audit_path=args.go_audit_json,
        )
    if args.run:
        if not (sandbox_root / "Reference_Data").is_dir():
            print("Run --init first or fix --sandbox path.")
            return 1
        return run_staged_balance(
            sandbox_root,
            failsafe=args.failsafe_to_100,
            balance_direction=args.balance_direction or "back",
            barn_day_max_abs=int(args.barn_day_delta_max),
            inventory_flow=bool(args.inventory_flow_balance),
            inventory_flow_max_carry_weeks=int(args.inventory_flow_max_carry_weeks),
            inventory_flow_apply_to_traceability=bool(args.inventory_flow_apply_to_traceability),
        )
    if args.reexport_balanced:
        if not (sandbox_root / "Reference_Data").is_dir():
            print("Run --init first or fix --sandbox path.")
            return 1
        return reexport_balanced(
            sandbox_root,
            failsafe=args.failsafe_to_100,
            barn_day_max_abs=int(args.barn_day_delta_max),
            inventory_flow=bool(args.inventory_flow_balance),
            inventory_flow_max_carry_weeks=int(args.inventory_flow_max_carry_weeks),
            inventory_flow_apply_to_traceability=bool(args.inventory_flow_apply_to_traceability),
        )
    if not args.init:
        ap.print_help()
        print("\nExample: python Scripts/sandbox_traceability_pipeline.py --init && python Scripts/sandbox_traceability_pipeline.py --go")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
