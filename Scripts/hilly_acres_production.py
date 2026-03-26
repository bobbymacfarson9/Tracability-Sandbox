"""
Hilly Acres Production Reader
Reads production from Inputs sheet: TOTAL (stacks) section. 1 stack = 1 case.
Used for SQF traceability.

Path: Reference_Data/paths.json or HillyAcresPath; fallback: BASE_DIR / "Hilly Acres Slips For Barn Production"
Files: Week*.xlsx
Sheet: "Inputs" - Row 1 col 2 = Week #; find "TOTAL (stacks)" then sum White+Brown cols per barn.
"""

import os
import pandas as pd
from pathlib import Path
import re
import json

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
_REF = (os.environ.get("EGGROOM_TRACEABILITY_SANDBOX") or "").strip()
REF_DATA_DIR = (Path(_REF).resolve() / "Reference_Data") if _REF else (BASE_DIR / "Reference_Data")
_DATA_BASE = (os.environ.get("EGGROOM_TRACEABILITY_DATA_BASE") or "").strip()
DATA_BASE_DIR = Path(_DATA_BASE).resolve() if _DATA_BASE else BASE_DIR
INPUTS_SHEET = "Inputs"
_INPUTS_WEEK_CACHE = {}

# Pallet Information nest run: raw values may be in dozens (15 dozen = 1 case) or boxes (1:1 with cases). See paths.json NestRunPalletInfoUnit.
DOZEN_PER_CASE = 15
EGGS_PER_CASE = 180

# Row-count method: data starts at Excel row 7 (0-based index 6); each row = one pallet with UNITS_PER_PALLET cases.
# Column H (0-based index 7) is the stop factor: only rows where H has a value are counted; blanks are not counted.
# Medium-sized nest run pallets have blank weights, so using H (not weight column) ensures correct count.
PALLET_INFO_DATA_START_ROW = 6   # Excel row 7 (0-based)
PALLET_INFO_COL_H = 7            # Column H (0-based): stop factor — count row only if H non-blank
PALLET_INFO_STOP_COL = 10        # Column K (0-based) — legacy; row-count now uses PALLET_INFO_COL_H
PALLET_INFO_UNITS_PER_PALLET = 60  # cases per pallet (result is already in cases; no NestRunPalletInfoUnit conversion)
MAIN_SHEET = "Hilly Acres Farm Ltd."
PALLET_INFO_SHEET = "Pallet Information"
MAX_CASES_SANITY = 100000  # Reject if > 100k cases (likely data error)

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
PRODUCTION_ADJUSTMENTS_CSV = REF_DATA_DIR / "Traceability_Production_Adjustments.csv"


def _production_adj_max_abs():
    """Max |Delta_Stacks| per row; sandbox go may set EGGROOM_PRODUCTION_ADJ_MAX_ABS=6."""
    try:
        return int((os.environ.get("EGGROOM_PRODUCTION_ADJ_MAX_ABS") or "5").strip() or "5")
    except ValueError:
        return 5


def load_production_adjustments():
    """
    Load optional production adjustments.
    Columns: Week, DayName, Barn, Delta_Stacks, Evidence, Note
    Guardrail: each row must stay within +/- N stacks per day per barn (N from EGGROOM_PRODUCTION_ADJ_MAX_ABS or 5).
    """
    cols = ["Week", "DayName", "Barn", "Delta_Stacks", "Evidence", "Note"]
    if not PRODUCTION_ADJUSTMENTS_CSV.exists():
        return pd.DataFrame(columns=cols)
    try:
        df = pd.read_csv(PRODUCTION_ADJUSTMENTS_CSV)
    except Exception:
        return pd.DataFrame(columns=cols)
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    cleaned = []
    for _, row in df.iterrows():
        try:
            week = int(float(row.get("Week")))
            barn = int(float(row.get("Barn")))
            delta = float(row.get("Delta_Stacks"))
        except (TypeError, ValueError):
            continue
        day_name = str(row.get("DayName") or "").strip()
        if day_name not in DAY_NAMES:
            continue
        if abs(delta) > _production_adj_max_abs():
            continue
        cleaned.append({
            "Week": week,
            "DayName": day_name,
            "Barn": barn,
            "Delta_Stacks": delta,
            "Evidence": str(row.get("Evidence") or "").strip(),
            "Note": str(row.get("Note") or "").strip(),
        })
    return pd.DataFrame(cleaned, columns=cols)


def get_production_adjustment_rows_for_week(week_number):
    """Return validated production adjustment rows for one week."""
    df = load_production_adjustments()
    if df.empty:
        return df
    try:
        week_num = int(float(week_number))
    except (TypeError, ValueError):
        return df.iloc[0:0].copy()
    return df[df["Week"] == week_num].copy()


def get_production_adjustment_cases_for_week(week_number):
    """Return total production delta (stacks/cases) for a week."""
    df = get_production_adjustment_rows_for_week(week_number)
    if df.empty:
        return 0
    return int(round(df["Delta_Stacks"].sum()))


def _get_nest_run_unit():
    """
    Read NestRunPalletInfoUnit from paths.json. Raw values from Pallet Information are in this unit.
    "dozens" -> convert to cases (divide by 15). "eggs" -> divide by 180. "boxes" or missing -> no conversion (treat as cases).
    """
    try:
        with open(REF_DATA_DIR / "paths.json", encoding="utf-8") as f:
            config = json.load(f)
        u = (config.get("NestRunPalletInfoUnit") or "boxes").strip().lower()
        if u in ("dozen", "dozens"):
            return "dozens"
        if u in ("egg", "eggs"):
            return "eggs"
    except Exception:
        pass
    return "boxes"


def _nest_run_raw_to_cases(raw_value):
    """Convert raw nest run from Pallet Information to cases for traceability (to match Production)."""
    if raw_value is None or raw_value <= 0:
        return raw_value
    unit = _get_nest_run_unit()
    if unit == "dozens":
        return int(round(float(raw_value) / DOZEN_PER_CASE))
    if unit == "eggs":
        return int(round(float(raw_value) / EGGS_PER_CASE))
    return int(round(float(raw_value)))  # boxes = cases (1:1)


def _get_2026_week_mapping():
    """
    Report weeks 56-61 = early 2026 (use 2026 folder with calendar week 1-6).
    Returns (report_weeks_2026_set, iso_week_to_calendar_week) e.g. ({56..61}, {56:1, 57:2, ...}).
    """
    try:
        with open(REF_DATA_DIR / "paths.json", encoding="utf-8") as f:
            config = json.load(f)
        cal_to_iso = config.get("LoadingSlip2026CalendarToISO") or {}
        iso_to_cal = {int(v): int(k) for k, v in cal_to_iso.items()}
        report_2026 = config.get("ReportWeek2026Range")
        if isinstance(report_2026, list):
            weeks_2026 = set(int(w) for w in report_2026)
        else:
            weeks_2026 = set(iso_to_cal.keys())
        return weeks_2026, iso_to_cal
    except Exception:
        return {56, 57, 58, 59, 60, 61}, {56: 1, 57: 2, 58: 3, 59: 4, 60: 5, 61: 6}


def _is_2026_folder(folder_path):
    """True if this folder path represents 2026 data (e.g. '2026 EFC Reports')."""
    name = Path(folder_path).name if hasattr(folder_path, "name") else str(folder_path)
    return "2026" in name


def _get_week_to_look_for_in_folder(folder_path, report_week):
    """
    For a given report week and folder, return the week number to look for in that folder's filenames.
    Returns None if this folder should be skipped for this report week (e.g. 2025 folder when report week is 57).
    """
    weeks_2026, iso_to_cal = _get_2026_week_mapping()
    is_2026 = _is_2026_folder(folder_path)
    if report_week in weeks_2026:
        if is_2026:
            return iso_to_cal.get(report_week)  # 57 -> 2
        return None  # don't use 2025 folder for report weeks 56-61
    return report_week  # 1-55: look for same week in any folder


def _get_hilly_acres_paths():
    """Get list of Hilly Acres folder paths from paths.json or fallback. Supports HillyAcresPaths (list) or HillyAcresPath (single)."""
    paths_file = REF_DATA_DIR / "paths.json"
    if paths_file.exists():
        try:
            with open(paths_file, encoding="utf-8") as f:
                config = json.load(f)
            # List order: 2024, 2025, 2026. We search newest-first so 2026 EFC Reports overrides same week in earlier years.
            multi = config.get("HillyAcresPaths")
            if isinstance(multi, list) and multi:
                out = []
                for path in multi:
                    p = Path(path)
                    if not p.is_absolute():
                        p = DATA_BASE_DIR / p
                    if p.exists():
                        out.append(p)
                if out:
                    return list(reversed(out))  # newest folder first
            single = config.get("HillyAcresPath")
            if single:
                p = Path(single)
                if not p.is_absolute():
                    p = DATA_BASE_DIR / p
                if p.exists():
                    return [p]
        except Exception:
            pass
    fallback = DATA_BASE_DIR / "Hilly Acres Slips For Barn Production"
    return [fallback] if fallback.exists() else []


def _extract_barn_from_name(cell_val):
    """Extract barn number from 'Theodore & Aaron Eyking Barn 6' or 'Chris & Nico Eyking Barn 7'."""
    if cell_val is None or pd.isna(cell_val):
        return None
    s = str(cell_val).strip()
    m = re.search(r"[Bb]arn\s*(\d+)", s, re.I)
    return int(m.group(1)) if m else None


def _extract_week_from_filename(path_obj):
    """
    Extract week number from workbook filename.
    Supports names like:
    - Week 45 2025_ Hilly Acres Farm Ltd.xlsx
    - Hilly Acres Farm Ltd workbook Week 48.xlsx
    - Week1.xlsx
    Returns int or None.
    """
    stem = Path(path_obj).stem
    stem_norm = stem.replace(" ", "").replace("_", "").lower()
    if "copyof" in stem_norm or "copy of" in stem.lower():
        return None
    if "week" not in stem_norm:
        return None
    after_last_week = stem_norm.split("week")[-1].strip()
    match = re.match(r"^(\d{1,2})(?=\d{4}|\D|$)", after_last_week)
    if not match:
        return None
    try:
        return int(match.group(1).lstrip("0") or "0")
    except ValueError:
        return None


def _read_inputs_week_number(path_obj):
    """Read Inputs!C2 week number from a workbook. Returns int or None."""
    cache_key = str(Path(path_obj))
    if cache_key in _INPUTS_WEEK_CACHE:
        return _INPUTS_WEEK_CACHE[cache_key]
    try:
        df = pd.read_excel(path_obj, sheet_name=INPUTS_SHEET, header=None)
        if df.shape[0] < 2 or df.shape[1] < 3:
            _INPUTS_WEEK_CACHE[cache_key] = None
            return None
        cell_val = df.iloc[1, 2]
        if cell_val is None or pd.isna(cell_val):
            _INPUTS_WEEK_CACHE[cache_key] = None
            return None
        s = str(cell_val).strip().replace(".", "")
        if not s.isdigit():
            _INPUTS_WEEK_CACHE[cache_key] = None
            return None
        result = int(float(cell_val))
        _INPUTS_WEEK_CACHE[cache_key] = result
        return result
    except Exception:
        _INPUTS_WEEK_CACHE[cache_key] = None
        return None


def _is_single_barn_workbook(path_obj):
    """True if the workbook appears to be a single-barn file (e.g. '... Barn 3 STMRQ.xlsx'). Prefer full-farm workbooks over these."""
    name = Path(path_obj).name if hasattr(path_obj, "name") else str(path_obj)
    return " barn " in name.lower()


def _find_file_for_week(folder_path, week_number):
    """
    Find Hilly Acres file for the given week.

    When multiple files match the same week, prefer the full-farm workbook over single-barn
    workbooks (e.g. prefer "Week 21 2025_ Hilly Acres Farm Ltd.xlsx" over "Week 21 2025_ ... Barn 3 STMRQ.xlsx").

    Matching rules are intentionally strict to avoid false positives:
    1. Prefer files where both filename week and Inputs!C2 week match.
    2. Then allow filename-week match when Inputs!C2 is unreadable.
    3. Finally allow Inputs!C2-only match only for files with no parseable week in the filename.

    We never accept a file when filename week and Inputs!C2 disagree, since that caused
    wrong matches such as Week 42 -> Week 14 and Week 49 -> workbook Week 50.
    """
    folder = Path(folder_path)
    if not folder.exists():
        return None
    candidates = []
    for f in sorted(folder.glob("*.xlsx")):
        if "copy of" in f.name.lower():
            continue
        file_week = _extract_week_from_filename(f)
        inputs_week = _read_inputs_week_number(f)
        candidates.append((f, file_week, inputs_week))

    def _prefer_full_farm(matches):
        """From a list of (f, file_week, inputs_week), return full-farm workbook if any, else first."""
        if not matches:
            return None
        for f, _, _ in matches:
            if not _is_single_barn_workbook(f):
                return f
        return matches[0][0]

    # 1) Strongest match: filename week AND Inputs!C2 week both agree with target.
    strong = [(f, fw, iw) for f, fw, iw in candidates if fw == week_number and iw == week_number]
    if strong:
        return _prefer_full_farm(strong)

    # 2) Accept filename match when Inputs!C2 is unreadable, or for the 2026 folder where
    # the workbook filenames are the calendar-week source of truth and Inputs!C2 may carry
    # ISO-continuation values (e.g. workbook Week 1 with Inputs!C2 = 53).
    filename_matches = [(f, fw, iw) for f, fw, iw in candidates if fw == week_number and (iw is None or _is_2026_folder(folder))]
    if filename_matches:
        return _prefer_full_farm(filename_matches)

    # 3) Accept Inputs!C2-only match only when filename has no parseable week at all.
    inputs_only = [(f, fw, iw) for f, fw, iw in candidates if fw is None and iw == week_number]
    if inputs_only:
        return _prefer_full_farm(inputs_only)

    return None


# Inputs sheet: row 3, column C (0-based iloc[2,2]) = Week Ending date (e.g. January 4, 2026)
WEEK_ENDING_ROW = 2
WEEK_ENDING_COL = 2


def _get_week_ending_from_inputs(df):
    """Read Week Ending date from Inputs sheet row 3, col C. Returns datetime.date or None."""
    if df.shape[0] <= WEEK_ENDING_ROW or df.shape[1] <= WEEK_ENDING_COL:
        return None
    val = df.iloc[WEEK_ENDING_ROW, WEEK_ENDING_COL]
    if val is None or pd.isna(val):
        return None
    if hasattr(val, "date"):
        return val.date()
    if isinstance(val, (int, float)) and 40000 < val < 50000:
        from datetime import datetime as dt
        d = dt(1900, 1, 1) + pd.Timedelta(days=int(val) - 2)
        return d.date()
    s = str(val).strip()
    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d %H:%M:%S"]:
        try:
            return pd.to_datetime(s[:50], format=fmt).date()
        except Exception:
            continue
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None


def get_production_for_week_ending(week_ending_date, folder_path=None):
    """
    Find Hilly Acres workbook whose Inputs 'Week Ending' (row 3, col C) matches week_ending_date.
    Return total production (cases) for that workbook, or None.
    Used to match loading slips (Friday ship date + 1 = week ending) to the correct production week.
    """
    if week_ending_date is None:
        return None
    if hasattr(week_ending_date, "date"):
        target = week_ending_date.date()
    else:
        target = pd.to_datetime(week_ending_date).date()
    if folder_path is None:
        folders = _get_hilly_acres_paths()
    else:
        folders = [Path(folder_path)] if folder_path else []
    for folder in folders:
        for f in sorted(folder.glob("*.xlsx")):
            if "copy of" in f.name.lower():
                continue
            try:
                df = pd.read_excel(f, sheet_name=INPUTS_SHEET, header=None)
                we = _get_week_ending_from_inputs(df)
                if we is None:
                    continue
                if we == target:
                    total_stacks, _ = _parse_inputs_total_stacks(df)
                    if total_stacks is not None and 0 < total_stacks <= MAX_CASES_SANITY:
                        return total_stacks
                    try:
                        df_main = pd.read_excel(f, sheet_name=MAIN_SHEET, header=None)
                        cases = _parse_main_sheet_fallback(df_main)
                        if cases and 0 < cases <= MAX_CASES_SANITY:
                            return cases
                    except Exception:
                        pass
                    return total_stacks
            except Exception:
                continue
    return None


def _parse_inputs_total_stacks(df):
    """
    Parse Inputs sheet: find "TOTAL (stacks)" then sum White+Brown for each barn.
    Returns (total_stacks, barn_stacks_dict). 1 stack = 1 case.
    barn_stacks: {barn: total_stacks}
    """
    if df.shape[0] < 10 or df.shape[1] < 7:
        return None, None
    total_stacks = 0
    barn_stacks = {}
    for row_num in range(len(df)):
        cell = str(df.iloc[row_num, 4] or "").strip().upper()
        if "TOTAL" in cell and "STACK" in cell:
            for rr in range(row_num + 2, min(row_num + 15, len(df))):
                barn_val = df.iloc[rr, 4]
                if pd.isna(barn_val) or str(barn_val).strip() == "":
                    break
                try:
                    barn = int(float(barn_val))
                    w = float(df.iloc[rr, 5] or 0)
                    b = float(df.iloc[rr, 6] or 0)
                    st = w + b
                    barn_stacks[barn] = st
                    total_stacks += st
                except (ValueError, TypeError, IndexError):
                    break
            return (int(round(total_stacks)), barn_stacks) if total_stacks > 0 else (None, None)
    return None, None


def _parse_main_sheet_fallback(df):
    """Fallback: parse main sheet RECEIVED dozen, 15 dozen = 1 case."""
    if df.shape[0] < 7 or df.shape[1] < 8:
        return None
    total_dozen = 0
    for row_num in range(6, len(df)):
        cell_a = str(df.iloc[row_num, 0] or "").strip()
        if "Totals" in cell_a or "BIRD INVENTORY" in cell_a:
            break
        barn = _extract_barn_from_name(df.iloc[row_num, 0])
        if barn is None:
            continue
        try:
            received = df.iloc[row_num, 7]
            if pd.notna(received):
                total_dozen += float(received)
        except (ValueError, TypeError, IndexError):
            pass
    if total_dozen <= 0:
        return None
    return int(round(total_dozen / 15))


def get_barn_stacks_for_week(week_number, folder_path=None):
    """
    Get production (cases) per barn from Inputs TOTAL (stacks). Returns {barn: stacks}.
    Includes barns with 0. Returns None if no file or parse failure.
    """
    if folder_path is None:
        folders = _get_hilly_acres_paths()
    else:
        folders = [Path(folder_path)] if folder_path else []
    file_path = None
    for folder in folders:
        week_to_use = _get_week_to_look_for_in_folder(folder, week_number)
        if week_to_use is None:
            continue
        file_path = _find_file_for_week(folder, week_to_use)
        if file_path is not None:
            break
    if file_path is None:
        return None
    try:
        df = pd.read_excel(file_path, sheet_name=INPUTS_SHEET, header=None)
    except Exception:
        return None
    _, barn_stacks = _parse_inputs_total_stacks(df)
    if barn_stacks:
        adjustments = get_production_adjustment_rows_for_week(week_number)
        if not adjustments.empty:
            for _, row in adjustments.iterrows():
                barn = int(row["Barn"])
                barn_stacks[barn] = barn_stacks.get(barn, 0) + float(row["Delta_Stacks"])
    return barn_stacks


def get_production_by_barn_day(week_number, folder_path=None):
    """
    Get production (cases) by (day_name, barn). Inputs TOTAL (stacks) gives per-barn;
    distribute each barn's weekly total evenly across 7 days. 1 stack = 1 case.
    """
    if folder_path is None:
        folders = _get_hilly_acres_paths()
    else:
        folders = [Path(folder_path)] if folder_path else []
    file_path = None
    for folder in folders:
        week_to_use = _get_week_to_look_for_in_folder(folder, week_number)
        if week_to_use is None:
            continue
        file_path = _find_file_for_week(folder, week_to_use)
        if file_path is not None:
            break
    if file_path is None:
        return None
    try:
        df = pd.read_excel(file_path, sheet_name=INPUTS_SHEET, header=None)
    except Exception:
        return None
    _, barn_stacks = _parse_inputs_total_stacks(df)
    if not barn_stacks:
        return None
    result = {}
    for barn, stacks in barn_stacks.items():
        cases_per_day = stacks / 7
        for day in DAY_NAMES:
            result[(day, barn)] = cases_per_day
    adjustments = get_production_adjustment_rows_for_week(week_number)
    if not adjustments.empty:
        for _, row in adjustments.iterrows():
            key = (row["DayName"], int(row["Barn"]))
            result[key] = result.get(key, 0) + float(row["Delta_Stacks"])
    return result


def get_production_for_week(week_number, folder_path=None):
    """
    Get total production (cases) for the week from Inputs sheet TOTAL (stacks). 1 stack = 1 case.
    Fallback to main sheet RECEIVED dozen if Inputs not found.
    Report weeks 56-61 use 2026 folder with calendar week 1-6; 1-55 use week number (2025 or 2026 by folder).
    """
    if folder_path is None:
        folders = _get_hilly_acres_paths()
    else:
        folders = [Path(folder_path)] if folder_path else []
    file_path = None
    for folder in folders:
        week_to_use = _get_week_to_look_for_in_folder(folder, week_number)
        if week_to_use is None:
            continue
        file_path = _find_file_for_week(folder, week_to_use)
        if file_path is not None:
            break
    if file_path is None:
        return None
    try:
        df_in = pd.read_excel(file_path, sheet_name=INPUTS_SHEET, header=None)
    except Exception:
        df_in = None
    cases = None
    if df_in is not None:
        total_stacks, _ = _parse_inputs_total_stacks(df_in)
        if total_stacks is not None:
            cases = total_stacks
    if cases is None:
        try:
            df_main = pd.read_excel(file_path, sheet_name=MAIN_SHEET, header=None)
            cases = _parse_main_sheet_fallback(df_main)
        except Exception:
            pass
    if cases is None or cases <= 0:
        return None
    if cases > MAX_CASES_SANITY:
        return None
    return cases


def diagnose_week_file_finding(week_number):
    """
    For a given week, report which folders we search, which files exist in each,
    and whether we found a workbook for this week. Use to see why week 45 might
    not find a file (e.g. folder missing, or file named differently).
    Returns dict: folders_searched, file_found, per_folder[{path, exists, file_count, file_names, chosen}].
    """
    folders = _get_hilly_acres_paths()
    result = {"week": week_number, "folders_searched": [str(p) for p in folders], "file_found": None, "per_folder": []}
    for folder in folders:
        folder = Path(folder)
        entry = {"path": str(folder), "exists": folder.exists(), "file_count": 0, "file_names": [], "chosen": None}
        if folder.exists():
            files = sorted(folder.glob("*.xlsx"))
            entry["file_count"] = len(files)
            entry["file_names"] = [f.name for f in files[:50]]  # first 50
            if len(files) > 50:
                entry["file_names"].append(f"... and {len(files) - 50} more")
            week_to_use = _get_week_to_look_for_in_folder(folder, week_number)
            chosen = _find_file_for_week(folder, week_to_use) if week_to_use is not None else None
            if chosen is not None:
                entry["chosen"] = str(chosen)
                if result["file_found"] is None:
                    result["file_found"] = str(chosen)
        result["per_folder"].append(entry)
    return result


def diagnose_pallet_sheet_for_week(week_number):
    """
    If we have a workbook for this week, load Pallet Information and report structure
    and why parsing might fail. Use to compare week 45 vs week 42.
    Returns dict: file_path, sheet_exists, shape, header_row_0, header_row_1,
    totals_header_found, line_item_header_found, nest_run_result, parser_layout.
    """
    out = {"file_path": None, "sheet_exists": False, "shape": None, "header_row_0": [], "header_row_1": [],
           "totals_header_found": None, "line_item_header_found": None, "nest_run_result": None, "parser_layout": None}
    folders = _get_hilly_acres_paths()
    file_path = None
    for folder in folders:
        week_to_use = _get_week_to_look_for_in_folder(folder, week_number)
        if week_to_use is None:
            continue
        file_path = _find_file_for_week(folder, week_to_use)
        if file_path is not None:
            break
    if file_path is None:
        return out
    out["file_path"] = str(file_path)
    try:
        df = pd.read_excel(file_path, sheet_name=PALLET_INFO_SHEET, header=None)
    except Exception as e:
        out["error"] = str(e)
        return out
    out["sheet_exists"] = True
    out["shape"] = (df.shape[0], df.shape[1])
    out["header_row_0"] = [str(df.iloc[0, c]) if c < df.shape[1] else "" for c in range(min(15, df.shape[1]))]
    if df.shape[0] >= 2:
        out["header_row_1"] = [str(df.iloc[1, c]) if c < df.shape[1] else "" for c in range(min(15, df.shape[1]))]
    out["totals_header_found"] = _find_nest_run_header_row(df, max_rows=5, max_cols=min(30, df.shape[1]))
    out["line_item_header_found"] = _find_line_item_nr_columns(df, max_rows=5, max_cols=min(25, df.shape[1]))
    diag = []
    nr = _parse_pallet_info_nest_run_boxes(df, _diagnostic_out=diag)
    out["nest_run_result"] = nr
    if diag:
        out["parser_layout"] = diag[0].get("layout", "totals")
    return out


def _find_nest_run_header_row(df, max_rows=5, max_cols=30):
    """
    Scan first max_rows for a row containing both NR Grid (Size) and NR Total column labels.
    Returns (header_row_index, nr_grid_col, nr_total_col) or (None, None, None).
    """
    # Normalize: look for these substrings (case-insensitive)
    grid_patterns = ("nr grid size", "nr grid", "grid size")
    total_patterns = ("nr total", "total nr", "total boxes", "nr boxes")
    rows = min(max_rows, df.shape[0])
    cols = min(max_cols, df.shape[1])
    for row in range(rows):
        nr_grid_col = None
        nr_total_col = None
        for c in range(cols):
            val = str(df.iloc[row, c] or "").strip().lower()
            if not val:
                continue
            for pat in grid_patterns:
                if pat in val:
                    nr_grid_col = c
                    break
            for pat in total_patterns:
                if pat in val:
                    nr_total_col = c
                    break
        if nr_grid_col is not None and nr_total_col is not None:
            return (row, nr_grid_col, nr_total_col)
    return (None, None, None)


def _find_line_item_nr_columns(df, max_rows=5, max_cols=25):
    """
    Detect the line-item layout: header row with 'NR' (nest run indicator) and a quantity column
    (GROSS, QUAN, NET, QTY, etc.). Used when sheet has no 'NR Grid Size' / 'NR Total' totals block.
    Returns (header_row, nr_indicator_col, quantity_col, quantity_header_text)
    or (None, None, None, None).
    """
    nr_header_patterns = ("nr",)  # column header exactly or containing "nr"
    preferred_qty_patterns = ("quan", "quantity", "qty", "boxes")
    fallback_qty_patterns = ("gross", "net")
    rows = min(max_rows, df.shape[0])
    cols = min(max_cols, df.shape[1])
    for row in range(rows):
        nr_col = None
        qty_match = None
        for c in range(cols):
            val = str(df.iloc[row, c] or "").strip().lower()
            if not val:
                continue
            # NR column: header is exactly "nr" or is a word that is just "nr"
            if val == "nr" or (val.replace(" ", "") == "nr"):
                nr_col = c
            for priority, patterns in enumerate((preferred_qty_patterns, fallback_qty_patterns)):
                for pat in patterns:
                    if pat in val:
                        candidate = (priority, c, val)
                        if qty_match is None or candidate < qty_match:
                            qty_match = candidate
                        break
        if nr_col is not None and qty_match is not None:
            _, qty_col, qty_header = qty_match
            return (row, nr_col, qty_col, qty_header)
    return (None, None, None, None)


def _pallet_info_row_has_h(df, r):
    """True if column H (stop factor) at row r has a value; blanks are not counted."""
    if df.shape[1] <= PALLET_INFO_COL_H:
        return False
    val = df.iloc[r, PALLET_INFO_COL_H]
    if pd.isna(val):
        return False
    if str(val).strip() in ("", "NA", "N/A", "#N/A"):
        return False
    return True


def _parse_pallet_info_nest_run_line_item(df, header_row, nr_col, qty_col, qty_header_text, _diagnostic_out=None):
    """
    Sum quantity column for every row where NR column indicates nest run (e.g. cell value "NR")
    and column H has a value (stop factor: blanks not counted).
    Returns total boxes or None. If _diagnostic_out is a list, append one dict with layout='line_item'.
    """
    total = 0
    count = 0
    qty_header_norm = str(qty_header_text or "").strip().lower()
    qty_is_case_count = any(pat in qty_header_norm for pat in ("quan", "quantity", "qty", "boxes"))
    data_start = max(header_row + 1, PALLET_INFO_DATA_START_ROW)
    for r in range(data_start, len(df)):
        if not _pallet_info_row_has_h(df, r):
            continue
        nr_val = str(df.iloc[r, nr_col] or "").strip().upper()
        if nr_val != "NR":
            continue
        try:
            v = df.iloc[r, qty_col]
            if pd.isna(v) or str(v).strip() == "":
                continue
            num = int(float(v))
            total += num
            count += 1
        except (ValueError, TypeError):
            continue
    if _diagnostic_out is not None:
        _diagnostic_out.append({
            "layout": "line_item",
            "header_row": header_row,
            "nr_col": nr_col,
            "qty_col": qty_col,
            "qty_header": qty_header_text,
            "qty_is_case_count": qty_is_case_count,
            "data_start_row": data_start,
            "rows_included": count,
            "rows_included_detail": [],  # line-item doesn't list each row in detail
        })
    return total if total > 0 else None


def _is_totals_row(grid_cell_text):
    """True if this looks like a per-barn Totals row or a Grand Total row."""
    t = str(grid_cell_text or "").strip().lower()
    if not t:
        return False
    return "total" in t  # Totals, Total, Grand Totals, Grand Total, etc.


def _is_grand_total_row(grid_cell_text):
    """True if this looks like a single Grand Total row (whole sheet)."""
    t = str(grid_cell_text or "").strip().lower()
    return "grand" in t and "total" in t


def _parse_pallet_info_nest_run_by_row_count(df, _diagnostic_out=None):
    """
    Row-count method: data starts at row 7 (0-based index 6). Each row = one pallet = 60 cases.
    Column H (index 7) is the stop factor: only count rows where H has a value; stop when H is blank.
    Total = (rows counted) × 60 cases. Result is already in cases (no unit conversion applied).
    """
    start_row = PALLET_INFO_DATA_START_ROW
    col_h = PALLET_INFO_COL_H
    units_per = PALLET_INFO_UNITS_PER_PALLET
    if df.shape[0] <= start_row or df.shape[1] <= col_h:
        return None
    count = 0
    for r in range(start_row, len(df)):
        if not _pallet_info_row_has_h(df, r):
            break
        count += 1
    if count == 0:
        return None
    raw = count * units_per
    if _diagnostic_out is not None:
        _diagnostic_out.append({
            "layout": "row_count",
            "data_start_row": start_row,
            "stop_col_H": col_h,
            "units_per_pallet": units_per,
            "rows_counted": count,
            "raw_total": raw,
        })
    return raw


def _parse_pallet_info_nest_run_boxes(df, _diagnostic_out=None):
    """
    Parse Pallet Information sheet for nest run. Uses column H as stop factor: only rows
    where H has a value are counted; blanks are not. Tries in order:

    1) Totals layout: header has 'NR Grid Size' and 'NR Total'; sum Totals rows where column H is non-blank.

    2) Line-item layout: header has 'NR' and 'GROSS'/'QUAN'/etc.; sum quantity where NR = 'NR' and H is non-blank.

    3) Row-count method: data from row 7 down; each row = one pallet × 60 units;
       only count rows where column H is non-blank; stop when H is blank.

    Returns total nest run (raw units) or None. If _diagnostic_out is a list, append diagnostic dict(s).
    """
    if df.shape[0] < 2 or df.shape[1] < 6:
        return None

    # --- Try totals layout (NR Grid Size + NR Total columns, Totals rows) ---
    header_row, nr_grid_col, nr_total_col = _find_nest_run_header_row(df, max_rows=5, max_cols=min(30, df.shape[1]))
    if header_row is None and df.shape[1] >= 22:
        header_row, nr_grid_col, nr_total_col = 0, 20, 21
    if header_row is not None:
        total_boxes = 0
        grand_total_value = None
        data_start = header_row + 1
        rows_included = []
        for r in range(data_start, len(df)):
            if not _pallet_info_row_has_h(df, r):
                continue
            grid_val = str(df.iloc[r, nr_grid_col] or "").strip()
            if not _is_totals_row(grid_val):
                continue
            try:
                v = df.iloc[r, nr_total_col]
                if pd.isna(v) or str(v).strip() == "":
                    continue
                num = int(float(v))
            except (ValueError, TypeError):
                continue
            if _is_grand_total_row(grid_val):
                grand_total_value = num
                rows_included.append({"row": r, "label": grid_val or "(blank)", "value": num, "type": "grand_total"})
            else:
                total_boxes += num
                rows_included.append({"row": r, "label": grid_val or "(blank)", "value": num, "type": "barn_total"})
        if grand_total_value is not None:
            total_boxes = grand_total_value
        if total_boxes > 0:
            if _diagnostic_out is not None:
                _diagnostic_out.append({
                    "layout": "totals",
                    "header_row": header_row,
                    "nr_grid_col": nr_grid_col,
                    "nr_total_col": nr_total_col,
                    "grand_total_used": grand_total_value is not None,
                    "rows_included": rows_included,
                })
            return total_boxes

    # --- Fallback: line-item layout (NR column + GROSS/QUAN/etc., sum where NR = 'NR') ---
    li_header, nr_col, qty_col, qty_header = _find_line_item_nr_columns(df, max_rows=5, max_cols=min(25, df.shape[1]))
    if li_header is not None:
        result = _parse_pallet_info_nest_run_line_item(df, li_header, nr_col, qty_col, qty_header, _diagnostic_out=_diagnostic_out)
        if result is not None:
            return result

    # --- Last resort: row-count method (row 7 down, 60 per pallet) ---
    # Some workbooks use a simple pallet-count layout, but detailed NR line-item sheets can
    # also satisfy the row-count heuristic. Running row-count last avoids inflating NestRun
    # by counting every pallet row when the sheet already exposes explicit NR rows/totals.
    row_count_result = _parse_pallet_info_nest_run_by_row_count(df, _diagnostic_out=_diagnostic_out)
    if row_count_result is not None and row_count_result > 0:
        return row_count_result

    return None


def get_nest_run_boxes_for_week(week_number, folder_path=None):
    """
    Get nest run (NR) total boxes for the week from Hilly Acres Pallet Information sheet.
    Sums the 'NR Total' (boxes) from each barn's Totals row (or Grand Total). Returns int or None if not found.
    """
    boxes, _, _ = get_nest_run_boxes_for_week_diagnostic(week_number, folder_path)
    return boxes


def get_nest_run_boxes_for_week_diagnostic(week_number, folder_path=None):
    """
    Same as get_nest_run_boxes_for_week but returns (boxes, file_path, diagnostic_dict).
    diagnostic_dict has: header_row, nr_grid_col, nr_total_col, grand_total_used, rows_included.
    """
    if folder_path is None:
        folders = _get_hilly_acres_paths()
    else:
        folders = [Path(folder_path)] if folder_path else []
    file_path = None
    for folder in folders:
        week_to_use = _get_week_to_look_for_in_folder(folder, week_number)
        if week_to_use is None:
            continue
        file_path = _find_file_for_week(folder, week_to_use)
        if file_path is not None:
            break
    if file_path is None:
        return (None, None, None)
    try:
        df = pd.read_excel(file_path, sheet_name=PALLET_INFO_SHEET, header=None)
    except Exception:
        return (None, file_path, None)
    diag = []
    result = _parse_pallet_info_nest_run_boxes(df, _diagnostic_out=diag)
    if result is not None:
        # Row-count and QUAN/QTY-style line-item layouts are already in cases.
        if diag and (
            diag[0].get("layout") == "row_count"
            or (diag[0].get("layout") == "line_item" and diag[0].get("qty_is_case_count"))
        ):
            result = int(round(result))
        else:
            result = _nest_run_raw_to_cases(result)
    return (result, file_path, diag[0] if diag else None)


def get_nest_run_boxes_for_week_ending(week_ending_date, folder_path=None):
    """
    Get nest run boxes from Hilly Acres workbook whose Inputs Week Ending matches week_ending_date.
    """
    boxes, _, _ = get_nest_run_boxes_for_week_ending_diagnostic(week_ending_date, folder_path)
    return boxes


def get_nest_run_boxes_for_week_ending_diagnostic(week_ending_date, folder_path=None):
    """
    Same as get_nest_run_boxes_for_week_ending but returns (boxes, file_path, diagnostic_dict).
    """
    if week_ending_date is None:
        return (None, None, None)
    if hasattr(week_ending_date, "date"):
        target = week_ending_date.date()
    else:
        target = pd.to_datetime(week_ending_date).date()
    if folder_path is None:
        folders = _get_hilly_acres_paths()
    else:
        folders = [Path(folder_path)] if folder_path else []
    for folder in folders:
        for f in sorted(folder.glob("*.xlsx")):
            if "copy of" in f.name.lower():
                continue
            try:
                df_in = pd.read_excel(f, sheet_name=INPUTS_SHEET, header=None)
                we = _get_week_ending_from_inputs(df_in)
                if we != target:
                    continue
                df_pallet = pd.read_excel(f, sheet_name=PALLET_INFO_SHEET, header=None)
                diag = []
                boxes = _parse_pallet_info_nest_run_boxes(df_pallet, _diagnostic_out=diag)
                if boxes is not None:
                    if diag and (
                        diag[0].get("layout") == "row_count"
                        or (diag[0].get("layout") == "line_item" and diag[0].get("qty_is_case_count"))
                    ):
                        boxes = int(round(boxes))
                    else:
                        boxes = _nest_run_raw_to_cases(boxes)
                    return (boxes, f, diag[0] if diag else None)
            except Exception:
                continue
    return (None, None, None)
