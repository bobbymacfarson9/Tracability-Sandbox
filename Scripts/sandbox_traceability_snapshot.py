"""
Emit a JSON array of per-week traceability metrics for a sandbox (stdout).
Uses BalancedWorking PalletLines; does not enable failsafe top-up.

Usage:
  python Scripts/sandbox_traceability_snapshot.py SANDBOX_ROOT PROJECT_ROOT [PRODUCTION_ADJ_MAX_ABS] [FAILSAFE]
  FAILSAFE: if 1 or true, apply Traceability_Failsafe_Evidence top-up (EGGROOM_FAILSAFE_TO_100).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 3:
        print(
            "Usage: sandbox_traceability_snapshot.py SANDBOX_ROOT PROJECT_ROOT [PRODUCTION_ADJ_MAX_ABS]",
            file=sys.stderr,
        )
        return 1
    sandbox = Path(sys.argv[1]).resolve()
    project_root = Path(sys.argv[2]).resolve()
    os.environ["EGGROOM_TRACEABILITY_SANDBOX"] = str(sandbox)
    os.environ["EGGROOM_TRACEABILITY_DATA_BASE"] = str(sandbox)
    if len(sys.argv) > 4 and str(sys.argv[4]).strip().lower() in ("1", "true", "yes", "failsafe"):
        os.environ["EGGROOM_FAILSAFE_TO_100"] = "1"
    else:
        os.environ.pop("EGGROOM_FAILSAFE_TO_100", None)
    if len(sys.argv) > 3:
        t = str(sys.argv[3]).strip()
        if t and t != "-":
            try:
                os.environ["EGGROOM_PRODUCTION_ADJ_MAX_ABS"] = str(int(float(t)))
            except (TypeError, ValueError):
                pass

    scripts = project_root / "Scripts"
    sys.path.insert(0, str(scripts))

    import sqf_traceability as sqf  # noqa: E402

    rows = sqf.run_all_weeks(
        sqf.BALANCED_WORKING_DIR,
        palletlines_source=sqf.PALLETLINES_SOURCE_PREFERRED,
    )
    keys = [
        "Week",
        "Year",
        "Production",
        "Accounted",
        "Traceability_Pct",
        "Variance",
        "Shipped_Orders",
        "Shipped_NestRun",
        "Manual_Adjustments",
        "Production_Adjustment",
        "OffGrades_GradeOut",
        "Eggs_Stored",
    ]
    out = []
    for r in rows or []:
        out.append({k: r.get(k) for k in keys})
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
