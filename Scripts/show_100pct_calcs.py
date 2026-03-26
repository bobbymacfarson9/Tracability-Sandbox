"""Show sample calculations for weeks displaying 100% traceability."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import sqf_traceability as sqf

results = sqf.run_all_weeks()
weeks_at_100 = [r for r in results if r["Traceability_Pct"] == 100.0]

print("Sample calculations for weeks displaying 100% traceability:")
print("=" * 80)
for r in weeks_at_100:
    prod = r["Production"]
    orders = r["Shipped_Orders"]
    nest = r["Shipped_NestRun"]
    off = r["OffGrades_GradeOut"]
    accounted = r["Accounted"]
    raw_pct = (accounted / prod * 100) if prod > 0 else None
    over = "YES - was over 100%, capped" if raw_pct and raw_pct > 100 else "NO - legit 100%"
    print(f"""
Week {r['Week']}:
  Production:         {prod:,}
  Shipped_Orders:     {orders:,}
  Shipped_NestRun:    {nest:,.0f}
  OffGrades_GradeOut: {off:,.0f}
  Accounted (sum):    {accounted:,}  = {orders:,} + {nest:,.0f} + {off:,.0f}
  Uncapped %:         {raw_pct:.2f}%  (Accounted / Production)
  Over 100%?          {over}
""")
