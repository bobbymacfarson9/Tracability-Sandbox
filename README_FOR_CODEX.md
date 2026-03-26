# Sandbox Traceability Handoff (For Codex)

This document is the handoff context for autonomous agent work in the sandbox traceability environment.

## Primary Objective

Bring weekly traceability as close as possible to `99-100%` in sandbox with evidence-defensible logic.

Priority order:

1. Backward order movement (`Week N -> Week N-1`) when supported by date evidence
2. Inventory-flow carryover logic (FIFO)
3. Manual evidence-backed adjustments only when needed

## Scope Rules

- Work in sandbox only.
- Do not overwrite office/source-of-truth files outside sandbox.
- Keep changes auditable and reversible.
- Preserve existing behavior unless explicitly testing a new mode.

## Sandbox Layout (must exist)

- `Sandbox_Traceability/Reference_Data`
- `Sandbox_Traceability/Traceability_Exports`
- `Sandbox_Traceability/Traceability_Exports/Original`
- `Sandbox_Traceability/Traceability_Exports/BalancedWorking`
- `Sandbox_Traceability/grade outs`
- `Sandbox_Traceability/2024 Reports`
- `Sandbox_Traceability/Hilly Acres Slips For Barn Production`
- `Sandbox_Traceability/Mapping`

## Key Input Files

- `Sandbox_Traceability/Reference_Data/SQF_Traceability_Inputs.csv`
- `Sandbox_Traceability/Reference_Data/Weekly_Reconciliation_Inputs.csv`
- `Sandbox_Traceability/Reference_Data/Traceability_Adjustments.csv`
- `Sandbox_Traceability/Reference_Data/Traceability_Production_Weekly_Overrides.csv`
- `Sandbox_Traceability/Reference_Data/Traceability_Production_Adjustments.csv`
- `Sandbox_Traceability/Reference_Data/paths.json`

## Key Output Files

- `Sandbox_Traceability/Traceability_Exports/BalancedWorking/Traceability_2025_balanced.csv`
- Latest `Sandbox_Traceability/Traceability_Exports/SQF_Traceability_Report_*.xlsx`

Most useful tabs in the report:

- `Traceability`
- `Suggested_Reallocation`
- `OldDate_NewDate_ByDay`
- `OldDate_NewDate_BySKU`
- `Week_Reconciliation`
- `Reference_Usage_By_Week`
- `Inventory_Flow_By_Week`

## Core Scripts

- `Scripts/sandbox_traceability_pipeline.py`
- `Scripts/sqf_traceability.py`
- `Scripts/sync_weekly_recon_inputs.py`
- `Scripts/process_weekly_loading_slip.py`

## Baseline Commands

From project root:

```powershell
python Scripts/sandbox_traceability_pipeline.py --run
```

Re-export only:

```powershell
python Scripts/sandbox_traceability_pipeline.py --reexport-balanced
```

## Inventory Flow Mode (implemented)

Flow mode exists in both `sqf_traceability.py` and `sandbox_traceability_pipeline.py`.

Flags:

- `--inventory-flow-balance`
- `--inventory-flow-max-carry-weeks <N>`
- `--inventory-flow-apply-to-traceability`

Important behavior:

- Flow can be applied to headline traceability.
- It is currently guarded to only apply when it improves closeness to 100% (does not intentionally worsen baseline).

Example:

```powershell
python Scripts/sandbox_traceability_pipeline.py --run --inventory-flow-balance --inventory-flow-max-carry-weeks 6 --inventory-flow-apply-to-traceability
```

## Backward Reallocation Logic

`Reallocate_To_Prior_Week` in `SQF_Traceability_Inputs.csv` is used to move order cases backward by one week for 2025.

Common pitfall:

- Do **not** put reallocation values in `Shipped_Orders_Override`.
- Correct target column is `Reallocate_To_Prior_Week`.

## Current Known Situation

- Missing production weeks still exist: `14`, `33` (and `50` in full-year view).
- Backward moves helped some week-pairs, but large-gap weeks still need additional evidence and/or flow strategy.
- `Cases_Other` in Old/New splits is often timing/mapping bucket (not necessarily dropped rows).

## Operational Interpretation (important)

Desired operational model:

- Week N production can be consumed in week N+1 (and sometimes later).
- Monday usage often reflects prior-week production.
- Therefore strict same-week matching is not sufficient; carryover model is needed.

## Recommended Agent Workflow

1. Run baseline sandbox pipeline and collect deficits.
2. Compute week-level gaps to 99%.
3. Use `Suggested_Reallocation` + `OldDate_*` tabs for defensible backward moves.
4. Test inventory flow windows (`2`, `4`, `6`, `8`).
5. Keep only changes that improve or maintain week-level closeness to 100%.
6. For remaining gaps, use `Weekly_Reconciliation_Inputs.csv`:
   - `PartnerShipment_Cases`
   - `Production_Delta_Cases`
   - `Carryover_Cases`
   - always include `Evidence` and `Note`
7. Re-run and produce before/after summary.

## Expected Final Deliverables

- Updated sandbox inputs (not production files)
- Latest traceability report workbook
- Week-by-week action table:
  - Week
  - Current traceability
  - Gap to target
  - Lever used (reallocation / flow / evidence adjustment)
  - Exact value entered
  - Evidence note

## Acceptance Criteria

- Maximum achievable weeks at `>=99%` in sandbox
- Remaining below-target weeks explicitly explained by documented evidence gaps
- No unexplained regressions from baseline

