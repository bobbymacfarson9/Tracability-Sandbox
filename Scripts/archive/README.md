# Script Archive

Backup copies of scripts, kept for rollback if needed.

## process_weekly_loading_slip_backup_20260309.py

Snapshot of `process_weekly_loading_slip.py` as of 2026-03-09.

**To restore:** Copy this file over `Scripts/process_weekly_loading_slip.py`

**Note:** This backup was taken before the undercounting fix. It includes the stop-boundary fix (prevents overcounting from reusing pharmacy blocks) but had a bug where numeric values like "5.0" or "8.0" in the qty column were wrongly treated as stop headers, causing early loop exit and undercounting. The current script excludes those numeric values from stop-header detection.
