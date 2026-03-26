# Weekly Loading Slip Processor

## Overview
Processes an entire week's loading slip in one shot, extracting all SKUs, quantities, and OD (Old Date) quantities, then generates a comprehensive traceability report.

## Features

✅ **One-Shot Processing** - Process entire week's loading slip automatically  
✅ **OD Quantity Tracking** - Extracts OD quantities from loading slip  
✅ **Complete Traceability** - Shows exactly how much of each SKU went to each stop  
✅ **Summary Reports** - Generates per-stop and per-SKU summaries  
✅ **Excel Export** - Creates formatted Excel report with multiple sheets  

## Usage

### Quick Start (Default File)
```bash
python process_weekly_loading_slip.py
```
Uses default file: `Week 59 loading slip 2026 test.xlsx`

### Specify File
```bash
python process_weekly_loading_slip.py --file "Week 60 loading slip 2026.xlsx"
```

### Full Options
```bash
python process_weekly_loading_slip.py --file "Week 60 loading slip 2026.xlsx" --week "60" --day "Tuesday" --ship-date "2026-02-17"
```

### Command Line Arguments
- `--file` or `-f`: Loading slip filename (default: Week 59 loading slip 2026 test.xlsx)
- `--week` or `-w`: Week number (auto-detected from filename if not provided)
- `--day` or `-d`: Day name (auto-detected from filename if not provided)
- `--ship-date` or `-s`: Ship date in YYYY-MM-DD format (default: today)

## What It Does

1. **Loads Loading Slip**
   - Opens the Excel file
   - Finds header row automatically
   - Identifies all stops

2. **Extracts Data**
   - Finds all SKUs for each stop
   - Extracts quantities (regular and OD)
   - Maps SKUs to stops

3. **Generates Report**
   - Creates comprehensive traceability data
   - Calculates statistics
   - Generates summaries

4. **Saves Output**
   - Saves to `Traceability_Exports` folder
   - Filename: `Week{week}_{day}_Traceability_{timestamp}.xlsx`
   - Multiple sheets: Data, Summary, Stop Summary, SKU Summary

## Output Format

### Traceability Data Sheet
Columns:
- Week, Day, ShipDate
- Stop, SKU
- TotalQty, ODQty, RegularQty
- ProcessedDate
- SourceRow, QtyColumn, SKUColumn, ODColumn

### Summary Sheet
- Total Stops
- Total SKUs
- Total Boxes (Regular + OD)

### Stop Summary Sheet
Per-stop totals:
- TotalQty, ODQty, RegularQty
- SKU Count

### SKU Summary Sheet
Per-SKU totals:
- TotalQty, ODQty, RegularQty
- Stop Count

## Requirements

- Python 3.7+
- pandas
- openpyxl

Install: `pip install pandas openpyxl`

## Troubleshooting

**File Permission Error:**
- Close the Excel file if it's open
- Make sure you have read permissions

**No Data Found:**
- Check that the loading slip has the expected format
- Verify header row is detected correctly
- Check that SKUs and quantities are in expected columns

**Wrong Day/Week:**
- Use `--day` and `--week` arguments to override auto-detection
- Check filename format matches expected pattern

## Example Output

```
=== SUMMARY STATISTICS ===

Total Stops: 15
Total Unique SKUs: 45
Total Boxes: 1,234
  - Regular: 1,100
  - OD (Old Date): 134
Total Records: 156
```

## Integration

This script can be integrated with your existing traceability system:
1. Run weekly after loading slip is finalized
2. Import the generated Excel file into your database
3. Use for audit and traceability reporting
