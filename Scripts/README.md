# Week 60 SKU Mapping Table Generator

Python script to create a Week 60 SKU mapping table by comparing Week 56 (old format) and Week 60 (new format with OD column) loading slips.

## Setup

1. **Install Python** (if not already installed)
   - Download from https://www.python.org/downloads/
   - Make sure to check "Add Python to PATH" during installation

2. **Install required packages**:
   ```bash
   pip install pandas openpyxl
   ```
   
   Or use the requirements file:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Ensure files exist** in `Reference_Data` folder:
   - `Week 56 Loading Slipp 2026.xlsx` (old format)
   - `Week 60 loading slip 2026.xlsx` (new format with OD column)
   - `Week_42_Stop_SKU_Final_POLISHED.xlsx` (existing mapping table)

2. **Run the script**:
   ```bash
   python create_week60_mapping.py
   ```

3. **Output**:
   - Creates `Week_60_Stop_SKU_Final_POLISHED.xlsx` in `Reference_Data` folder
   - Shows comparison report in console
   - All QtyCellAddr and SKUCellAddr addresses automatically shifted to account for OD column

## What It Does

1. **Compares** Week 56 vs Week 60 column structures
2. **Identifies** column shifts (OD column insertion)
3. **Reads** existing Week 42 mapping table
4. **Shifts** all cell addresses right by the appropriate amount
5. **Creates** new Week 60 mapping table with correct addresses

## Output Example

```
=== COLUMN STRUCTURE COMPARISON ===

WEEK 56 (OLD) COLUMNS:
  B: Qty
  C: SKU
  D: Qty
  E: SKU

WEEK 60 (NEW) COLUMNS:
  B: OD
  C: Qty
  D: SKU
  E: OD
  F: Qty
  G: SKU

NEW COLUMNS IN WEEK 60:
  B: OD
  E: OD

COLUMN SHIFTS:
  'Qty': B → C (shift: +1)
  'SKU': C → D (shift: +1)
```

## Troubleshooting

**File not found errors:**
- Check that all required files are in `Reference_Data` folder
- Verify file names match exactly (including spaces and capitalization)

**Column detection issues:**
- Script looks for header row automatically (usually row 4)
- If headers aren't detected, check the loading slip format

**Address shift issues:**
- Script handles columns A-Z automatically
- For columns beyond Z (AA, AB, etc.), manual adjustment may be needed
