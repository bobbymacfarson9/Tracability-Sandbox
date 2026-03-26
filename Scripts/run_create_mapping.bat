@echo off
echo ========================================
echo Week 60 SKU Mapping Table Generator
echo ========================================
echo.

cd /d "%~dp0"

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python from https://www.python.org/downloads/
    pause
    exit /b 1
)

REM Check if required packages are installed
python -c "import pandas, openpyxl" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install pandas openpyxl
    if errorlevel 1 (
        echo ERROR: Failed to install required packages
        pause
        exit /b 1
    )
)

REM Run the script
echo Running mapping table generator...
echo.
python create_week60_mapping.py

if errorlevel 1 (
    echo.
    echo ERROR: Script failed. Check error messages above.
    pause
    exit /b 1
)

echo.
echo ========================================
echo Script completed successfully!
echo ========================================
pause
