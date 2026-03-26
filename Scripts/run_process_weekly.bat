@echo off
echo ========================================
echo Weekly Loading Slip Processor
echo ========================================
echo.

cd /d "%~dp0"

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Run with default file (Week 59)
echo Processing Week 59 loading slip...
echo.
python process_weekly_loading_slip.py --file "Week 59 loading slip 2026 test.xlsx"

if errorlevel 1 (
    echo.
    echo ERROR: Script failed.
    pause
    exit /b 1
)

echo.
echo ========================================
echo Processing complete!
echo ========================================
pause
