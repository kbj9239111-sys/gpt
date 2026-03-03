@echo off
chcp 65001 > nul
echo Starting Auction Trainer...
python --version > nul 2>&1
if errorlevel 1 (
    echo Python not found. Please install Python 3.
    pause
    exit
)
pip show flask > nul 2>&1
if errorlevel 1 (
    echo Installing Flask...
    pip install flask -q
)
cd /d "%~dp0"
python app.py
pause
