@echo off
setlocal
cd /d "%~dp0"

py -3 app.py

echo.
echo Press any key to close this window...
pause >nul
