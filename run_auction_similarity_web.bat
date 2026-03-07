@echo off
cd /d "%~dp0"
py -3 -c "import flask" >nul 2>&1
if errorlevel 1 (
  echo [INFO] Missing dependency: flask
  echo [INFO] Installing flask with pip...
  py -3 -m pip install flask
  if errorlevel 1 (
    echo [ERROR] Failed to install flask. Run: py -3 -m pip install flask
    pause
    exit /b 1
  )
)

py -3 tools\auction_similarity_web\app.py
if errorlevel 1 (
  echo [ERROR] Failed to start auction similarity web.
  pause
)
