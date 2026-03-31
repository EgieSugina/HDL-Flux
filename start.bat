@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "ROOT=%~dp0"
set "VENV=%ROOT%.venv"
set "REQ=%ROOT%requirements.txt"
set "MAIN=%ROOT%serve.py"

where python >nul 2>&1
if errorlevel 1 (
  echo Error: python not found. Install Python 3.10+ and add it to PATH.
  exit /b 1
)

if not exist "%VENV%\Scripts\python.exe" (
  echo [start] Creating virtual environment at .venv …
  python -m venv "%VENV%"
  if errorlevel 1 (
    echo Error: failed to create venv.
    exit /b 1
  )
)

call "%VENV%\Scripts\activate.bat"
if errorlevel 1 (
  echo Error: failed to activate venv.
  exit /b 1
)

if not exist "%REQ%" (
  echo Error: requirements.txt not found in %ROOT%
  exit /b 1
)

echo [start] Installing / checking dependencies …
python -m pip install --upgrade pip setuptools wheel -q
python -m pip install -q -r "%REQ%"
if errorlevel 1 (
  echo Error: pip install failed.
  exit /b 1
)

if not exist "%MAIN%" (
  echo Error: serve.py not found in %ROOT%
  exit /b 1
)

echo [start] Launching serve.py …
python "%MAIN%" %*

endlocal
