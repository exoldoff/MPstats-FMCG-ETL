@echo off
setlocal EnableExtensions

set "ROOT_DIR=%~dp0"
set "VENV_DIR=%ROOT_DIR%.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"

pushd "%ROOT_DIR%" >nul 2>nul
if errorlevel 1 (
  echo [MPStats] Cannot open project directory: %ROOT_DIR%
  pause
  exit /b 1
)

if not exist "web\dist\index.html" (
  echo [MPStats] Missing web\dist\index.html.
  echo [MPStats] This Windows launcher does not require Node.js, but web\dist must be committed or built on another computer.
  pause
  exit /b 1
)

if not exist "%VENV_PY%" (
  echo [MPStats] Creating local Python virtual environment...
  call :create_venv
  if errorlevel 1 (
    echo [MPStats] Failed to create .venv. Install Python 3.10+ and try again.
    pause
    exit /b 1
  )
)

"%VENV_PY%" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>nul
if errorlevel 1 (
  echo [MPStats] Python 3.10+ is required.
  pause
  exit /b 1
)

"%VENV_PY%" -c "import duckdb, fastapi, numpy, openpyxl, pandas, requests, uvicorn" >nul 2>nul
if errorlevel 1 (
  echo [MPStats] Installing Python dependencies into .venv...
  "%VENV_PY%" -m pip install -r requirements.txt
  if errorlevel 1 (
    echo [MPStats] Failed to install dependencies.
    pause
    exit /b 1
  )
)

"%VENV_PY%" -c "import duckdb, fastapi, numpy, openpyxl, pandas, requests, uvicorn" >nul 2>nul
if errorlevel 1 (
  echo [MPStats] Dependencies are still incomplete after install.
  pause
  exit /b 1
)

"%VENV_PY%" "scripts\launch_local_app_windows.py"
set "EXIT_CODE=%ERRORLEVEL%"

popd >nul 2>nul
exit /b %EXIT_CODE%

:create_venv
if defined PYTHON_BIN (
  "%PYTHON_BIN%" -m venv "%VENV_DIR%"
  exit /b %ERRORLEVEL%
)
py -3 -m venv "%VENV_DIR%" >nul 2>nul
if not errorlevel 1 (
  exit /b 0
)
python -m venv "%VENV_DIR%" >nul 2>nul
if not errorlevel 1 (
  exit /b 0
)
exit /b 1
