@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

REM --- Args ---
REM 1: CHANNEL_ID (required)
REM 2: WEBHOOK_URL (required, quote it)
REM 3: DISCORD_TOKEN (required, quote it)
REM 4: SINCE (optional, YYYY-MM-DD or ISO)
REM 5: UNTIL (optional, YYYY-MM-DD or ISO)

if "%~1"=="" (
  echo Usage: %~nx0 CHANNEL_ID "WEBHOOK_URL" "DISCORD_TOKEN" [SINCE] [UNTIL]
  exit /b 1
)

set "CHANNEL_ID=%~1"
set "WEBHOOK_URL=%~2"
set "DISCORD_TOKEN=%~3"
set "SINCE=%~4"
set "UNTIL=%~5"

REM --- Paths (edit if needed) ---
set "EXPORTER_EXE=PATH_TO_EXPORTER"
set "EXPORT_DIR=exports"
set "STATE_DIR=state"

REM --- Build Python call ---
set "PYCALL=export_once.py --channel %CHANNEL_ID% --webhook "%WEBHOOK_URL%" --token "%DISCORD_TOKEN%" --export-dir "%EXPORT_DIR%" --state-dir "%STATE_DIR%" --exporter-path "%EXPORTER_EXE%""

if not "%SINCE%"=="" (
  set "PYCALL=%PYCALL% --since %SINCE%"
)
if not "%UNTIL%"=="" (
  set "PYCALL=%PYCALL% --until %UNTIL%"
)

echo Running: python %PYCALL%
python %PYCALL%
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  echo [error] export_and_forward_once failed with code %RC%
  exit /b %RC%
)

echo [done] One-shot export & forward complete.
exit /b 0
