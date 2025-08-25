@echo off
setlocal ENABLEDELAYEDEXPANSION

REM ========= User-config =========
set "PY=%~dp0venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

set "EXPORTER=%~dp0..\DiscordChatExporterCli\DiscordChatExporter.Cli.exe"
set "FORWARDER=%~dp0forward_once.py"
set "EXPORTER_WRAPPER=%~dp0export_once.py"

REM Provide your secrets here or via env before running.
set "DISCORD_TOKEN=YOUR_DISCORD_TOKEN_HERE"
set "CHANNEL_ID=YOUR_CHANNEL_ID_HERE"
set "WEBHOOK_URL=YOUR_WEBHOOK_URL_HERE"

REM Options
set "EXPORT_DIR=exports"
set "STATE_DIR=state"
set "MAX_ATTACH_MB=7.8"
set "MAX_FILES_PER_POST=8"
set "EDGE_OVERLAP_SECONDS=60"
set "VERBOSE=1"
set "DRY_RUN="  REM set to 1 for dry-run
REM =================================

if "%DISCORD_TOKEN%"=="" (
  echo [error] DISCORD_TOKEN not set
  exit /b 2
)
if "%CHANNEL_ID%"=="" (
  echo [error] CHANNEL_ID not set
  exit /b 2
)
if "%WEBHOOK_URL%"=="" (
  echo [error] WEBHOOK_URL not set
  exit /b 2
)

if not exist "%EXPORTER%" (
  echo [error] DiscordChatExporter not found: %EXPORTER%
  exit /b 2
)

REM Redacted echo (do not show secrets)
echo [info] Running export+forward for channel=%CHANNEL_ID%

set "CMD_ARGS=--token *** --channel %CHANNEL_ID% --webhook *** --export-dir %EXPORT_DIR% --state-dir %STATE_DIR% --exporter-path %EXPORTER% --forwarder-path %FORWARDER% --max-attach-mb %MAX_ATTACH_MB% --max-files-per-post %MAX_FILES_PER_POST% --edge-overlap-seconds %EDGE_OVERLAP_SECONDS%"

if defined VERBOSE if "%VERBOSE%"=="1" set "CMD_ARGS=%CMD_ARGS% --verbose"
if defined DRY_RUN if "%DRY_RUN%"=="1" set "CMD_ARGS=%CMD_ARGS% --dry-run"

REM Real call (with real token & webhook)
set "REAL_ARGS=--token %DISCORD_TOKEN% --channel %CHANNEL_ID% --webhook %WEBHOOK_URL% --export-dir %EXPORT_DIR% --state-dir %STATE_DIR% --exporter-path %EXPORTER% --forwarder-path %FORWARDER% --max-attach-mb %MAX_ATTACH_MB% --max-files-per-post %MAX_FILES_PER_POST% --edge-overlap-seconds %EDGE_OVERLAP_SECONDS%"
if defined VERBOSE if "%VERBOSE%"=="1" set "REAL_ARGS=%REAL_ARGS% --verbose"
if defined DRY_RUN if "%DRY_RUN%"=="1" set "REAL_ARGS=%REAL_ARGS% --dry-run"

echo [info] python %EXPORTER_WRAPPER% %CMD_ARGS%
"%PY%" "%EXPORTER_WRAPPER%" %REAL_ARGS%
set EC=%ERRORLEVEL%
if not "%EC%"=="0" (
  echo [error] export+forward failed with code %EC%
  exit /b %EC%
)

echo [done] Completed.
exit /b 0
