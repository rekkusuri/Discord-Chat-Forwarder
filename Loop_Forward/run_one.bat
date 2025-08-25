@echo off
setlocal EnableExtensions

REM Usage (but we can auto-default some):
REM run_one.bat "<CHANNEL_ID>" "<WEBHOOK_URL>" "<STATE_PATH>" "<EXPORTER_EXE>" "<BOT_TOKEN>" "<EXPORT_ROOT>" "<PROGRESS_PATH>"

if "%~3"=="" (
  echo Usage: run_one.bat "CHANNEL_ID" "WEBHOOK_URL" "STATE_PATH" "EXPORTER_EXE" "BOT_TOKEN" "EXPORT_ROOT" "PROGRESS_PATH"
  exit /b 2
)

set "CHANNEL_ID=%~1"
set "DEST_WEBHOOK_URL=%~2"
set "STATE_PATH=%~3"
set "EXPORTER_EXE=%~4"
set "BOT_TOKEN=%~5"
set "EXPORT_ROOT=%~6"
set "PROGRESS_PATH=%~7"

REM Defaults if not provided
if "%EXPORT_ROOT%"=="" set "EXPORT_ROOT=%~dp0export"
if "%PROGRESS_PATH%"=="" set "PROGRESS_PATH=%~dp0progress_%CHANNEL_ID%.json"

echo [DEBUG] CHANNEL_ID=%CHANNEL_ID%
echo [DEBUG] WEBHOOK=**** (hidden)
echo [DEBUG] STATE=%STATE_PATH%
echo [DEBUG] EXPORTER_EXE=%EXPORTER_EXE%
echo [DEBUG] BOT_TOKEN=**** (hidden)
echo [DEBUG] EXPORT_ROOT=%EXPORT_ROOT%
echo [DEBUG] PROGRESS=%PROGRESS_PATH%

python "%~dp0orchestrate_one.py" ^
  --channel-id "%CHANNEL_ID%" ^
  --webhook "%DEST_WEBHOOK_URL%" ^
  --exporter-exe "%EXPORTER_EXE%" ^
  --bot-token "%BOT_TOKEN%" ^
  --export-root "%EXPORT_ROOT%" ^
  --state "%STATE_PATH%" ^
  --progress "%PROGRESS_PATH%" ^
  --window-min 21 ^
  --overlap-min 1 ^
  --retention 100

endlocal & exit /b 0
