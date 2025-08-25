@echo off
setlocal EnableExtensions

REM === GLOBAL CONFIG ===
set "EXPORTER_EXE=PATH_TO_EXPORTER"
set "BOT_TOKEN=BOT_TOKEN"
set "EXPORT_ROOT=%~dp0export"
set "SLEEP_SECS=360"  REM 10 minutes
REM =====================

if not exist "%EXPORT_ROOT%" mkdir "%EXPORT_ROOT%"

REM === CHANNEL CONFIG ===
REM Channel A
set "CHAN_A_ID=1003508251545042998"
set "CHAN_A_WEBHOOK=https://discord.com/api/webhooks/1407523493121359873/jLZEN0wb2mDrK6C-C6CeEeF31udFSHRpCDkvq4rYl9AyeC8G7bJWU41fvokkcHodb4hz"
set "CHAN_A_STATE=%~dp0state_A.json"

REM Channel B
set "CHAN_B_ID=1003508522790694942"
set "CHAN_B_WEBHOOK=https://discord.com/api/webhooks/1407523520421826701/NLSyMQmIVJh1AnnVLo-IjiiMn5ims9SCk3LVcX2Wje-rpPjhTTRRnic77tFmPZqfIa08"
set "CHAN_B_STATE=%~dp0state_B.json"

REM Channel C
set "CHAN_C_ID=936916901056098354"
set "CHAN_C_WEBHOOK=https://discord.com/api/webhooks/1407567339683053618/V8l-o2u2eumy0_KyR4WAUpUsQI35iehbU9c5IM8SLPFKs-y2E7LCPCm6C1BBfdVHngJm"
set "CHAN_C_STATE=%~dp0state_C.json"
REM =======================

:loop

call "%~dp0run_one.bat" "%CHAN_A_ID%" "%CHAN_A_WEBHOOK%" "%CHAN_A_STATE%" "%EXPORTER_EXE%" "%BOT_TOKEN%" "%EXPORT_ROOT%" "%CHAN_A_PROGRESS%"
echo [MASTER] Sleep %SLEEP_SECS%s...
timeout /t %SLEEP_SECS% /nobreak >nul

call "%~dp0run_one.bat" "%CHAN_B_ID%" "%CHAN_B_WEBHOOK%" "%CHAN_B_STATE%" "%EXPORTER_EXE%" "%BOT_TOKEN%" "%EXPORT_ROOT%" "%CHAN_B_PROGRESS%"
echo [MASTER] Sleep %SLEEP_SECS%s...
timeout /t %SLEEP_SECS% /nobreak >nul

call "%~dp0run_one.bat" "%CHAN_C_ID%" "%CHAN_C_WEBHOOK%" "%CHAN_C_STATE%" "%EXPORTER_EXE%" "%BOT_TOKEN%" "%EXPORT_ROOT%" "%CHAN_C_PROGRESS%"
echo [MASTER] Sleep %SLEEP_SECS%s...
timeout /t %SLEEP_SECS% /nobreak >nul

goto :loop