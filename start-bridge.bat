@echo off
cd /d "%~dp0"

rem ---- clear stale stop flag so the supervisor will start ----
if exist bridge.stop del bridge.stop >nul 2>&1

rem ---- single instance guard: port 8765 already listening ----
netstat -ano | findstr /C:":8765" | findstr /C:"LISTENING" >nul
if %errorlevel%==0 (
    echo [wecom-bridge] Already running. Nothing to do.
    timeout /t 2 >nul
    exit /b 0
)

rem ---- rotate log when over 5MB ----
if exist bridge.log for %%F in (bridge.log) do if %%~zF GTR 5242880 move /y bridge.log bridge.log.old >nul

rem ---- windowless start: no console window will remain. To stop: stop-bridge.bat ----
echo [wecom-bridge] Starting in background (windowless)...
wscript.exe run-hidden.vbs "%~dp0."
timeout /t 4 >nul
netstat -ano | findstr /C:":8765" | findstr /C:"LISTENING" >nul
if %errorlevel%==0 (
    echo [wecom-bridge] Started OK. Subscribed to WeCom long connection.
) else (
    echo [wecom-bridge] Not listening yet - check bridge.log for errors.
)
timeout /t 2 >nul
