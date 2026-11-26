@echo off
rem 脚本在 scripts/ 下，切到项目根
cd /d "%~dp0.."

rem ---- clear stale stop flag so the supervisor will start ----
if exist bridge.stop del bridge.stop >nul 2>&1

rem ---- single instance guard: port 8765 already listening ----
netstat -ano | findstr /C:":8765" | findstr /C:"LISTENING" >nul
if %errorlevel%==0 (
    echo [wecom-bridge] Already running. Nothing to do.
    timeout /t 2 >nul
    exit /b 0
)

rem ---- windowless start: no console window will remain. To stop: stop-bridge.bat ----
echo [wecom-bridge] Starting in background (windowless)...
wscript.exe "%~dp0run-hidden.vbs" "%~dp0.."
timeout /t 6 >nul
netstat -ano | findstr /C:":8765" | findstr /C:"LISTENING" >nul
if %errorlevel%==0 (
    echo [wecom-bridge] Started OK. Subscribed to WeCom long connection.
) else (
    echo [wecom-bridge] Not listening yet - check logs\bridge.log for errors.
)
timeout /t 2 >nul
