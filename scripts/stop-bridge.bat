@echo off
rem 脚本在 scripts/ 下，切到项目根
cd /d "%~dp0.."
set FOUND=0
rem ---- tell the supervisor loop to exit (otherwise it would restart node) ----
echo. > bridge.stop
for /f "tokens=5" %%a in ('netstat -ano ^| findstr /C:":8765" ^| findstr /C:"LISTENING"') do (
    echo [wecom-bridge] Stopping pid=%%a
    taskkill /f /pid %%a >nul 2>&1
    set FOUND=1
)
if %FOUND%==0 echo [wecom-bridge] Not running.
echo [wecom-bridge] Done.
timeout /t 3 >nul
