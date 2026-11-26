@echo off
rem ---- wecom-bridge supervisor ----
rem node 崩溃 5s 后自动重启；bridge.stop 标志存在则退出（stop-bridge.bat / 面板停止写入）
rem 端口已被占用说明另一实例活着：不重复拉起，只守望
cd /d "%~dp0"
if not exist logs mkdir logs
:loop
if exist bridge.stop (
    del bridge.stop >nul 2>&1
    echo [%date% %time%] stop flag found - supervisor exiting >> logs\bridge-crash.log
    exit /b 0
)
netstat -ano | findstr /C:":8765" | findstr /C:"LISTENING" >nul
if %errorlevel%==0 (
    timeout /t 10 /nobreak >nul
    goto loop
)
echo [%date% %time%] starting node >> logs\bridge-crash.log
node wecom-bridge.mjs 2>>logs\bridge-crash.log
echo [%date% %time%] node exited, restarting in 5s >> logs\bridge-crash.log
timeout /t 5 /nobreak >nul
goto loop
