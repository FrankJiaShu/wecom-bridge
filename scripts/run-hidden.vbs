' Windowless launcher for wecom-bridge (called by start-bridge.bat)
' Usage: wscript.exe run-hidden.vbs "<bridge directory>"
Set sh = CreateObject("WScript.Shell")
If WScript.Arguments.Count > 0 Then sh.CurrentDirectory = WScript.Arguments(0)
sh.Run "cmd /c scripts\bridge-forever.cmd", 0, False
