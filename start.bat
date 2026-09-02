@echo off
rem One-click launcher: double-click or run `start.bat` to start backend + frontend via dev.ps1
rem Closing this window (or Ctrl-C) stops both processes.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0dev.ps1" %*
pause
