@echo off
REM JobSonar local dashboard launcher
REM Opens http://localhost:8050 and runs the Dash app in this window.
REM Close this window (or press Ctrl+C) to stop the server.

title JobSonar
cd /d "%~dp0"

REM open the browser a few seconds after the server starts booting
start "" cmd /c "timeout /t 5 /nobreak >nul & start http://localhost:8050"

python dashboard\app.py

REM keep window open if the app exits with an error
echo.
echo [JobSonar stopped] Press any key to close...
pause >nul
