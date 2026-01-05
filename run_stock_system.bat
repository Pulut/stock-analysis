@echo off
setlocal

echo [1/3] Checking port 8501...

:: Find and kill process on port 8501
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8501 ^| findstr LISTENING') do (
    echo Cleaning up port 8501 PID: %%a ...
    taskkill /F /PID %%a >nul 2>&1
)

echo [2/3] Starting service in background...

:: Create a temporary VBS script to launch hidden
echo Set WshShell = CreateObject("WScript.Shell") > launcher.vbs
echo WshShell.Run "python -m streamlit run dashboard.py", 0 >> launcher.vbs

:: Execute the VBS script
cscript //nologo launcher.vbs

:: Clean up
del launcher.vbs

echo Waiting for service to start...
timeout /t 5 /nobreak >nul

echo [3/3] Opening browser...
start http://localhost:8501

echo.
echo ==========================================
echo  System Started Successfully!
echo  The service is running in the background.
echo  To stop it later, use Task Manager to kill 'python.exe'.
echo ==========================================
echo.
pause