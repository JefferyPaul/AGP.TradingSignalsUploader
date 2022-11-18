chcp 936
@echo off
echo ============================================
echo.

cd %~dp0
cd venv/Scripts
call activate.bat


python ../../draw_signal.py
pause