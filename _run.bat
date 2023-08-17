chcp 936
@echo off
echo ============================================
echo.

cd %~dp0
cd venv/Scripts
call activate.bat
start "Cffex" python ../../TradingSignalsUploader/main.py


exit