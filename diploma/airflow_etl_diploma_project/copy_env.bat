@echo off
copy /Y "%~dp0env.ready" "%~dp0.env" >nul
if %errorlevel% neq 0 (echo Ошибка копирования & exit /b 1)
echo Готово: создан файл .env
