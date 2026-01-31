# Копирует env.ready в .env (для локального запуска Docker)
$ErrorActionPreference = "Stop"
$src = Join-Path $PSScriptRoot "env.ready"
$dst = Join-Path $PSScriptRoot ".env"
if (-not (Test-Path $src)) { Write-Error "Файл env.ready не найден." }
Copy-Item -Path $src -Destination $dst -Force
Write-Host "Готово: создан файл .env" -ForegroundColor Green
