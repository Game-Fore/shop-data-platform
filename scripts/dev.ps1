# Типовые команды для Windows, те же, что в Makefile: .\scripts\dev.ps1 <команда>
param([string]$Task = "help")

$ErrorActionPreference = "Stop"
# PowerShell 7.3+: ошибка внешней программы тоже останавливает скрипт
$PSNativeCommandUseErrorActionPreference = $true
Set-Location (Join-Path $PSScriptRoot "..")

$py = if (Test-Path .\.venv\Scripts\python.exe) { ".\.venv\Scripts\python.exe" } else { "python" }
$allProfiles = "--profile", "bi", "--profile", "orch", "--profile", "obs", "--profile", "tools"

function Ensure-Env { if (-not (Test-Path .env)) { Copy-Item .env.example .env } }

$tasks = [ordered]@{
    reset       = @("стереть данные и поднять стенд с коннектором", { Ensure-Env; .\reset.ps1 })
    down        = @("остановить стенд, данные сохраняются (продолжить: up)", { docker compose @allProfiles stop })
    up          = @("запустить основной стенд", { Ensure-Env; docker compose up -d --wait })
    consumer    = @("consumer: Kafka -> ClickHouse (занимает терминал)", { & $py consumer\consumer.py })
    backfill    = @("история за 180 дней, consumer должен работать", { & $py generator.py --backfill-days 180 })
    verify      = @("сверка Postgres и ClickHouse", { & $py verify.py })
    rates       = @("загрузить курсы ЦБ", { docker compose run --rm --entrypoint python dbt /include/cbr_rates.py })
    dbt         = @("собрать витрины и прогнать тесты dbt", {
        docker compose --profile tools build dbt
        docker compose run --rm dbt deps
        docker compose run --rm dbt build
    })
    docs        = @("документация dbt: http://localhost:8081", {
        docker compose run --rm dbt docs generate
        docker compose run --rm --service-ports dbt docs serve --host 0.0.0.0 --port 8081
    })
    bi          = @("Superset: http://localhost:8088", { docker compose --profile bi up -d --wait superset })
    orch        = @("Airflow: http://localhost:8082", { docker compose --profile orch up -d --wait airflow-api-server })
    obs         = @("Prometheus: http://localhost:9090, дашборд: http://localhost:3000/d/cdc-consumer", {
        docker compose --profile obs up -d --wait prometheus
    })
    lint        = @("линтер", { & $py -m ruff check . })
    test        = @("unit-тесты, сервисы не нужны", { & $py -m pytest })
    integration = @("полный прогон на настоящем стенде, СТИРАЕТ данные (нужен Git Bash)", {
        $env:PYTHON = (Resolve-Path $py).Path -replace '\\', '/'
        # именно Git Bash: просто "bash" в Windows находит bash.exe из WSL, который без дистрибутива не работает
        # git.exe лежит в разных подкаталогах установки (cmd, ucrt64\bin): ищем вверх каталог с bin\bash.exe
        $gitRoot = Split-Path (Get-Command git).Source
        while ($gitRoot -and -not (Test-Path (Join-Path $gitRoot "bin\bash.exe"))) { $gitRoot = Split-Path $gitRoot }
        if (-not $gitRoot) { throw "Не найден Git Bash рядом с git" }
        & (Join-Path $gitRoot "bin\bash.exe") scripts/integration_test.sh
    })
}

if ($Task -eq "help" -or -not $tasks.Contains($Task)) {
    if ($Task -ne "help") { Write-Host "Неизвестная команда: $Task`n" }
    $tasks.GetEnumerator() | ForEach-Object { "{0,-13} {1}" -f $_.Key, $_.Value[0] }
    if ($Task -ne "help") { exit 1 }
    return
}
& $tasks[$Task][1]
