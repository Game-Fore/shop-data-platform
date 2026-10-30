# Полный сброс стенда: удаляет контейнеры и тома, поднимает стек заново
# и регистрирует Debezium-коннектор.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path .env)) {
    throw "Нет файла .env. Создайте его: Copy-Item .env.example .env и задайте пароли."
}

# все профили: иначе запущенный Superset держит сеть, и она не удалится
docker compose --profile bi --profile tools down -v
docker compose up -d --wait

# В JSON пароль не хранится: ${env:POSTGRES_PASSWORD} подставляет сам Kafka Connect
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors `
  -ContentType "application/json" -InFile .\connectors\postgres-shop.json | Out-Null

Start-Sleep -Seconds 5
Invoke-RestMethod http://localhost:8083/connectors/shop-connector/status | ConvertTo-Json -Depth 5
