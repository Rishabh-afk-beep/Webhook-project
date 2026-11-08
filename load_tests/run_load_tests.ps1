param (
    [string]$ApiUrl = "http://localhost:8000",
    [string]$ApiKey = $env:DEMO_API_KEY,
    [int]$Vus = 10,
    [string]$Duration = "30s",
    [string]$Scenario = "event_ingestion"
)

if (-not $ApiKey) {
    Write-Error "Error: ApiKey parameter or DEMO_API_KEY environment variable is required."
    exit 1
}

$ScenarioPath = "load_tests/k6/scenarios/$Scenario.js"
if (-not (Test-Path $ScenarioPath)) {
    Write-Error "Scenario file not found: $ScenarioPath"
    exit 1
}

Write-Host "=========================================================="
Write-Host " Running k6 Load Test Scenario: $Scenario"
Write-Host " API URL:   $ApiUrl"
Write-Host " VUs:       $Vus"
Write-Host " Duration:  $Duration"
Write-Host "=========================================================="

if (Get-Command k6 -ErrorAction SilentlyContinue) {
    k6 run `
        -e API_BASE_URL=$ApiUrl `
        -e API_KEY=$ApiKey `
        -e VUS=$Vus `
        -e DURATION=$Duration `
        $ScenarioPath
} else {
    Write-Host "k6 binary not found on PATH. Running via Docker (grafana/k6)..."
    $DockerApiUrl = $ApiUrl -replace "localhost", "host.docker.internal" -replace "127.0.0.1", "host.docker.internal"
    docker run --rm -i `
        --add-host=host.docker.internal:host-gateway `
        -v "${PWD}:/work" `
        -w /work `
        grafana/k6 run `
        -e "API_BASE_URL=$DockerApiUrl" `
        -e "API_KEY=$ApiKey" `
        -e "VUS=$Vus" `
        -e "DURATION=$Duration" `
        $ScenarioPath
}
