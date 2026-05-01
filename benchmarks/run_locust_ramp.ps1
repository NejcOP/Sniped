param(
  [string]$HostUrl = "http://127.0.0.1:8000",
  [int[]]$UserStages = @(100, 250, 500, 1000, 2000, 5000),
  [int]$SpawnRate = 100,
  [string]$RunTime = "90s",
  [string]$AuthToken = "",
  [string]$AuthTokenFile = "benchmarks/results/loadtest_token.txt",
  [string]$OutDir = "benchmarks/results"
)

$ErrorActionPreference = "Stop"

if (!(Test-Path $OutDir)) {
  New-Item -Path $OutDir -ItemType Directory | Out-Null
}

$python = "c:/Users/nejco/Desktop/Scraper/.venv/Scripts/python.exe"

if ([string]::IsNullOrWhiteSpace($AuthToken) -and (Test-Path $AuthTokenFile)) {
  $AuthToken = (Get-Content $AuthTokenFile -Raw).Trim()
}

if ([string]::IsNullOrWhiteSpace($AuthToken) -and $env:LOCUST_AUTH_TOKEN) {
  $AuthToken = [string]$env:LOCUST_AUTH_TOKEN
}

if ([string]::IsNullOrWhiteSpace($AuthToken)) {
  Write-Warning "LOCUST_AUTH_TOKEN is empty. Authenticated endpoints may return 401."
}

foreach ($users in $UserStages) {
  $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $prefix = Join-Path $OutDir ("locust_u{0}_{1}" -f $users, $stamp)

  Write-Host ("[locust] running users={0} spawn={1} runtime={2}" -f $users, $SpawnRate, $RunTime)

  $env:LOCUST_AUTH_TOKEN = $AuthToken
  & $python -m locust -f "benchmarks/locustfile.py" --headless --host $HostUrl --users $users --spawn-rate $SpawnRate --run-time $RunTime --csv $prefix --only-summary

  Write-Host ("[locust] output prefix: {0}" -f $prefix)
}

Write-Host "[locust] ramp complete"
