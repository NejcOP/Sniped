param(
  [string]$EnvFile = ".env",
  [string]$BindHost = "127.0.0.1",
  [int]$Port = 8000,
  [int]$Workers = 0
)

$ErrorActionPreference = "Stop"

if (!(Test-Path $EnvFile)) {
  throw "Missing $EnvFile. Create it from .env.example first."
}

Get-Content $EnvFile | ForEach-Object {
  $line = $_.Trim()
  if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) { return }
  $parts = $line -split "=", 2
  if ($parts.Length -eq 2) {
    $k = $parts[0].Trim()
    $v = $parts[1].Trim().Trim('"').Trim("'")
    Set-Item -Path ("Env:" + $k) -Value $v
  }
}

if ($Workers -le 0) {
  $rawWorkers = [string]$env:WEB_CONCURRENCY
  if ([string]::IsNullOrWhiteSpace($rawWorkers)) {
    $Workers = 4
  } else {
    $Workers = [Math]::Max(1, [int]$rawWorkers)
  }
}

$python = "c:/Users/nejco/Desktop/Scraper/.venv/Scripts/python.exe"
Write-Host ("[backend] starting uvicorn with workers={0} host={1} port={2}" -f $Workers, $BindHost, $Port)
& $python -m uvicorn backend.app:app --host $BindHost --port $Port --workers $Workers --log-level warning --no-access-log
