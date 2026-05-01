param(
  [string]$HostUrl,
  [int[]]$UserStages = @(500, 1000),
  [int]$SpawnRate = 80,
  [string]$RunTime = "90s",
  [string]$OutDir = "benchmarks/results/production"
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($HostUrl)) {
  throw "HostUrl is required, e.g. https://your-app.up.railway.app"
}

$scriptPath = ".\benchmarks\run_locust_ramp.ps1"
if (!(Test-Path $scriptPath)) {
  throw "Missing $scriptPath"
}

Write-Host ("[bench] running production benchmark against {0}" -f $HostUrl)
& $scriptPath -HostUrl $HostUrl -UserStages $UserStages -SpawnRate $SpawnRate -RunTime $RunTime -OutDir $OutDir
