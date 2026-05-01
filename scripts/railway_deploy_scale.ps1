param(
  [string]$Service = "",
  [string]$Environment = "",
  [string]$Region = "europe-west4",
  [int]$Replicas = 4,
  [switch]$SkipDeploy
)

$ErrorActionPreference = "Stop"

$railway = Get-Command railway -ErrorAction SilentlyContinue
if (-not $railway) {
  throw "Railway CLI is not installed. Install with: npm install -g @railway/cli"
}

$whoami = ""
try {
  $whoami = (railway whoami 2>$null | Out-String).Trim()
} catch {
  $whoami = ""
}

if ([string]::IsNullOrWhiteSpace($whoami)) {
  throw "Not logged in. Run: railway login"
}

if (-not $SkipDeploy) {
  if ([string]::IsNullOrWhiteSpace($Service) -and [string]::IsNullOrWhiteSpace($Environment)) {
    Write-Host "[railway] deploying linked service/environment..."
    railway up
  } elseif ([string]::IsNullOrWhiteSpace($Environment)) {
    Write-Host "[railway] deploying service=$Service"
    railway up --service $Service
  } else {
    Write-Host "[railway] deploying service=$Service environment=$Environment"
    railway up --service $Service --environment $Environment
  }
}

$scaleArg = "--{0}={1}" -f $Region, $Replicas

if ([string]::IsNullOrWhiteSpace($Service) -and [string]::IsNullOrWhiteSpace($Environment)) {
  Write-Host ("[railway] scaling linked service with {0}" -f $scaleArg)
  railway scale $scaleArg
} elseif ([string]::IsNullOrWhiteSpace($Environment)) {
  Write-Host ("[railway] scaling service={0} with {1}" -f $Service, $scaleArg)
  railway scale --service $Service $scaleArg
} else {
  Write-Host ("[railway] scaling service={0} environment={1} with {2}" -f $Service, $Environment, $scaleArg)
  railway scale --service $Service --environment $Environment $scaleArg
}

Write-Host "[railway] done"
