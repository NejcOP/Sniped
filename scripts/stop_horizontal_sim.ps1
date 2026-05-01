$ErrorActionPreference = "Stop"

$statePath = "runtime/scale_sim_state.json"
if (!(Test-Path $statePath)) {
  Write-Host "[scale-sim] no state file found"
  exit 0
}

$state = Get-Content $statePath -Raw | ConvertFrom-Json
foreach ($pid in $state.pids) {
  try {
    Stop-Process -Id ([int]$pid) -Force -ErrorAction Stop
  } catch {
    # Process may already be gone.
  }
}

Remove-Item $statePath -Force
Write-Host "[scale-sim] stopped"
