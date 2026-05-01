param(
  [string]$EnvFile = ".env",
  [int]$InstanceCount = 4,
  [int]$BasePort = 8001,
  [int]$ProxyPort = 8000
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

$python = "c:/Users/nejco/Desktop/Scraper/.venv/Scripts/python.exe"
$startedPids = @()
$upstreams = @()

for ($i = 0; $i -lt $InstanceCount; $i++) {
  $port = $BasePort + $i
  $upstreams += ("http://127.0.0.1:{0}" -f $port)
  $args = @("-m", "uvicorn", "backend.app:app", "--host", "127.0.0.1", "--port", "$port", "--log-level", "warning", "--no-access-log")
  $p = Start-Process -FilePath $python -ArgumentList $args -PassThru -WindowStyle Hidden
  $startedPids += $p.Id
}

$upstreamArg = ($upstreams -join ",")
$proxyArgs = @("benchmarks/rr_proxy.py", "--listen-host", "127.0.0.1", "--listen-port", "$ProxyPort", "--upstreams", $upstreamArg)
$proxyProc = Start-Process -FilePath $python -ArgumentList $proxyArgs -PassThru -WindowStyle Hidden
$startedPids += $proxyProc.Id

if (!(Test-Path "runtime")) {
  New-Item -ItemType Directory -Path "runtime" | Out-Null
}

@{
  started_at = (Get-Date).ToString("o")
  instance_count = $InstanceCount
  proxy_port = $ProxyPort
  upstreams = $upstreams
  pids = $startedPids
} | ConvertTo-Json -Depth 4 | Set-Content -Path "runtime/scale_sim_state.json" -Encoding UTF8

Write-Host ("[scale-sim] started {0} instances (single-process each); proxy on :{1}" -f $InstanceCount, $ProxyPort)
Write-Host "[scale-sim] state file: runtime/scale_sim_state.json"
