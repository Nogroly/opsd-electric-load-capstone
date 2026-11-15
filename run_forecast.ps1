param([switch]$NoOpen)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
function Invoke-Step([string[]]$DockerArgs) {
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Forecast stopped. Docker exit code: $LASTEXITCODE" }
}
New-Item -ItemType Directory -Force -Path '.\outputs\forecast' | Out-Null
Set-Content -LiteralPath '.\outputs\forecast\index.html' -Encoding UTF8 -Value '<meta charset="utf-8"><h1>Forecast running</h1><p>Wait for FORECAST COMPLETED. Check the terminal if a step failed.</p>'
Write-Host '[F0/4] Start Docker services' -ForegroundColor Cyan
Invoke-Step @('compose','up','-d')
Write-Host '[F1/4] Check forecast reporting packages inside Spark' -ForegroundColor Cyan
Invoke-Step @('exec','spark','python3','-m','pip','install','--disable-pip-version-check','-r','/workspace/requirements-forecast.txt')
Write-Host '[F2/4] Baselines: forecast from the last hour and exactly 168 hours earlier' -ForegroundColor Cyan
Invoke-Step @('exec','spark','/opt/spark/bin/spark-submit','--master','local[2]','/workspace/jobs/forecast_baselines.py')
Write-Host '[F3/4] Verify predictions and MAE/RMSE against source data' -ForegroundColor Cyan
Invoke-Step @('exec','spark','python3','/workspace/tests/check_forecast.py')
Write-Host '[F4/4] Render the baseline forecast dashboard' -ForegroundColor Cyan
Invoke-Step @('exec','spark','python3','/workspace/jobs/render_forecast.py')
Write-Host 'FORECAST COMPLETED: outputs/forecast/index.html'
if (-not $NoOpen) { Start-Process -FilePath (Join-Path $PSScriptRoot 'outputs\forecast\index.html') }
