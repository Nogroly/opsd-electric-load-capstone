param([switch]$NoOpen)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
Write-Host 'PLAN B - Compare three one-hour-ahead forecasting methods' -ForegroundColor Cyan
Write-Host 'Input: the verified HDFS Parquet created by Plan A.'
Write-Host 'Output: two baselines, a trained Random Forest, and one comparison dashboard.'

function Invoke-Docker([string[]]$Arguments) {
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Plan B stopped; Docker exit code ${LASTEXITCODE}: docker $($Arguments -join ' ')"
    }
}

New-Item -ItemType Directory -Force -Path '.\outputs\ml_forecast' | Out-Null
Set-Content -LiteralPath '.\outputs\ml_forecast\index.html' -Encoding UTF8 -Value '<meta charset="utf-8"><h1>Plan B is running</h1><p>Wait for PLAN B COMPLETED in PowerShell.</p>'
Write-Host '[B0/5] Start Docker: HDFS and Spark' -ForegroundColor Cyan
Invoke-Docker -Arguments @('compose', 'up', '-d')

Write-Host '[B1/5] Compute last-hour and 168-hour baselines from HDFS' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/forecast_baselines.py')
Write-Host '[B2/5] Cross-check baseline target hours and errors against the source CSV' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/tests/check_forecast.py')
Write-Host '[B3/5] Train Random Forest and score all three models on the same hours' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/ml_forecast.py')
Write-Host '[B4/5] Independently check ML targets and error metrics' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/tests/check_ml_forecast.py')
Write-Host '[B5/5] Render one dashboard for Random Forest and both baselines' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/jobs/render_ml_forecast.py')

Write-Host 'PLAN B COMPLETED: outputs/ml_forecast/index.html' -ForegroundColor Green
Write-Host 'Review MAE/RMSE on the same 2208 test hours.'
Write-Host 'NEXT: .\run_plan_c.ps1' -ForegroundColor Yellow
if (-not $NoOpen) {
    Start-Process -FilePath (Join-Path $PSScriptRoot 'outputs\ml_forecast\index.html')
}
