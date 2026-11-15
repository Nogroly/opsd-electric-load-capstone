param([switch]$NoOpen)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Invoke-Docker([string[]]$Arguments) {
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Plan B stopped; Docker exit code ${LASTEXITCODE}: docker $($Arguments -join ' ')"
    }
}

New-Item -ItemType Directory -Force -Path '.\outputs\trends', '.\outputs\ml_forecast' | Out-Null
Write-Host '[B0/6] Start Docker: HDFS and Spark' -ForegroundColor Cyan
Invoke-Docker -Arguments @('compose', 'up', '-d')

Write-Host '[1/6] Analyze within-2019 trends from HDFS' -ForegroundColor Cyan
Set-Content -LiteralPath '.\outputs\trends\index.html' -Encoding UTF8 -Value '<meta charset="utf-8"><h1>Trend analysis running</h1><p>Wait for PLAN B COMPLETED.</p>'
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/trend_analysis.py')

Write-Host '[2/6] Independently check trend tables' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/tests/check_trends.py')
Write-Host '[3/6] Render trend dashboard' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/jobs/render_trends.py')

Write-Host '[4/6] Train and evaluate Random Forest on HDFS' -ForegroundColor Cyan
Set-Content -LiteralPath '.\outputs\ml_forecast\index.html' -Encoding UTF8 -Value '<meta charset="utf-8"><h1>ML forecast running</h1><p>Wait for PLAN B COMPLETED.</p>'
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/ml_forecast.py')

Write-Host '[5/6] Independently check ML targets and metrics' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/tests/check_ml_forecast.py')
Write-Host '[6/6] Render ML dashboard' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/jobs/render_ml_forecast.py')

Write-Host 'PLAN B COMPLETED: outputs/trends/index.html and outputs/ml_forecast/index.html' -ForegroundColor Green
if (-not $NoOpen) {
    Start-Process -FilePath (Join-Path $PSScriptRoot 'outputs\trends\index.html')
    Start-Process -FilePath (Join-Path $PSScriptRoot 'outputs\ml_forecast\index.html')
}
