param([switch]$SkipEtl, [switch]$NoOpen)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
Write-Host 'PLAN A - Prepare verified data and analyze electricity load' -ForegroundColor Cyan
Write-Host 'Input: OPSD hourly CSV, Germany 2019.'
Write-Host 'Output: verified HDFS Parquet, SQL tables, and the Plan A dashboard.'
function Invoke-Step([string[]]$DockerArgs) {
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Plan A stopped. Docker exit code: $LASTEXITCODE" }
}
New-Item -ItemType Directory -Force -Path '.\outputs\analytics', '.\outputs\trends' | Out-Null
$pendingPage = '<meta charset="utf-8"><h1>Plan A is running</h1><p>Wait for PLAN A COMPLETED in PowerShell.</p>'
Set-Content -LiteralPath '.\outputs\analytics\index.html' -Encoding UTF8 -Value $pendingPage
Set-Content -LiteralPath '.\outputs\trends\index.html' -Encoding UTF8 -Value $pendingPage
Write-Host '[A0/6] Start Docker: NameNode, DataNode, Spark' -ForegroundColor Cyan
Invoke-Step @('compose','up','-d')
if (-not $SkipEtl) {
    Write-Host '[A1/6] ETL: read OPSD CSV, select Germany 2019, write HDFS Parquet' -ForegroundColor Cyan
    Invoke-Step @('exec','spark','/opt/spark/bin/spark-submit','--master','local[2]','/workspace/jobs/etl.py')
} else {
    Write-Host '[A1/6] Reuse existing HDFS Parquet and ETL summary' -ForegroundColor Cyan
}
Write-Host '[A2/6] Verify HDFS readback, then compute Spark SQL aggregates' -ForegroundColor Cyan
Invoke-Step @('exec','spark','/opt/spark/bin/spark-submit','--master','local[2]','/workspace/jobs/analytics.py')
Write-Host '[A3/6] Cross-check SQL aggregates against the source CSV' -ForegroundColor Cyan
Invoke-Step @('exec','spark','python3','/workspace/tests/check_analytics.py')
Write-Host '[A4/6] Analyze trends: 7- and 30-day averages and weekday-hour patterns' -ForegroundColor Cyan
Invoke-Step @('exec','spark','/opt/spark/bin/spark-submit','--master','local[2]','/workspace/jobs/trend_analysis.py')
Write-Host '[A5/6] Independently check trend windows and heatmap groups' -ForegroundColor Cyan
Invoke-Step @('exec','spark','python3','/workspace/tests/check_trends.py')
Write-Host '[A6/6] Render the trend dashboard and SQL detail page' -ForegroundColor Cyan
Invoke-Step @('exec','spark','python3','/workspace/jobs/render_dashboard.py')
Invoke-Step @('exec','spark','python3','/workspace/jobs/render_trends.py')
Write-Host 'PLAN A COMPLETED: outputs/trends/index.html' -ForegroundColor Green
Write-Host 'Review hourly/monthly averages, weekday vs weekend, MA7/MA30, and heatmaps.'
Write-Host 'NEXT: .\run_plan_b.ps1' -ForegroundColor Yellow
if (-not $NoOpen) { Start-Process -FilePath (Join-Path $PSScriptRoot 'outputs\trends\index.html') }
