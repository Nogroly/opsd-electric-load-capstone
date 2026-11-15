param([switch]$SkipEtl, [switch]$NoOpen)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
function Invoke-Step([string[]]$DockerArgs) {
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Plan A stopped. Docker exit code: $LASTEXITCODE" }
}
New-Item -ItemType Directory -Force -Path '.\outputs\analytics' | Out-Null
Set-Content -LiteralPath '.\outputs\analytics\index.html' -Encoding UTF8 -Value '<meta charset="utf-8"><h1>Plan A đang chạy</h1><p>Chỉ xem kết quả khi PowerShell báo PLAN A COMPLETED. Nếu job lỗi, kiểm tra log và chạy lại.</p>'
Write-Host '[A0/4] Start Docker: NameNode, DataNode, Spark' -ForegroundColor Cyan
Invoke-Step @('compose','up','-d')
if (-not $SkipEtl) {
    Write-Host '[A1/4] ETL: read OPSD CSV, select Germany 2019, write HDFS Parquet' -ForegroundColor Cyan
    Invoke-Step @('exec','spark','/opt/spark/bin/spark-submit','--master','local[2]','/workspace/jobs/etl.py')
} else {
    Write-Host '[A1/4] ETL skipped: read existing verified HDFS Parquet' -ForegroundColor Cyan
}
Write-Host '[A2/4] Verify HDFS readback, then Spark SQL aggregates' -ForegroundColor Cyan
Invoke-Step @('exec','spark','/opt/spark/bin/spark-submit','--master','local[2]','/workspace/jobs/analytics.py')
Write-Host '[A3/4] Verify: compare analytics with source and quality evidence' -ForegroundColor Cyan
Invoke-Step @('exec','spark','python3','/workspace/tests/check_analytics.py')
Write-Host '[A4/4] Render: create the local HTML dashboard' -ForegroundColor Cyan
Invoke-Step @('exec','spark','python3','/workspace/jobs/render_dashboard.py')
Write-Host 'PLAN A COMPLETED: outputs/analytics/index.html'
if (-not $NoOpen) { Start-Process -FilePath (Join-Path $PSScriptRoot 'outputs\analytics\index.html') }
