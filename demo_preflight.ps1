$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Read-Json([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { throw "Missing evidence: $Path" }
    return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}

$quality = Read-Json '.\outputs\quality\quality_summary.json'
$etl = Read-Json '.\outputs\quality\etl_summary.json'
$analytics = Read-Json '.\outputs\analytics\summary.json'
$trends = Read-Json '.\outputs\trends\summary.json'
$ml = Read-Json '.\outputs\ml_forecast\summary.json'
$stream = Read-Json '.\outputs\streaming\checks.json'
$streamSource = Read-Json (Join-Path -Path (Join-Path -Path '.\outputs\streaming\runs' -ChildPath $stream.run_id) -ChildPath 'source_events.json')
$planALineage = ($etl.run_id -eq $quality.etl_run_id -and
    $etl.run_id -eq $analytics.etl_run_id)
$planBLineage = ($trends.etl_run_id -eq $ml.etl_run_id)
$sameSource = ($etl.source_sha256 -eq $analytics.source_sha256 -and
    $etl.source_sha256 -eq $trends.source_sha256 -and
    $etl.source_sha256 -eq $ml.source_sha256)
$streamLineage = ($streamSource.etl_run_id -eq $etl.run_id -and
    $streamSource.ml_run_id -eq $ml.run_id -and
    $streamSource.source_sha256 -eq $etl.source_sha256)

Write-Host 'PROJECT 4: READ-ONLY DEMO PREFLIGHT' -ForegroundColor Cyan
[pscustomobject]@{
    Plan_A_ETL = $etl.status
    Plan_A_quality = $quality.overall_status
    Plan_A_SQL = $analytics.status
    Plan_B_trends = $trends.status
    Plan_B_ML = $ml.status
    Plan_C_streaming = $stream.status
    Plan_A_same_ETL_run = $planALineage
    Plan_B_same_ETL_run = $planBLineage
    A_and_B_same_ETL_run = ($etl.run_id -eq $trends.etl_run_id)
    A_and_B_same_source_SHA256 = $sameSource
    Plan_C_same_ETL_and_ML_run = $streamLineage
    Stream_unique_hours = $stream.unique_hdfs_rows
} | Format-List

if ($etl.status -ne 'SUCCESS' -or $quality.overall_status -ne 'PASSED' -or
    $analytics.status -ne 'PASSED' -or $trends.status -ne 'PASSED' -or
    $ml.status -ne 'PASSED' -or $stream.status -ne 'PASSED' -or
    -not $planALineage -or -not $planBLineage -or -not $sameSource -or -not $streamLineage) {
    throw 'At least one evidence status or lineage check failed.'
}
Write-Host 'EVIDENCE PREFLIGHT PASSED' -ForegroundColor Green
if ($etl.run_id -ne $trends.etl_run_id) {
    Write-Warning 'Plan A and Plan B evidence came from different ETL runs of the same source CSV. Keep this distinction clear in the demo.'
}
Write-Host 'Docker services (expect namenode, datanode, spark):' -ForegroundColor Cyan
& docker compose ps
if ($LASTEXITCODE -ne 0) {
    Write-Warning 'Docker is unavailable; start Docker Desktop before the live demo.'
} else {
    $running = @(& docker compose ps --status running --services)
    $composeExit = $LASTEXITCODE
    $runningServices = @($running | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $missingServices = @('namenode','datanode','spark') | Where-Object { $_ -notin $runningServices }
    if ($composeExit -ne 0 -or $missingServices.Count -gt 0) {
        Write-Warning 'The three project services are not all running; run docker compose up -d before the live demo.'
    }
}
