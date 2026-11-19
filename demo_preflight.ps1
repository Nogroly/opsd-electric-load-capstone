param([switch]$CoreOnly)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Read-Json([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { throw "Missing evidence: $Path" }
    return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}

$quality = Read-Json '.\outputs\quality\quality_summary.json'
$etl = Read-Json '.\outputs\quality\etl_summary.json'
$analytics = Read-Json '.\outputs\analytics\summary.json'
$analyticsProof = Read-Json '.\outputs\analytics\verification.json'
$trends = Read-Json '.\outputs\trends\summary.json'
$trendsProof = Read-Json '.\outputs\trends\verification.json'
$baseline = Read-Json '.\outputs\forecast\summary.json'
$baselineProof = Read-Json '.\outputs\forecast\verification.json'
$ml = Read-Json '.\outputs\ml_forecast\summary.json'
$mlProof = Read-Json '.\outputs\ml_forecast\verification.json'
$planALineage = ($etl.run_id -eq $quality.etl_run_id -and
    $etl.run_id -eq $analytics.etl_run_id -and $etl.run_id -eq $trends.etl_run_id)
$planBLineage = ($etl.run_id -eq $baseline.etl_run_id -and $etl.run_id -eq $ml.etl_run_id)
$baselineLineage = ($baseline.run_id -eq $baselineProof.forecast_run_id -and
    $baseline.run_id -eq $ml.baseline_run_id)
$planAProofPassed = ($analyticsProof.status -eq 'PASSED' -and $trendsProof.status -eq 'PASSED' -and
    $analyticsProof.analytics_run_id -eq $analytics.run_id -and $trendsProof.trend_run_id -eq $trends.run_id)
$mlProofPassed = ($mlProof.status -eq 'PASSED' -and $mlProof.ml_run_id -eq $ml.run_id)
$sameSource = ($etl.source_sha256 -match '^[a-f0-9]{64}$' -and
    $etl.source_sha256 -eq $analytics.source_sha256 -and
    $etl.source_sha256 -eq $trends.source_sha256 -and
    $etl.source_sha256 -eq $baseline.source_sha256 -and
    $etl.source_sha256 -eq $ml.source_sha256)

Write-Host 'PROJECT 4: READ-ONLY DEMO PREFLIGHT' -ForegroundColor Cyan
$evidence = [ordered]@{
    Plan_A_ETL = $etl.status
    Plan_A_quality = $quality.overall_status
    Plan_A_SQL = $analyticsProof.status
    Plan_A_trends = $trendsProof.status
    Plan_B_baselines = $baselineProof.status
    Plan_B_ML = $mlProof.status
    Plan_A_same_ETL_run = $planALineage
    Plan_B_same_ETL_run = $planBLineage
    Plan_B_same_baseline_run = $baselineLineage
    A_and_B_same_source_SHA256 = $sameSource
}
$streamPassed = $true
if (-not $CoreOnly) {
    $stream = Read-Json '.\outputs\streaming\checks.json'
    $streamRun = Join-Path '.\outputs\streaming\runs' $stream.run_id
    $streamSource = Read-Json (Join-Path $streamRun 'source_events.json')
    $streamLineage = ($streamSource.etl_run_id -eq $etl.run_id -and
        $streamSource.ml_run_id -eq $ml.run_id -and
        $streamSource.source_sha256 -eq $etl.source_sha256)
    $evidence['Plan_C_streaming'] = $stream.status
    $evidence['Plan_C_same_ETL_and_ML_run'] = $streamLineage
    $evidence['Stream_unique_hours'] = $stream.unique_hdfs_rows
    $streamPassed = ($stream.status -eq 'PASSED' -and $streamLineage)
}
[pscustomobject]$evidence | Format-List

if ($etl.status -ne 'SUCCESS' -or $quality.overall_status -ne 'PASSED' -or
    $analytics.status -ne 'PASSED' -or $trends.status -ne 'PASSED' -or
    $baseline.status -ne 'PASSED' -or $baselineProof.status -ne 'PASSED' -or $ml.status -ne 'PASSED' -or
    -not $planALineage -or -not $planBLineage -or -not $baselineLineage -or
    -not $planAProofPassed -or -not $mlProofPassed -or -not $sameSource -or -not $streamPassed) {
    throw 'At least one evidence status or lineage check failed.'
}
Write-Host 'EVIDENCE PREFLIGHT PASSED' -ForegroundColor Green
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
