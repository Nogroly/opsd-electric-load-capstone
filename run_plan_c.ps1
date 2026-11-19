param([switch]$NoOpen)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
Write-Host 'PLAN C - Replay historical data through Structured Streaming' -ForegroundColor Cyan
Write-Host 'Input: 24 historical hours from Plan A and the trained model from Plan B.'
Write-Host 'Output: streaming predictions on HDFS, checkpoint, and a replay dashboard.'

function Invoke-Docker([string[]]$Arguments) {
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Plan C stopped; Docker exit code ${LASTEXITCODE}: docker $($Arguments -join ' ')"
    }
}

$runId = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff')
New-Item -ItemType Directory -Force -Path '.\outputs\streaming' | Out-Null
Set-Content -LiteralPath '.\outputs\streaming\index.html' -Encoding UTF8 -Value '<meta charset="utf-8"><meta http-equiv="refresh" content="5"><h1>Historical streaming replay is starting</h1><p>Wait for the first 8 events.</p>'
Write-Host "PLAN C RUN ID: $runId" -ForegroundColor Cyan
Write-Host '[C0/8] Start Docker: HDFS and Spark' -ForegroundColor Cyan
Invoke-Docker -Arguments @('compose', 'up', '-d')

Write-Host '[C1/8] Prepare 24 historical hours from HDFS' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/prepare_replay.py', '--run-id', $runId)

$opened = $false
$stageNumber = 1
foreach ($stage in @(@('stage08', '8'), @('stage16', '16'), @('stage24', '24'))) {
    $stageNumber++
    $name = $stage[0]
    $through = $stage[1]
    Write-Host "[C$stageNumber/8] Publish and process through $through hours" -ForegroundColor Cyan
    Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/jobs/replay_events.py', '--run-id', $runId, '--through', $through)
    Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/stream_forecast.py', '--run-id', $runId, '--stage', $name)
    Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/stream_snapshot.py', '--run-id', $runId, '--stage', $name)
    Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/jobs/render_streaming.py', '--run-id', $runId)
    if (-not $NoOpen -and -not $opened) {
        Start-Process -FilePath (Join-Path $PSScriptRoot 'outputs\streaming\index.html')
        $opened = $true
    }
}

Write-Host '[C5/8] Restart same streaming query and checkpoint' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/stream_forecast.py', '--run-id', $runId, '--stage', 'restart')
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/stream_snapshot.py', '--run-id', $runId, '--stage', 'restart')
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/jobs/render_streaming.py', '--run-id', $runId)

Write-Host '[C6/8] Publish a duplicate timestamp and process it' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/jobs/replay_events.py', '--run-id', $runId, '--duplicate')
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/stream_forecast.py', '--run-id', $runId, '--stage', 'duplicate')
Invoke-Docker -Arguments @('exec', 'spark', '/opt/spark/bin/spark-submit', '--master', 'local[2]', '/workspace/jobs/stream_snapshot.py', '--run-id', $runId, '--stage', 'duplicate')

Write-Host '[C7/8] Independently check all five stages' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/tests/check_streaming.py', '--run-id', $runId)
Write-Host '[C8/8] Render final dashboard and report' -ForegroundColor Cyan
Invoke-Docker -Arguments @('exec', 'spark', 'python3', '/workspace/jobs/render_streaming.py', '--run-id', $runId)
Write-Host "PLAN C COMPLETED: $runId; outputs/streaming/index.html" -ForegroundColor Green
Write-Host 'Review stages 8 -> 16 -> 24, checkpoint restart, and duplicate handling.'
Write-Host 'FINAL CHECK: .\demo_preflight.ps1' -ForegroundColor Yellow
