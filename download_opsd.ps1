$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

$release = 'https://data.open-power-system-data.org/time_series/2020-10-06'
$metadataPath = Join-Path $PSScriptRoot 'data\metadata\datapackage.json'
$rawPath = Join-Path $PSScriptRoot 'data\raw\time_series_60min_singleindex.csv'
New-Item -ItemType Directory -Force -Path (Split-Path $metadataPath), (Split-Path $rawPath) | Out-Null

if (-not (Test-Path -LiteralPath $metadataPath)) {
    & curl.exe --location --fail --retry 5 --retry-all-errors --retry-delay 3 --connect-timeout 30 --output $metadataPath "$release/datapackage.json"
    if ($LASTEXITCODE -ne 0) { throw 'Could not download OPSD metadata.' }
}
$metadata = Get-Content -LiteralPath $metadataPath -Raw | ConvertFrom-Json
$resource = $metadata.resources | Where-Object { $_.path -eq 'time_series_60min_singleindex.csv' } | Select-Object -First 1
if ($null -eq $resource -or -not $resource.hash -or -not $resource.bytes) { throw 'CSV hash or size is missing from OPSD metadata.' }

$valid = $false
if (Test-Path -LiteralPath $rawPath) {
    $file = Get-Item -LiteralPath $rawPath
    if ($file.Length -eq [long]$resource.bytes) {
        $valid = ((Get-FileHash -LiteralPath $rawPath -Algorithm SHA256).Hash -ieq $resource.hash)
    }
}
if (-not $valid) {
    & curl.exe --location --fail --retry 5 --retry-all-errors --retry-delay 3 --connect-timeout 30 --output $rawPath "$release/time_series_60min_singleindex.csv"
    if ($LASTEXITCODE -ne 0) { throw 'Could not download OPSD CSV.' }
}
$file = Get-Item -LiteralPath $rawPath
$hash = (Get-FileHash -LiteralPath $rawPath -Algorithm SHA256).Hash
if ($file.Length -ne [long]$resource.bytes -or $hash -ine $resource.hash) {
    throw "OPSD CSV verification failed. Size=$($file.Length); SHA256=$hash"
}
Write-Host "OPSD CSV VERIFIED: $($file.Length) bytes; SHA256=$hash" -ForegroundColor Green
