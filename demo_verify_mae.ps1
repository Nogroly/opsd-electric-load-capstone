$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

$predictions = @(Import-Csv -LiteralPath '.\outputs\ml_forecast\predictions.csv' |
    Where-Object { $_.split -eq 'test' })
$metric = Import-Csv -LiteralPath '.\outputs\ml_forecast\metrics.csv' |
    Where-Object { $_.split -eq 'test' -and $_.period -eq 'all' -and $_.model -eq 'random_forest' } |
    Select-Object -First 1
if ($predictions.Count -ne 2208 -or $null -eq $metric) {
    throw 'Expected 2208 test targets and one Random Forest metric row.'
}

$first = $predictions[0]
$actual = [double]::Parse($first.actual_mw, [Globalization.CultureInfo]::InvariantCulture)
$forecast = [double]::Parse($first.random_forest_mw, [Globalization.CultureInfo]::InvariantCulture)
$firstError = [Math]::Abs($forecast - $actual)
$sum = 0.0
foreach ($row in $predictions) {
    $y = [double]::Parse($row.actual_mw, [Globalization.CultureInfo]::InvariantCulture)
    $yhat = [double]::Parse($row.random_forest_mw, [Globalization.CultureInfo]::InvariantCulture)
    $sum += [Math]::Abs($yhat - $y)
}
$mae = $sum / $predictions.Count
$reported = [double]::Parse($metric.mae_mw, [Globalization.CultureInfo]::InvariantCulture)
$gap = [Math]::Abs($mae - $reported)

Write-Host 'MAE = sum(abs(forecast - actual)) / number_of_hours' -ForegroundColor Cyan
Write-Host ("Example target UTC: {0}" -f $first.target_utc)
Write-Host ("Actual: {0:N2} MW; forecast: {1:N2} MW; absolute error: {2:N2} MW" -f $actual,$forecast,$firstError)
Write-Host ("All test hours: {0}; calculated MAE: {1:N4} MW; report MAE: {2:N4} MW" -f $predictions.Count,$mae,$reported)
if ($gap -gt 0.000001) { throw "MAE does not match report; absolute difference: $gap" }
Write-Host 'MAE CROSS-CHECK PASSED' -ForegroundColor Green
