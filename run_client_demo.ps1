param(
    [string]$Source = "data/demo_trades.csv"
)

$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

Write-Host "[1/2] Resetting demo environment..."
python run_toolbox.py demo-reset
if ($LASTEXITCODE -ne 0) {
    throw "demo-reset failed with exit code $LASTEXITCODE"
}

Write-Host "[2/2] Running demo pipeline..."
python run_toolbox.py demo-run --source $Source
if ($LASTEXITCODE -ne 0) {
    throw "demo-run failed with exit code $LASTEXITCODE"
}

Write-Host "Client demo completed successfully."
