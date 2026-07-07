# run_all.ps1 — run both partition strategies and build the comparison dashboard.
# Windows PowerShell 5.1 compatible (no && / || pipeline chain operators).
#
# Usage (from anywhere):
#   powershell -ExecutionPolicy Bypass -File scripts\run_all.ps1
#   powershell -File scripts\run_all.ps1 -Inputs examples\exports -OutRoot output

param(
    [string]$Inputs = "examples\exports",
    [string]$OutRoot = "output"
)

$ErrorActionPreference = "Stop"

# Repo root = parent of this script's directory; python = repo venv.
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $RepoRoot "venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    Write-Error "venv python not found at $Python"
    exit 1
}

Push-Location $RepoRoot
try {
    Write-Host "== Strategy A: components ==" -ForegroundColor Cyan
    & $Python "strategy_components\run.py" $Inputs -o (Join-Path $OutRoot "components")
    if ($LASTEXITCODE -ne 0) { Write-Error "components strategy failed"; exit 1 }

    Write-Host "== Strategy B: single-entry ==" -ForegroundColor Cyan
    & $Python "strategy_single_entry\run.py" $Inputs -o (Join-Path $OutRoot "single_entry")
    if ($LASTEXITCODE -ne 0) { Write-Error "single-entry strategy failed"; exit 1 }

    Write-Host "== Dashboard ==" -ForegroundColor Cyan
    & $Python "dashboard\build.py" --a (Join-Path $OutRoot "components") --b (Join-Path $OutRoot "single_entry") -o (Join-Path $OutRoot "dashboard\index.html")
    if ($LASTEXITCODE -ne 0) { Write-Error "dashboard build failed"; exit 1 }

    Write-Host ""
    Write-Host "Done. Open $(Join-Path $OutRoot 'dashboard\index.html') in a browser." -ForegroundColor Green
}
finally {
    Pop-Location
}
