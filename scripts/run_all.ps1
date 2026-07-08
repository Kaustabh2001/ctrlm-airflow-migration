# run_all.ps1 — the whole pipeline in one command, from a bare clone to a pushed repo.
# Windows PowerShell 5.1 compatible (no && / || pipeline chain operators).
#
#   1. Bootstrap : create venv + install dependencies if missing/incomplete.
#   2. Validate  : the input directory must contain .xml Control-M exports.
#   3. Convert   : run BOTH partitioning strategies (DAGs + IR + reports per scope).
#   4. Dashboard : build the offline comparison dashboard.
#   5. Push      : commit tracked changes and push to origin (skip with -SkipPush).
#
# Usage (from anywhere):
#   powershell -ExecutionPolicy Bypass -File scripts\run_all.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\run_all.ps1 -Inputs my\exports -SkipPush

param(
    [string]$Inputs = "examples\exports",
    [string]$OutRoot = "output",
    [switch]$SkipPush
)

$ErrorActionPreference = "Stop"

function Fail([string]$Message) {
    Write-Host "ERROR: $Message" -ForegroundColor Red
    Pop-Location -ErrorAction SilentlyContinue
    exit 1
}

# Repo root = parent of this script's directory.
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot

# ---------------------------------------------------------------- 1. venv
$Python = Join-Path $RepoRoot "venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    Write-Host "[1/5] venv not found - creating and installing dependencies..." -ForegroundColor Cyan
    python -m venv venv
    if ($LASTEXITCODE -ne 0) { Fail "could not create venv (is python 3.10+ on PATH?)" }
    & $Python -m pip install --upgrade pip --quiet
    & $Python -m pip install -r requirements.txt --quiet
    & $Python -m pip install -e core --quiet
    if ($LASTEXITCODE -ne 0) { Fail "dependency installation failed" }
} else {
    Write-Host "[1/5] venv found - checking dependencies..." -ForegroundColor Cyan
    & $Python -c "import pydantic, jinja2, networkx, pyvis, yaml, black, ctrlm_core"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "      dependencies incomplete - installing..."
        & $Python -m pip install -r requirements.txt --quiet
        & $Python -m pip install -e core --quiet
        if ($LASTEXITCODE -ne 0) { Fail "dependency installation failed" }
    }
}

# ---------------------------------------------------------------- 2. inputs
if (-not (Test-Path $Inputs)) { Fail "input directory '$Inputs' does not exist" }
$XmlFiles = @(Get-ChildItem -Path $Inputs -Filter *.xml -File)
if ($XmlFiles.Count -eq 0) { Fail "no .xml files in '$Inputs' - nothing to convert" }
Write-Host "[2/5] found $($XmlFiles.Count) XML export(s) in $Inputs" -ForegroundColor Cyan

# ---------------------------------------------------------------- 3. convert
Write-Host "[3/5] converting - strategy A: components..." -ForegroundColor Cyan
& $Python "strategy_components\run.py" $Inputs -o (Join-Path $OutRoot "components")
if ($LASTEXITCODE -ne 0) { Fail "components strategy failed" }

Write-Host "      converting - strategy B: single-entry..." -ForegroundColor Cyan
& $Python "strategy_single_entry\run.py" $Inputs -o (Join-Path $OutRoot "single_entry")
if ($LASTEXITCODE -ne 0) { Fail "single-entry strategy failed" }

# ---------------------------------------------------------------- 4. dashboard
Write-Host "[4/5] building dashboard..." -ForegroundColor Cyan
& $Python "dashboard\build.py" --a (Join-Path $OutRoot "components") --b (Join-Path $OutRoot "single_entry") -o (Join-Path $OutRoot "dashboard\index.html")
if ($LASTEXITCODE -ne 0) { Fail "dashboard build failed" }
Write-Host "      dashboard: $(Join-Path $RepoRoot (Join-Path $OutRoot 'dashboard\index.html'))"

# ---------------------------------------------------------------- 5. push
if ($SkipPush) {
    Write-Host "[5/5] push skipped (-SkipPush)" -ForegroundColor Yellow
} else {
    $Changes = git status --porcelain
    if ($Changes) {
        Write-Host "[5/5] committing tracked changes and pushing..." -ForegroundColor Cyan
        git add -A
        git commit -m "run_all: convert $($XmlFiles.Count) export(s) from $Inputs"
        if ($LASTEXITCODE -ne 0) { Fail "git commit failed" }
    } else {
        Write-Host "[5/5] working tree clean - nothing to commit" -ForegroundColor Cyan
    }
    git push
    if ($LASTEXITCODE -ne 0) { Fail "git push failed (no remote, or auth needed?)" }
}

Write-Host ""
Write-Host "DONE." -ForegroundColor Green
Pop-Location
