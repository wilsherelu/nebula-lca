$ErrorActionPreference = "Stop"

if (-not $env:TIANGONG_OUTPUT_DIR) {
  $env:TIANGONG_OUTPUT_DIR = "exports"
}
if (-not $env:TIANGONG_EF31_DIR) {
  $env:TIANGONG_EF31_DIR = "data/EF3.1"
}

Write-Host "TIANGONG_OUTPUT_DIR=$env:TIANGONG_OUTPUT_DIR"
Write-Host "TIANGONG_EF31_DIR=$env:TIANGONG_EF31_DIR"

py -m uvicorn app.main:app --reload
