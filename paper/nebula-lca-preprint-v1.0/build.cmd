@echo off
setlocal
set "PAPER_DIR=%~dp0"
set "CASE_ROOT=%~1"
if "%CASE_ROOT%"=="" set "CASE_ROOT=%PAPER_DIR%..\..\..\tmp\refinery-paper-cases\20260831T135829Z"

where quarto >nul 2>nul
if errorlevel 1 (
  echo Quarto is required on PATH. Install Quarto and TinyTeX, then rerun.
  exit /b 1
)

python "%PAPER_DIR%scripts\build_validation_evidence.py" --case-root "%CASE_ROOT%" --output-dir "%PAPER_DIR%data" || exit /b 1
python "%PAPER_DIR%scripts\build_method_figures.py" --output-dir "%PAPER_DIR%figures" || exit /b 1
python "%PAPER_DIR%scripts\build_public_case_package.py" --case-root "%CASE_ROOT%" --validation-dir "%PAPER_DIR%data" --output-dir "%PAPER_DIR%benchmark-package" || exit /b 1

pushd "%PAPER_DIR%"
quarto render paper.qmd --to pdf || exit /b 1
quarto render supporting_information.qmd --to pdf || exit /b 1
if not exist output mkdir output
copy /y paper.pdf output\Nebula-LCA-Preprint-v1.0.pdf >nul
copy /y supporting_information.pdf output\Nebula-LCA-Preprint-v1.0-Supporting-Information.pdf >nul
popd
echo Build completed.
