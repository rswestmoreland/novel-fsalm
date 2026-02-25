@echo off
REM demo: scale-demo full loop, run twice, and compare the final scale report line.
REM Assumes you run this from the repo root where Cargo.toml is.

setlocal

set "ROOT1=demo_scale_run1"
set "ROOT2=demo_scale_run2"
set "OUT1=%ROOT1%\out.txt"
set "OUT2=%ROOT2%\out.txt"

if exist "%ROOT1%" rmdir /s /q "%ROOT1%"
if exist "%ROOT2%" rmdir /s /q "%ROOT2%"
mkdir "%ROOT1%"
mkdir "%ROOT2%"

REM Optional: tune evidence-stage caps for the demo.
REM set FSA_LM_SCALE_DEMO_EVIDENCE_K=16
REM set FSA_LM_SCALE_DEMO_EVIDENCE_MAX_BYTES=65536
REM set FSA_LM_SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES=8388608

echo Running scale-demo (run1)...
cargo run --quiet --bin fsa_lm -- scale-demo --seed 1 --docs 64 --queries 32 --min_doc_tokens 16 --max_doc_tokens 32 --vocab 1024 --query_tokens 4 --tie_pair 1 --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1 --root "%ROOT1%" --out-file "%OUT1%"

echo Running scale-demo (run2)...
cargo run --quiet --bin fsa_lm -- scale-demo --seed 1 --docs 64 --queries 32 --min_doc_tokens 16 --max_doc_tokens 32 --vocab 1024 --query_tokens 4 --tie_pair 1 --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1 --root "%ROOT2%" --out-file "%OUT2%"

set "LINE1="
set "LINE2="
for /f "usebackq delims=" %%L in (`findstr /B /C:"scale_demo_scale_report_v1" "%OUT1%"`) do set "LINE1=%%L"
for /f "usebackq delims=" %%L in (`findstr /B /C:"scale_demo_scale_report_v1" "%OUT2%"`) do set "LINE2=%%L"

if not defined LINE1 (
 echo ERROR: could not find scale_demo_scale_report_v1 line in %OUT1%
 exit /b 1
)
if not defined LINE2 (
 echo ERROR: could not find scale_demo_scale_report_v1 line in %OUT2%
 exit /b 1
)

echo.
echo Run1: %LINE1%
echo Run2: %LINE2%
echo.

if "%LINE1%"=="%LINE2%" (
 echo OK: scale report lines match.
) else (
 echo ERROR: scale report lines differ.
 exit /b 1
)

endlocal
