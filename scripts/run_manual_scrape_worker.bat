@echo off
REM From repo: npf-scraper-webapp (this script lives in scripts\)
cd /d "%~dp0.."
if "%REDIS_URL%"=="" set REDIS_URL=redis://127.0.0.1:6379/0
if "%MANUAL_SCRAPE_RQ_QUEUE%"=="" set MANUAL_SCRAPE_RQ_QUEUE=manual_scrape
set PYTHONPATH=%CD%
REM Windows: rq worker uses SIGALRM for timeouts — use TimerDeathPenalty worker instead.
python -m scrapers.manual_rq_worker
if errorlevel 1 pause
