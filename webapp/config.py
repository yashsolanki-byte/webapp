"""
Paths and constants for the NPF scraper web app.
Uses project_paths: data/history, data/reference, data/runtime, logs/app, logs/runs.
"""
import os
import sys

# Ensure project root importable as `import project_paths`
_WEBAPP_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.dirname(_WEBAPP_DIR)
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import project_paths as _paths

LOGS_DIR = _paths.LOGS_DIR
LOGS_APP_DIR = _paths.LOGS_APP_DIR
LOGS_RUNS_DIR = _paths.LOGS_RUNS_DIR
LOGS_SCRIPT_DIR = _paths.LOGS_SCRIPT_DIR
SCRAPER_LOG_FILE = _paths.SCRAPER_LOG_FILE
DATA_SCRAPED_DIR = _paths.DATA_SCRAPED_DIR
INSTITUTES_JSON = _paths.INSTITUTES_JSON
URLS_JSON = _paths.URLS_JSON
SCRAPE_LIST_JSON = _paths.SCRAPE_LIST_JSON
SCRAPE_HISTORY_JSON = _paths.SCRAPE_HISTORY_JSON
UPLOAD_HISTORY_JSON = _paths.UPLOAD_HISTORY_JSON
MANUAL_CREDENTIALS_JSON = _paths.MANUAL_CREDENTIALS_JSON
SCRAPE_LIST_MAX_LENGTH = 100

UPLOAD_LOG_FILE = _paths.UPLOAD_LOG_FILE
FEEDBACK_READY_LOG_FILE = _paths.FEEDBACK_READY_LOG_FILE
MANUAL_SCRAPE_LOG_FILE = _paths.MANUAL_SCRAPE_LOG_FILE

TEMPLATES_DIR = _paths.TEMPLATES_DIR
