import sys
import os
import shutil
import subprocess
import logging
from datetime import datetime as dt_log

# Project root (parent of scrapers/) for logs and shared data/
_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
import project_paths as paths
from scrapers.export_columns import drop_phone_mobile_columns
from scrapers.manual_scrape_errors import ManualScrapeLogicalError, ManualScrapeTransientError
from scrapers.npf_post_login import ensure_campaign_detailed_view

# ======================
# LOGGING SETUP
# ======================
def setup_logging():
    """Setup logging to logs/script/ with timestamped file."""
    if not getattr(sys, "frozen", False):
        paths.ensure_layout_migrated()
    base = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else paths.ROOT
    logs_dir = os.path.join(base, "logs", "script") if getattr(sys, "frozen", False) else paths.LOGS_SCRIPT_DIR
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = dt_log.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(logs_dir, f"script_scraper_{timestamp}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(log_filename, encoding="utf-8")],
        force=True,
    )
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("Script_Scraper - Nopaperforms Data Scraper")
    logger.info(f"Log file: {log_filename}")
    logger.info("=" * 60)
    return logger

logger = setup_logging()

def setup_playwright_for_distribution():
    """Comprehensive Playwright setup for distributed executable"""
    
    # Set Playwright environment variables
    os.environ['PLAYWRIGHT_BROWSERS_PATH'] = '0'  # Disable browser download
    
    # Check if Playwright is installed
    try:
        import playwright
    except ImportError:
        logger.error("Playwright not installed. Please run: pip install playwright")
        return False
    
    if getattr(sys, 'frozen', False):
        # Running as bundled executable
        base_path = sys._MEIPASS
        
        # Try multiple possible browser locations in the bundle
        possible_paths = [
            os.path.join(base_path, 'playwright', 'browsers'),
            os.path.join(base_path, 'ms-playwright'),
            os.path.join(base_path, 'playwright'),
        ]
        
        # Also check if browsers exist in user's AppData
        user_browsers_path = os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'ms-playwright')
        
        # If user doesn't have browsers, copy from bundle
        if not os.path.exists(user_browsers_path):
            for bundled_path in possible_paths:
                if os.path.exists(bundled_path):
                    try:
                        # Remove existing directory if it exists
                        if os.path.exists(user_browsers_path):
                            shutil.rmtree(user_browsers_path)
                        
                        shutil.copytree(bundled_path, user_browsers_path)
                        logger.info(f"Browsers copied from bundle to: {user_browsers_path}")
                        break
                    except Exception as e:
                        logger.warning(f"Could not copy browsers: {e}")
        
        # Set the browser path to user location
        if os.path.exists(user_browsers_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = user_browsers_path
            logger.info(f"Using browsers from: {user_browsers_path}")
    
    return True

def verify_playwright_setup():
    """Verify Playwright is properly set up and browsers are available"""
    try:
        # Try to import and use Playwright
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            # Test Chromium (most common)
            try:
                browser = p.chromium.launch(headless=False)
                browser.close()
                logger.info("Playwright Chromium browser is working")
                return True
            except Exception as e:
                logger.error(f"Chromium browser failed: {e}")
                return False
                
    except Exception as e:
        logger.error(f"Playwright setup verification failed: {e}")
        return False

def install_playwright_browsers():
    """Install Playwright browsers if not available"""
    try:
        logger.info("Installing Playwright browsers... This may take a few minutes.")
        result = subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], 
                              capture_output=True, text=True, timeout=300000)  # 5 minute timeout
        
        if result.returncode == 0:
            logger.info("Playwright browsers installed successfully")
            return True
        else:
            logger.error(f"Failed to install browsers: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("Browser installation timed out")
        return False
    except Exception as e:
        logger.error(f"Error during browser installation: {e}")
        return False

# Initialize Playwright setup
PLAYWRIGHT_AVAILABLE = False
try:
    # First, run our distribution setup
    setup_success = setup_playwright_for_distribution()
    
    if setup_success:
        # Then verify the setup works
        PLAYWRIGHT_AVAILABLE = verify_playwright_setup()
        
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright browsers not available, attempting installation...")
            PLAYWRIGHT_AVAILABLE = install_playwright_browsers()
            
except Exception as e:
    logger.warning(f"Playwright initialization error: {e}")
    PLAYWRIGHT_AVAILABLE = False

import asyncio
import json
import re
import pandas as pd
import threading
import datetime
from datetime import datetime as dt
from types import SimpleNamespace

try:
    import tkinter as tk
    from tkinter import ttk, messagebox
    TKINTER_AVAILABLE = True
except Exception:
    tk = None
    ttk = None
    messagebox = SimpleNamespace(
        showinfo=lambda *a, **k: None,
        showerror=lambda *a, **k: None,
        showwarning=lambda *a, **k: None,
        askyesno=lambda *a, **k: False,
    )
    TKINTER_AVAILABLE = False

# Import Playwright only if available
if PLAYWRIGHT_AVAILABLE:
    from playwright.async_api import async_playwright
else:
    logger.warning("Playwright not available - scraping functionality will be disabled")

# ======================
# TIMEOUT CONFIG (ms) - tune for speed vs reliability
# ======================
TIMEOUT_LOGIN = 12000        # Wait for post-login (was 30000)
TIMEOUT_NAV_AFTER_LOGIN = 8000   # Institute/source dropdowns to appear
TIMEOUT_FETCH_LOGIN = 10000  # Headless fetch: post-login
TIMEOUT_FETCH_NAV = 5000     # Headless fetch: after institute/source
TIMEOUT_TABLE_LOAD = 15000   # Table/leads data to load after Search
TIMEOUT_PAGINATION = 1500    # Between page clicks
TIMEOUT_MANUAL_SELECT = 8000 # Manual institute selection fallback (was 10000)
TIMEOUT_UI_SETTLE = 400      # Short settle after clicks (was 500-800)
TIMEOUT_UI_SHORT = 250       # Minimal settle (was 200-300)

LEADS_LIMIT = 100000         # Default max leads (1 lakh) when env is unset — see manual_scrape_max_leads_limit()


def manual_scrape_max_leads_limit():
    """
    Max Primary Leads count before manual scrape aborts and asks for a smaller date range.
    Env **NPF_MANUAL_SCRAPE_MAX_LEADS**: integer, default 100000. Set to **0** to disable this check.
    """
    raw = (os.getenv("NPF_MANUAL_SCRAPE_MAX_LEADS") or "").strip()
    if not raw:
        return LEADS_LIMIT
    try:
        n = int(raw, 10)
    except ValueError:
        logger.warning("Invalid NPF_MANUAL_SCRAPE_MAX_LEADS=%r — using default %s", raw, LEADS_LIMIT)
        return LEADS_LIMIT
    if n <= 0:
        return None
    return n


class ManualScrapeLeadsLimitExceeded(Exception):
    """Headless/web app: total leads exceeded NPF_MANUAL_SCRAPE_MAX_LEADS (no GUI dialog)."""

    pass


# Web app manual scrape: cooperative stop (checked in pagination / DOM loops).
_web_manual_stop = threading.Event()
_headless_stopped_by_user = False


def request_web_manual_stop():
    """Signal headless manual scrape to stop (no CSV save)."""
    _web_manual_stop.set()


def clear_web_manual_stop():
    global _headless_stopped_by_user
    _web_manual_stop.clear()
    _headless_stopped_by_user = False


def _web_manual_stop_requested() -> bool:
    return _web_manual_stop.is_set()


def was_headless_stopped_by_user() -> bool:
    return _headless_stopped_by_user


def _set_headless_stopped_by_user() -> None:
    global _headless_stopped_by_user
    _headless_stopped_by_user = True


# Retry config for network/missing data
RETRY_MAX_ATTEMPTS = 3       # Max retries for fetch/apply operations
RETRY_DELAY_MS = 2000        # Delay between retries (ms)

# Data files under project data/ (see project_paths.py)
FILTER_CACHE_FILE = paths.FILTER_CACHE_JSON
# Manual-scrape institute autocomplete list (full institutes source)
INSTITUTES_JSON = paths.INSTITUTES_JSON
URLS_JSON = paths.URLS_JSON


def _cache_key(institute, source):
    """Normalized cache key for institute+source."""
    return f"{str(institute or '').strip().lower()}|{str(source or '').strip().lower()}"


def _load_institutes():
    """Load institute names from Institutes.json for autocomplete. Returns list."""
    try:
        if os.path.exists(INSTITUTES_JSON):
            with open(INSTITUTES_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    names = []
                    for row in data:
                        if isinstance(row, dict):
                            name = (row.get("university") or row.get("college") or "").strip()
                            if name:
                                names.append(name)
                        elif isinstance(row, str):
                            s = row.strip()
                            if s:
                                names.append(s)
                    # Preserve order, remove duplicates
                    return list(dict.fromkeys(names))
                if isinstance(data, dict):
                    values = []
                    for v in data.values():
                        if isinstance(v, str) and v.strip():
                            values.append(v.strip())
                    return list(dict.fromkeys(values))
    except Exception as e:
        logger.warning(f"Could not load institutes: {e}")
    return []


def _load_urls():
    """Load publisher URLs from JSON for autocomplete. Returns list."""
    try:
        if os.path.exists(URLS_JSON):
            with open(URLS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else list(data.values()) if isinstance(data, dict) else []
    except Exception as e:
        logger.warning(f"Could not load URLs: {e}")
    return []


def _load_filter_cache():
    """Load filter cache from file. Returns dict."""
    try:
        if os.path.exists(FILTER_CACHE_FILE):
            with open(FILTER_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load filter cache: {e}")
    return {}


def _save_filter_cache(cache):
    """Save filter cache to file."""
    try:
        cache_dir = os.path.dirname(FILTER_CACHE_FILE)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        with open(FILTER_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Could not save filter cache: {e}")


def _is_retryable_error(e):
    """Check if exception is network/timeout related and worth retrying."""
    if e is None:
        return False
    msg = str(e).lower()
    err_type = type(e).__name__
    return (
        "timeout" in msg or "timeout" in err_type.lower()
        or "net::" in msg or "network" in msg
        or "connection" in msg or "econnreset" in msg
        or "econnrefused" in msg or "enotfound" in msg
        or "target closed" in msg or "page closed" in msg
    )


async def _retry_async(async_fn, max_attempts=RETRY_MAX_ATTEMPTS, retry_on_empty=False, empty_check=None):
    """
    Retry an async function on network error or (optionally) empty result.
    empty_check: callable(result) -> bool, True if result is "empty" and should retry.
    """
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = await async_fn()
            if retry_on_empty and empty_check and empty_check(result) and attempt < max_attempts:
                logger.warning(f"Retry {attempt}/{max_attempts}: empty/missing result")
                await asyncio.sleep(RETRY_DELAY_MS / 1000)
                continue
            return result
        except Exception as e:
            last_error = e
            if _is_retryable_error(e) and attempt < max_attempts:
                logger.warning(f"Retry {attempt}/{max_attempts} after {e}")
                await asyncio.sleep(RETRY_DELAY_MS / 1000)
            else:
                raise
    raise last_error


async def _select_institute_option(page, institute_name, timeout_ms=TIMEOUT_MANUAL_SELECT):
    """
    Robust institute selection for ng-select dropdown.
    Handles exact text, partial matches, and spacing/case differences.
    """
    name = (institute_name or "").strip()
    if not name:
        return False
    await page.locator(".ng-input").first.click()
    try:
        await page.get_by_role("option", name=name, exact=True).first.click(timeout=2000)
        return True
    except Exception:
        pass
    try:
        inp = page.locator(".ng-input input").first
        await inp.fill("")
        await inp.type(name, delay=20)
        await page.wait_for_timeout(TIMEOUT_UI_SHORT)
    except Exception:
        pass
    try:
        clicked = await page.evaluate(
            """(target) => {
                const norm = s => (s || "").toLowerCase().replace(/\\s+/g, " ").trim();
                const t = norm(target);
                const labels = Array.from(document.querySelectorAll("ng-dropdown-panel .ng-option .ng-option-label"));
                if (!labels.length) return false;
                let best = labels.find(el => norm(el.textContent) === t);
                if (!best) best = labels.find(el => norm(el.textContent).includes(t));
                if (!best) return false;
                best.click();
                return true;
            }""",
            name,
        )
        if clicked:
            return True
    except Exception:
        pass
    try:
        await page.locator(f"ng-dropdown-panel .ng-option .ng-option-label:has-text('{name}')").first.click(timeout=2000)
        return True
    except Exception:
        return False

# Advanced filters with sub-options: {filter_id: {label, placeholder|label_text, type: "ng-select"|"ng-multiselect"}}
# ng-select: uses .ng-placeholder; ng-multiselect: uses ng-multiselect-dropdown with span text
SUBFILTER_CONFIG = {
    "u_status": {"label": "Lead Status", "placeholder": "Lead Status", "type": "ng-select"},
    "ud_lead_stage": {"label": "Lead Stage", "label_text": "Lead Stage", "type": "ng-multiselect"},
    "u_payment_approved": {"label": "Paid Applications", "placeholder": "Paid Applications", "type": "ng-select"},
    "u_form_initiated": {"label": "Form Initiated", "placeholder": "Form Initiated", "type": "ng-select"},
}


def _get_subfilter_label(fid):
    """Get display label for a subfilter (backward compat)."""
    c = SUBFILTER_CONFIG.get(fid)
    return c.get("label", fid) if isinstance(c, dict) else (c or fid)

class ScraperApp:
    def __init__(self, root=None):
        """root=None for headless (webapp); otherwise Tk root for GUI."""
        self.headless = root is None
        # Webapp Manual scrape: skip Campaigns → Detailed View; desktop GUI keeps it.
        self.skip_campaign_detailed_view = root is None
        self.root = root
        self.status_callback = None
        self.last_output_path = None
        from credential_env import build_gui_credentials_dict

        self.credentials = build_gui_credentials_dict()
        if self.headless:
            self.status_var = type("_", (), {"set": lambda s, m: None})()
            self.start_btn = self.screenshot_btn = self.stop_btn = self.progress = None
            self.scraping = True
            self._scraper_loop = self._scraper_task = None
            self.advanced_filters_resolved = True
            self.selected_advanced_filters = []
            self.selected_subfilter_options = {}
            self.headless_stopped_by_user = False
            self._cancel_check = None
            return
        self.root.title("Nopaperforms Scraper")
        self.setup_ui()

    def setup_ui(self):
        self.root.geometry("420x520")
        main = ttk.Frame(self.root, padding="15")
        main.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # --- Row 0: Title + status ---
        ttk.Label(main, text="Nopaperforms Scraper", font=("Segoe UI", 14, "bold")).grid(row=0, column=0, sticky=tk.W, pady=(0, 12))
        status_txt = "Ready" if PLAYWRIGHT_AVAILABLE else "Install browsers"
        status_fg = "green" if PLAYWRIGHT_AVAILABLE else "red"
        ttk.Label(main, text=status_txt, foreground=status_fg, font=("Segoe UI", 9)).grid(row=0, column=1, sticky=tk.E, pady=(0, 12))

        # --- Step 1: Required (URL, credentials, institute, source) ---
        req_frame = ttk.LabelFrame(main, text="1. Required", padding="8")
        req_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 8))
        ttk.Label(req_frame, text="URL:").grid(row=0, column=0, sticky=tk.W, pady=3)
        self._urls_list = _load_urls()
        self.url_entry = ttk.Combobox(req_frame, width=42, values=self._urls_list or [], state="normal")
        self.url_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=3, padx=(6, 0))
        self.url_entry.set("https://publisher.nopaperforms.com/lead/details")
        ttk.Label(req_frame, text="Credentials:").grid(row=1, column=0, sticky=tk.W, pady=3)
        self.cred_var = tk.StringVar(value="central")
        ttk.Combobox(req_frame, textvariable=self.cred_var, values=list(self.credentials.keys()), state="readonly", width=39).grid(row=1, column=1, sticky=(tk.W, tk.E), pady=3, padx=(6, 0))
        ttk.Label(req_frame, text="Institute:").grid(row=2, column=0, sticky=tk.W, pady=3)
        self._institutes_list = _load_institutes()
        self.institute_entry = ttk.Combobox(req_frame, width=42, values=[], state="normal")
        self.institute_entry.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=3, padx=(6, 0))
        ttk.Label(req_frame, text="Source:").grid(row=3, column=0, sticky=tk.W, pady=3)
        self.source_entry = ttk.Entry(req_frame, width=42)
        self.source_entry.insert(0, "Collegedunia")
        self.source_entry.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=3, padx=(6, 0))
        req_frame.columnconfigure(1, weight=1)

        # --- Step 2: Advanced Filters ---
        af_frame = ttk.LabelFrame(main, text="2. Advanced Filters", padding="8")
        af_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 8))
        self.load_filters_btn = ttk.Button(af_frame, text="Load Filters", command=self.load_advanced_filters)
        self.load_filters_btn.grid(row=0, column=0, padx=(0, 6), pady=2)
        self.skip_filters_btn = ttk.Button(af_frame, text="Skip", command=self._skip_advanced_filters)
        self.skip_filters_btn.grid(row=0, column=1, padx=6, pady=2)
        self.filters_status_var = tk.StringVar(value="(Load or skip to continue)")
        ttk.Label(af_frame, textvariable=self.filters_status_var, foreground="gray", font=("Segoe UI", 8)).grid(row=0, column=2, sticky=tk.W, padx=(8, 0), pady=2)

        # --- Step 3: Options ---
        opt_frame = ttk.LabelFrame(main, text="3. Options", padding="8")
        opt_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 8))

        ttk.Label(opt_frame, text="Instance:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.instance_var = tk.StringVar(value="All")
        ttk.Combobox(opt_frame, textvariable=self.instance_var, values=["All", "Primary", "Secondary", "Tertiary"], state="readonly", width=18).grid(row=0, column=1, sticky=tk.W, pady=2, padx=(6, 12))
        ttk.Label(opt_frame, text="Rows:").grid(row=0, column=2, sticky=tk.W, padx=(8, 0), pady=2)
        self.rows_var = tk.StringVar(value="5000")
        ttk.Combobox(opt_frame, textvariable=self.rows_var, values=["100", "500", "1000", "5000", "10000", "20000"], state="readonly", width=10).grid(row=0, column=3, sticky=tk.W, pady=2, padx=(6, 0))

        ttk.Label(opt_frame, text="Order:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.order_var = tk.StringVar(value="Ascending")
        ttk.Combobox(opt_frame, textvariable=self.order_var, values=["Ascending", "Descending"], state="readonly", width=18).grid(row=1, column=1, sticky=tk.W, pady=2, padx=(6, 12))
        ttk.Label(opt_frame, text="From:").grid(row=1, column=2, sticky=tk.W, padx=(8, 0), pady=2)
        self.from_date_entry = ttk.Entry(opt_frame, width=10)
        self.from_date_entry.grid(row=1, column=3, sticky=tk.W, pady=2, padx=(6, 4))
        ttk.Label(opt_frame, text="To:").grid(row=1, column=4, sticky=tk.W, padx=(4, 0), pady=2)
        self.to_date_entry = ttk.Entry(opt_frame, width=10)
        self.to_date_entry.grid(row=1, column=5, sticky=tk.W, pady=2, padx=(6, 0))

        ttk.Label(opt_frame, text="File:", font=("Segoe UI", 8)).grid(row=2, column=0, sticky=tk.W, pady=2)
        self.file_entry = ttk.Entry(opt_frame, width=42)
        self.file_entry.grid(row=2, column=1, columnspan=5, sticky=(tk.W, tk.E), pady=2, padx=(6, 0))
        self.add_placeholder(self.from_date_entry, "DD-MM-YYYY")
        self.add_placeholder(self.to_date_entry, "DD-MM-YYYY")
        opt_frame.columnconfigure(1, weight=1)

        # --- Status + Progress ---
        self.status_var = tk.StringVar(value="Fill required fields, then load or skip filters.")
        ttk.Label(main, textvariable=self.status_var, font=("Segoe UI", 9), foreground="blue").grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=(8, 4))
        self.progress = ttk.Progressbar(main, mode="indeterminate")
        self.progress.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=4)

        # --- Actions ---
        btn_frame = ttk.Frame(main)
        btn_frame.grid(row=6, column=0, columnspan=2, pady=(12, 0))
        self.start_btn = ttk.Button(btn_frame, text="Start Scraping", command=self.start_scraping, state="disabled")
        self.start_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.screenshot_btn = ttk.Button(btn_frame, text="Take Screenshot", command=self.start_screenshot, state="disabled")
        self.screenshot_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.stop_btn = ttk.Button(btn_frame, text="Stop", command=self.stop_scraping, state="disabled")
        self.stop_btn.pack(side=tk.LEFT, padx=4)
        if not PLAYWRIGHT_AVAILABLE:
            ttk.Button(btn_frame, text="Install Browsers", command=self.install_playwright).pack(side=tk.LEFT, padx=4)

        main.columnconfigure(1, weight=1)
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        self.institute_entry.bind("<KeyRelease>", self._on_institute_keyrelease)
        self.institute_entry.bind("<<ComboboxSelected>>", lambda e: self._update_flow_state())
        self.url_entry.bind("<KeyRelease>", self._on_url_keyrelease)
        self.url_entry.bind("<<ComboboxSelected>>", lambda e: self._update_flow_state())
        self.source_entry.bind("<KeyRelease>", lambda e: self._update_flow_state())
        self.cred_var.trace_add("write", lambda *a: self._update_flow_state())

        self.scraping = False
        self._scraper_loop = None
        self._scraper_task = None
        self.advanced_filters_resolved = False
        self.available_advanced_filters = []
        self.selected_advanced_filters = []
        self.selected_subfilter_options = {}

        self._update_flow_state()

    def _update_flow_state(self):
        """Enable/disable Load Filters and Start based on required fields and filter resolution."""
        url_ok = bool(self.url_entry.get().strip())
        inst_ok = bool(self.institute_entry.get().strip())
        src_ok = bool(self.source_entry.get().strip())
        cred_ok = bool(self.cred_var.get())
        req_ok = url_ok and inst_ok and src_ok and cred_ok

        if req_ok and PLAYWRIGHT_AVAILABLE:
            self.load_filters_btn.config(state="normal")
        else:
            self.load_filters_btn.config(state="disabled")

        start_ok = req_ok and self.advanced_filters_resolved and PLAYWRIGHT_AVAILABLE
        self.start_btn.config(state="normal" if start_ok else "disabled")
        self.screenshot_btn.config(state="normal" if start_ok else "disabled")
        if not req_ok:
            self.status_var.set("Fill URL, Institute, Source. Select credentials.")
        elif not self.advanced_filters_resolved:
            self.status_var.set("Load or skip advanced filters to continue.")
        elif start_ok:
            self.status_var.set("Ready. Set file name and Start.")

    def _skip_advanced_filters(self):
        """User chose to skip advanced filters."""
        self.advanced_filters_resolved = True
        self.filters_status_var.set("Skipped")
        self.status_var.set("Filters skipped. Ready to scrape.")
        self._update_flow_state()

    def install_playwright(self):
        """Install Playwright browsers"""
        response = messagebox.askyesno(
            "Install Playwright", 
            "This will install Playwright browsers (Chromium).\nThis may take a few minutes. Continue?"
        )
        
        if response:
            self.status_var.set("Installing Playwright browsers...")
            self.progress.start()
            
            def install_thread():
                success = install_playwright_browsers()
                
                def update_ui():
                    self.progress.stop()
                    if success:
                        global PLAYWRIGHT_AVAILABLE
                        PLAYWRIGHT_AVAILABLE = True
                        self.status_var.set("✅ Playwright installed successfully! Please restart the application.")
                        self.start_btn.config(state="normal")
                        messagebox.showinfo("Success", "Playwright installed successfully! Please restart the application.")
                    else:
                        self.status_var.set("❌ Failed to install Playwright")
                        messagebox.showerror("Error", "Failed to install Playwright browsers. Please install manually.")
                
                self._schedule( update_ui)
            
            thread = threading.Thread(target=install_thread)
            thread.daemon = True
            thread.start()

    def load_advanced_filters(self):
        """Load advanced filters from platform or cache. Show selection dialog."""
        if not PLAYWRIGHT_AVAILABLE:
            messagebox.showerror("Error", "Playwright is not available.")
            return
        url = self.url_entry.get().strip()
        institute = self.institute_entry.get().strip()
        source = self.source_entry.get().strip()
        if not url:
            messagebox.showerror("Error", "Enter Login URL.")
            return
        if not url.startswith(("http://", "https://")):
            messagebox.showerror("Error", "URL must start with http:// or https://")
            return
        if not institute:
            messagebox.showerror("Error", "Enter Institute name.")
            return
        if not source:
            messagebox.showerror("Error", "Enter Source.")
            return

        cred_key = self.cred_var.get()
        credentials = self.credentials[cred_key]
        params = {
            "login_url": url,
            "email": credentials["email"],
            "password": credentials["password"],
            "institute": institute,
            "source": source,
        }

        # Check cache first
        cache = _load_filter_cache()
        key = _cache_key(institute, source)
        cached = cache.get(key)

        if cached and cached.get("filters"):
            cached_at = cached.get("cached_at", "unknown")
            try:
                cached_at = dt.fromisoformat(cached_at).strftime("%d-%b-%Y %H:%M")
            except Exception:
                pass
            use_cache = messagebox.askyesno(
                "Use Cached Filters?",
                f"Cached filters found for '{institute}' (Source: {source}).\n\n"
                f"Last updated: {cached_at}\n\n"
                f"Use cached filters? (No = fetch fresh from platform)"
            )
            if use_cache:
                self.available_advanced_filters = cached["filters"]
                self.filters_status_var.set(f"{len(cached['filters'])} from cache")
                self.status_var.set("Select filters to apply.")
                self._show_filter_selection_dialog()
                return

        # Fetch from platform
        self.load_filters_btn.config(state="disabled")
        self.progress.start()
        self.status_var.set("Loading advanced filters from platform...")

        def run_fetch():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                filters = loop.run_until_complete(self._fetch_advanced_filters(params))
                if filters:
                    # Save to cache
                    cache = _load_filter_cache()
                    key = _cache_key(params["institute"], params["source"])
                    cache[key] = {
                        "filters": filters,
                        "subfilter_options": cache.get(key, {}).get("subfilter_options", {}),
                        "cached_at": dt.now().isoformat(),
                        "institute": params["institute"],
                        "source": params["source"],
                    }
                    _save_filter_cache(cache)
                    logger.info(f"Cached filters for {params['institute']}")
                self._schedule( lambda: self._on_filters_loaded(filters))
            except Exception as e:
                self._schedule( lambda: self._on_filters_error(str(e)))
            finally:
                loop.close()

        threading.Thread(target=run_fetch, daemon=True).start()

    def _on_filters_loaded(self, filters):
        """Called on main thread when filters are loaded."""
        self.progress.stop()
        if not filters:
            self.status_var.set("No advanced filters found")
            messagebox.showwarning("Filters", "No advanced filters could be read from the platform.")
            self.advanced_filters_resolved = True
            self._update_flow_state()
            return
        self.available_advanced_filters = filters
        self.filters_status_var.set(f"{len(filters)} loaded")
        self.status_var.set("Select filters to apply.")
        self._update_flow_state()
        self._show_filter_selection_dialog()

    def _on_filters_error(self, err_msg):
        """Called on main thread when filter fetch fails."""
        self.progress.stop()
        self._update_flow_state()
        self.status_var.set("Failed to load filters")
        messagebox.showerror("Error", f"Failed to load advanced filters:\n{err_msg}")

    def _load_and_show_subfilter_options(self, subfilter_ids, params):
        """Fetch sub-filter options from platform or cache. Show selection dialog."""
        institute = params.get("institute", "")
        source = params.get("source", "")
        key = _cache_key(institute, source)
        cache = _load_filter_cache()
        cached_sf = cache.get(key, {}).get("subfilter_options", {})

        # Check if cache has all requested subfilter options
        missing = [fid for fid in subfilter_ids if not cached_sf.get(fid)]
        if not missing:
            options_map = {fid: cached_sf[fid] for fid in subfilter_ids if cached_sf[fid]}
            if options_map:
                self.status_var.set("Loaded sub-filter options from cache")
                self._on_subfilter_options_loaded(options_map)
                return

        # Fetch from platform (all or missing)
        ids_to_fetch = missing if missing else subfilter_ids
        self.load_filters_btn.config(state="disabled")
        self.progress.start()
        self.status_var.set("Loading sub-filter options from platform...")

        def run_fetch():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                options_map = loop.run_until_complete(self._fetch_subfilter_options(params, ids_to_fetch))
                # Merge with cache for any we had
                for fid in subfilter_ids:
                    if fid not in options_map and fid in cached_sf:
                        options_map[fid] = cached_sf[fid]
                # Save to cache
                if options_map:
                    cache = _load_filter_cache()
                    if key not in cache:
                        cache[key] = {}
                    if "subfilter_options" not in cache[key]:
                        cache[key]["subfilter_options"] = {}
                    cache[key]["subfilter_options"].update(options_map)
                    _save_filter_cache(cache)
                self._schedule( lambda: self._on_subfilter_options_loaded(options_map))
            except Exception as e:
                self._schedule( lambda: self._on_subfilter_options_error(str(e)))
            finally:
                loop.close()

        threading.Thread(target=run_fetch, daemon=True).start()

    def _on_subfilter_options_loaded(self, options_map):
        """Called when sub-filter options are loaded. options_map: {filter_id: [option1, ...]}."""
        self.progress.stop()
        self._update_flow_state()
        if not options_map:
            self.status_var.set("No sub-filter options found")
            return
        self.status_var.set("Select sub-filter options...")
        self._show_subfilter_selection_dialog(options_map)

    def _on_subfilter_options_error(self, err_msg):
        """Called when sub-filter fetch fails."""
        self.progress.stop()
        self._update_flow_state()
        self.status_var.set("Failed to load sub-filter options")
        messagebox.showerror("Error", f"Failed to load sub-filter options:\n{err_msg}")

    def _show_subfilter_selection_dialog(self, options_map):
        """Show dialog to select options for each sub-filter. options_map: {filter_id: [option1, ...]}."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Select Sub-Filter Options")
        dialog.geometry("450x500")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Choose options for each filter (leave empty = no filter):", font=("Arial", 10, "bold")).pack(anchor=tk.W, padx=10, pady=(10, 5))

        scroll_frame = ttk.Frame(dialog)
        scroll_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        canvas = tk.Canvas(scroll_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(scroll_frame)
        inner = ttk.Frame(canvas)

        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.configure(command=canvas.yview)
        scroll_frame.columnconfigure(0, weight=1)
        scroll_frame.rowconfigure(0, weight=1)
        canvas.grid(row=0, column=0, sticky=(tk.N, tk.S, tk.E, tk.W))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        canvas.create_window((0, 0), window=inner, anchor=tk.NW)

        # options_map: {filter_id: [opt1, opt2, ...]}
        # selected_options: {filter_id: {option: var}}
        selected_options = {}

        for fid, opts in options_map.items():
            if not opts:
                continue
            label = _get_subfilter_label(fid)
            ttk.Label(inner, text=label, font=("Arial", 9, "bold")).pack(anchor=tk.W, pady=(10, 2))
            frame = ttk.Frame(inner)
            frame.pack(anchor=tk.W, padx=15)
            selected_options[fid] = {}
            for opt in opts:
                var = tk.BooleanVar(value=opt in self.selected_subfilter_options.get(fid, []))
                selected_options[fid][opt] = var
                cb = ttk.Checkbutton(frame, text=opt, variable=var)
                cb.pack(anchor=tk.W, pady=1)

        def on_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        inner.bind("<Configure>", on_configure)

        def select_all():
            for fid, opt_vars in selected_options.items():
                for v in opt_vars.values():
                    v.set(True)

        def select_none():
            for fid, opt_vars in selected_options.items():
                for v in opt_vars.values():
                    v.set(False)

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        ttk.Button(btn_frame, text="Select All", command=select_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Select None", command=select_none).pack(side=tk.LEFT, padx=2)

        def on_ok():
            self.selected_subfilter_options = {}
            for fid, opt_vars in selected_options.items():
                chosen = [opt for opt, v in opt_vars.items() if v.get()]
                if chosen:
                    self.selected_subfilter_options[fid] = chosen
            cnt = sum(len(v) for v in self.selected_subfilter_options.values())
            self.status_var.set(f"Selected {cnt} sub-filter option(s)")
            dialog.destroy()

        def on_cancel():
            dialog.destroy()

        ttk.Button(dialog, text="Apply", command=on_ok).pack(side=tk.RIGHT, padx=10, pady=10)
        ttk.Button(dialog, text="Skip", command=on_cancel).pack(side=tk.RIGHT, padx=2, pady=10)

    def _show_filter_selection_dialog(self):
        """Show dialog with checkboxes for each available filter."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Select Advanced Filters")
        dialog.geometry("400x450")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Choose filters to apply during scraping:", font=("Arial", 10, "bold")).pack(anchor=tk.W, padx=10, pady=(10, 5))

        scroll_frame = ttk.Frame(dialog)
        scroll_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        canvas = tk.Canvas(scroll_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(scroll_frame)
        inner = ttk.Frame(canvas)

        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.configure(command=canvas.yview)
        scroll_frame.columnconfigure(0, weight=1)
        scroll_frame.rowconfigure(0, weight=1)
        canvas.grid(row=0, column=0, sticky=(tk.N, tk.S, tk.E, tk.W))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        canvas.create_window((0, 0), window=inner, anchor=tk.NW)

        var_by_id = {}
        for f in self.available_advanced_filters:
            vid = f.get("id") or ""
            label = f.get("label") or vid
            var = tk.BooleanVar(value=vid in self.selected_advanced_filters)
            var_by_id[vid] = var
            cb = ttk.Checkbutton(inner, text=label, variable=var)
            cb.pack(anchor=tk.W, pady=2)

        def on_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        inner.bind("<Configure>", on_configure)

        def select_all():
            for v in var_by_id.values():
                v.set(True)

        def select_none():
            for v in var_by_id.values():
                v.set(False)

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        ttk.Button(btn_frame, text="Select All", command=select_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Select None", command=select_none).pack(side=tk.LEFT, padx=2)

        def on_ok():
            self.selected_advanced_filters = [fid for fid, v in var_by_id.items() if v.get()]
            cnt = len(self.selected_advanced_filters)
            self.filters_status_var.set(f"{cnt} selected")
            self.status_var.set(f"Selected {cnt} filter(s). Ready to scrape.")
            dialog.destroy()
            self.advanced_filters_resolved = True
            self._update_flow_state()

            subfilter_ids = [fid for fid in self.selected_advanced_filters if fid in SUBFILTER_CONFIG]
            if subfilter_ids:
                self._load_and_show_subfilter_options(subfilter_ids, params_for_subfilter())

        def params_for_subfilter():
            cred_key = self.cred_var.get()
            creds = self.credentials[cred_key]
            return {
                "login_url": self.url_entry.get().strip(),
                "email": creds["email"],
                "password": creds["password"],
                "institute": self.institute_entry.get().strip(),
                "source": self.source_entry.get().strip(),
            }

        def on_cancel():
            dialog.destroy()
            self.advanced_filters_resolved = True
            self.filters_status_var.set("None selected")
            self._update_flow_state()

        ttk.Button(dialog, text="Apply Selection", command=on_ok).pack(side=tk.RIGHT, padx=10, pady=10)
        ttk.Button(dialog, text="Cancel", command=on_cancel).pack(side=tk.RIGHT, padx=2, pady=10)

    async def _fetch_advanced_filters(self, params):
        """Login, navigate to leads form, open Advance Filter, read checkbox list. Returns [{id, label}, ...]."""
        async def _do_fetch():
            return await self._fetch_advanced_filters_impl(params)
        return await _retry_async(
            _do_fetch,
            retry_on_empty=True,
            empty_check=lambda r: not r
        )

    async def _fetch_advanced_filters_impl(self, params):
        """Implementation of advanced filters fetch (called with retry)."""
        headless = getattr(self, "headless", True)
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()
            page = await context.new_page()
            try:
                await page.goto(params["login_url"], wait_until="domcontentloaded", timeout=60000)
                await page.get_by_role("textbox", name="name").fill(params["email"])
                await page.get_by_role("textbox", name="password").fill(params["password"])
                await page.get_by_role("button", name="Log In").click()
                await page.wait_for_timeout(500)
                if not getattr(self, "skip_campaign_detailed_view", False):
                    await ensure_campaign_detailed_view(
                        page,
                        log_fn=logger.info,
                        timeout_goto=60000,
                        timeout_network=15000,
                        timeout_combobox=TIMEOUT_FETCH_LOGIN,
                    )
                else:
                    logger.info("Manual web app: skipping Campaign Detailed View (filter list fetch)")
                selected = await _select_institute_option(page, params["institute"])
                if not selected:
                    raise RuntimeError(f"Institute '{params['institute']}' not found")
                await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

                await page.get_by_role("listbox").filter(has_text="Select Source").locator("div").nth(4).click()
                try:
                    await page.get_by_role("option", name=params["source"], exact=True).click()
                except Exception:
                    pass
                await page.wait_for_timeout(TIMEOUT_FETCH_NAV)

                af_btn = page.locator("app-advancefilter button:has-text('Advance Filter')")
                await af_btn.first.click()
                await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

                filters = await page.evaluate("""() => {
                    const items = document.querySelectorAll('app-advancefilter .filter_list .custom-control.custom-checkbox');
                    return Array.from(items).map(item => {
                        const input = item.querySelector('input');
                        const label = item.querySelector('label');
                        return input && label ? { id: input.id, label: label.innerText.trim() } : null;
                    }).filter(x => x && x.id);
                }""")

                try:
                    cancel_btn = page.locator("app-advancefilter .filtersbtn button.btn-secondary")
                    await cancel_btn.first.click()
                except Exception:
                    pass

                await context.close()
                await browser.close()
                return filters or []
            except Exception as e:
                try:
                    await context.close()
                    await browser.close()
                except Exception:
                    pass
                raise e

    async def _fetch_subfilter_options(self, params, subfilter_ids):
        """
        Apply the given subfilters on platform, then read ng-select options for each.
        Returns {filter_id: [option_label1, option_label2, ...]}
        """
        async def _do_fetch():
            return await self._fetch_subfilter_options_impl(params, subfilter_ids)
        empty_check = lambda r: not r and len(subfilter_ids) > 0
        return await _retry_async(
            _do_fetch,
            retry_on_empty=True,
            empty_check=empty_check
        )

    async def _fetch_subfilter_options_impl(self, params, subfilter_ids):
        """Implementation of subfilter options fetch (called with retry)."""
        result = {}
        headless = getattr(self, "headless", True)
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()
            page = await context.new_page()
            try:
                await page.goto(params["login_url"], wait_until="domcontentloaded", timeout=60000)
                await page.get_by_role("textbox", name="name").fill(params["email"])
                await page.get_by_role("textbox", name="password").fill(params["password"])
                await page.get_by_role("button", name="Log In").click()
                await page.wait_for_timeout(500)
                if not getattr(self, "skip_campaign_detailed_view", False):
                    await ensure_campaign_detailed_view(
                        page,
                        log_fn=logger.info,
                        timeout_goto=60000,
                        timeout_network=15000,
                        timeout_combobox=TIMEOUT_FETCH_LOGIN,
                    )
                else:
                    logger.info("Manual web app: skipping Campaign Detailed View (subfilter fetch)")
                selected = await _select_institute_option(page, params["institute"])
                if not selected:
                    raise RuntimeError(f"Institute '{params['institute']}' not found")
                await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

                await page.get_by_role("listbox").filter(has_text="Select Source").locator("div").nth(4).click()
                try:
                    await page.get_by_role("option", name=params["source"], exact=True).click()
                except Exception:
                    pass
                await page.wait_for_timeout(TIMEOUT_FETCH_NAV)

                # Open Advance Filter and check the subfilter checkboxes
                af_btn = page.locator("app-advancefilter button:has-text('Advance Filter')")
                await af_btn.first.click()
                await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
                await page.wait_for_selector("app-advancefilter .dropdown-menu", state="visible", timeout=5000)

                for fid in subfilter_ids:
                    try:
                        input_loc = page.locator(f"app-advancefilter input[id='{fid}']")
                        if await input_loc.count() > 0 and not await input_loc.first.is_checked():
                            label_loc = page.locator(f"app-advancefilter label[for='{fid}']")
                            if await label_loc.count() > 0:
                                await label_loc.first.click()
                            else:
                                await input_loc.first.click()
                            await page.wait_for_timeout(TIMEOUT_UI_SHORT)
                    except Exception:
                        pass

                apply_btn = page.locator("app-advancefilter .filtersbtn button.btn-success")
                await apply_btn.first.click()
                await page.wait_for_timeout(TIMEOUT_UI_SETTLE * 2)  # Form updates with new dropdowns

                # Read options from each subfilter (ng-select or ng-multiselect)
                for fid in subfilter_ids:
                    cfg = SUBFILTER_CONFIG.get(fid)
                    if not cfg:
                        continue
                    ftype = cfg.get("type", "ng-select") if isinstance(cfg, dict) else "ng-select"
                    try:
                        if ftype == "ng-multiselect":
                            # ng-multiselect-dropdown: span.dropdown-btn has label text, options in li.multiselect-item-checkbox div
                            label_text = cfg.get("label_text") or cfg.get("label")
                            if not label_text:
                                continue
                            ms_dropdown = page.locator("ng-multiselect-dropdown").filter(has=page.locator(f".dropdown-btn:has-text('{label_text}')"))
                            if await ms_dropdown.count() == 0:
                                continue
                            await ms_dropdown.locator(".dropdown-btn").first.click()
                            await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

                            opts = await page.evaluate("""() => {
                                const list = document.querySelector('ng-multiselect-dropdown .dropdown-list');
                                if (!list) return [];
                                const items = list.querySelectorAll('.multiselect-item-checkbox div');
                                return Array.from(items).map(el => el.innerText.trim()).filter(t => t);
                            }""")
                            result[fid] = opts if opts else []
                            await page.keyboard.press("Escape")
                        else:
                            # ng-select
                            placeholder = cfg.get("placeholder") if isinstance(cfg, dict) else cfg
                            if not placeholder:
                                continue
                            ng_select = page.locator("ng-select").filter(has=page.locator(f".ng-placeholder:has-text('{placeholder}')"))
                            if await ng_select.count() == 0:
                                continue
                            await ng_select.first.click()
                            await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

                            opts = await page.evaluate("""() => {
                                const labels = document.querySelectorAll('ng-dropdown-panel .ng-option .ng-option-label');
                                return Array.from(labels).map(el => el.innerText.trim()).filter(t => t);
                            }""")
                            result[fid] = opts if opts else []

                            await page.keyboard.press("Escape")
                        await page.wait_for_timeout(TIMEOUT_UI_SHORT)
                    except Exception as e:
                        logger.warning(f"Could not read options for {fid}: {e}")

                await context.close()
                await browser.close()
            except Exception as e:
                try:
                    await context.close()
                    await browser.close()
                except Exception:
                    pass
                raise e
        return result

    def add_placeholder(self, entry, placeholder):
        """Add placeholder text functionality to entry widget"""
        entry.insert(0, placeholder)
        entry.config(foreground='gray')
        
        def on_focus_in(event):
            if entry.get() == placeholder:
                entry.delete(0, tk.END)
                entry.config(foreground='black')
        
        def on_focus_out(event):
            if entry.get() == '':
                entry.insert(0, placeholder)
                entry.config(foreground='gray')
        
        entry.bind('<FocusIn>', on_focus_in)
        entry.bind('<FocusOut>', on_focus_out)
        
    def validate_date_format(self, date_str):
        """Validate date format DD-MM-YYYY"""
        if not date_str or date_str == "DD-MM-YYYY":
            return True, None
            
        try:
            date_obj = dt.strptime(date_str, "%d-%m-%Y")
            return True, date_obj
        except ValueError:
            return False, None

    def _parse_date(self, date_str):
        """Parse date string (supports DD-MM-YYYY, YYYY-MM-DD, DD/MM/YYYY). Same as login2.py"""
        date_str = (date_str or "").strip()
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                date_obj = dt.strptime(date_str, fmt)
                return date_obj.year, date_obj.month, date_obj.day
            except ValueError:
                continue
        raise ValueError(f"Unsupported date format: {date_str}")
        
    def _on_url_keyrelease(self, event=None):
        """Autocomplete URL from urls.json when user types 2+ chars."""
        txt = self.url_entry.get().strip()
        if len(txt) >= 2 and self._urls_list:
            low = txt.lower()
            matches = [u for u in self._urls_list if low in u.lower()]
            self.url_entry["values"] = matches[:10]
            if matches:
                try:
                    self.url_entry.event_generate("<Down>")
                except Exception:
                    pass
        else:
            self.url_entry["values"] = self._urls_list or []
        self._update_flow_state()

    def _on_institute_keyrelease(self, event=None):
        """Autocomplete institute from institutes.json when user types 2+ chars."""
        txt = self.institute_entry.get().strip()
        if len(txt) >= 2 and self._institutes_list:
            low = txt.lower()
            matches = [i for i in self._institutes_list if low in i.lower()]
            self.institute_entry["values"] = matches[:30]  # Limit dropdown size
            if matches:
                try:
                    self.institute_entry.event_generate("<Down>")
                except Exception:
                    pass
        else:
            self.institute_entry["values"] = []
        self.auto_generate_filename(event)
        self._update_flow_state()

    def auto_generate_filename(self, event=None):
        institute = self.institute_entry.get().strip()
        if institute:
            # Clean institute name for filename
            clean_institute = "".join(c for c in institute if c.isalnum() or c in (' ', '-', '_')).rstrip()
            # Include date and time in filename
            datetime_str = datetime.datetime.now().strftime("%d-%m-%y_%H-%M-%S")
            filename = f"{clean_institute}({datetime_str}).csv"
            self.file_entry.delete(0, tk.END)
            self.file_entry.insert(0, filename)

    
    def start_scraping(self):
        if not PLAYWRIGHT_AVAILABLE:
            messagebox.showerror("Error", "Playwright is not available. Install browsers first.")
            return
        if not self.advanced_filters_resolved:
            messagebox.showerror("Error", "Load or skip advanced filters first.")
            return

        # Validate inputs
        url = self.url_entry.get().strip()
        if not url:
            logger.warning("Validation failed: login URL empty")
            messagebox.showerror("Error", "Please enter login URL")
            return
        if not url.startswith(("http://", "https://")):
            messagebox.showerror("Error", "Login URL must start with http:// or https://")
            return

        if not self.institute_entry.get().strip():
            logger.warning("Validation failed: institute name empty")
            messagebox.showerror("Error", "Please enter institute name")
            return
            
        if not self.source_entry.get().strip():
            logger.warning("Validation failed: source empty")
            messagebox.showerror("Error", "Please enter source")
            return
            
        filename = self.file_entry.get().strip()
        if not filename:
            logger.warning("Validation failed: file name empty")
            messagebox.showerror("Error", "Please enter file name")
            return
        invalid_chars = set('\\/:*?"<>|')
        if any(c in filename for c in invalid_chars):
            messagebox.showerror("Error", "File name contains invalid characters (\\ / : * ? \" < > |)")
            return
        
        # Validate date formats
        from_date = self.from_date_entry.get().strip()
        to_date = self.to_date_entry.get().strip()
        
        # Remove placeholder values
        if from_date == "DD-MM-YYYY":
            from_date = ""
        if to_date == "DD-MM-YYYY":
            to_date = ""
        
        # Check if only one date is provided
        if (from_date and not to_date) or (not from_date and to_date):
            logger.warning("Validation failed: one date provided without the other")
            messagebox.showerror("Error", "Please provide both From and To dates, or leave both empty")
            return
            
        # Validate date formats
        if from_date and to_date:
            from_valid, from_obj = self.validate_date_format(from_date)
            to_valid, to_obj = self.validate_date_format(to_date)
            
            if not from_valid:
                logger.warning("Validation failed: invalid From Date format")
                messagebox.showerror("Error", "Invalid From Date format. Use DD-MM-YYYY")
                return
                
            if not to_valid:
                logger.warning("Validation failed: invalid To Date format")
                messagebox.showerror("Error", "Invalid To Date format. Use DD-MM-YYYY")
                return
            
            # Check if from_date is before to_date
            if from_obj and to_obj and from_obj > to_obj:
                logger.warning("Validation failed: From Date after To Date")
                messagebox.showerror("Error", "From Date cannot be after To Date")
                return
        
        # Get selected credentials
        cred_key = self.cred_var.get()
        credentials = self.credentials[cred_key]
        
        # Prepare parameters
        params = {
            'login_url': self.url_entry.get().strip(),
            'email': credentials['email'],
            'password': credentials['password'],
            'institute': self.institute_entry.get().strip(),
            'source': self.source_entry.get().strip(),
            'rows_per_page': self.rows_var.get(),
            'filename': self.file_entry.get().strip(),
            'from_date': from_date,
            'to_date': to_date,
            'instance': self.instance_var.get(),
            'order': self.order_var.get(),
            'advanced_filter_ids': list(self.selected_advanced_filters),
            'subfilter_options': dict(self.selected_subfilter_options),  # {filter_id: [option1, ...]}
        }
        
        # Update UI
        self.scraping = True
        self.start_btn.config(state="disabled")
        self.screenshot_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.progress.start()
        self.status_var.set("Starting scraper...")
        logger.info(f"Starting scrape: institute={params.get('institute')}, source={params.get('source')}, file={params.get('filename')}")

        # Run scraping in separate thread
        thread = threading.Thread(target=self.run_async_scraper, args=(params,))
        thread.daemon = True
        thread.start()

    def start_scraping_with_params(self, params):
        """Start scraping with given params dict (for headless/webapp). No GUI field reads."""
        if not PLAYWRIGHT_AVAILABLE:
            self.update_status("Playwright not available")
            self.scraping_complete()
            return
        self.scraping = True
        if hasattr(self, "start_btn") and self.start_btn:
            self.start_btn.config(state="disabled")
        if hasattr(self, "screenshot_btn") and self.screenshot_btn:
            self.screenshot_btn.config(state="disabled")
        if hasattr(self, "stop_btn") and self.stop_btn:
            self.stop_btn.config(state="normal")
        if hasattr(self, "progress") and self.progress:
            self.progress.start()
        self.status_var.set("Starting scraper...")
        logger.info(f"Headless scrape: institute={params.get('institute')}, source={params.get('source')}")
        thread = threading.Thread(target=self.run_async_scraper, args=(params,))
        thread.daemon = True
        thread.start()

    def start_screenshot(self):
        """Take screenshot of the filtered page (same flow as scrape, but capture image instead)."""
        if not PLAYWRIGHT_AVAILABLE:
            messagebox.showerror("Error", "Playwright is not available. Install browsers first.")
            return
        if not self.advanced_filters_resolved:
            messagebox.showerror("Error", "Load or skip advanced filters first.")
            return
        url = self.url_entry.get().strip()
        if not url:
            messagebox.showerror("Error", "Please enter login URL")
            return
        if not url.startswith(("http://", "https://")):
            messagebox.showerror("Error", "Login URL must start with http:// or https://")
            return
        if not self.institute_entry.get().strip():
            messagebox.showerror("Error", "Please enter institute name")
            return
        if not self.source_entry.get().strip():
            messagebox.showerror("Error", "Please enter source")
            return
        from_date = self.from_date_entry.get().strip()
        to_date = self.to_date_entry.get().strip()
        if from_date == "DD-MM-YYYY":
            from_date = ""
        if to_date == "DD-MM-YYYY":
            to_date = ""
        if not from_date or not to_date:
            messagebox.showerror("Error", "From and To dates are required for taking a screenshot.")
            return
        if from_date and to_date:
            from_valid, from_obj = self.validate_date_format(from_date)
            to_valid, to_obj = self.validate_date_format(to_date)
            if not from_valid:
                messagebox.showerror("Error", "Invalid From Date format. Use DD-MM-YYYY")
                return
            if not to_valid:
                messagebox.showerror("Error", "Invalid To Date format. Use DD-MM-YYYY")
                return
            if from_obj and to_obj and from_obj > to_obj:
                messagebox.showerror("Error", "From Date cannot be after To Date")
                return
        cred_key = self.cred_var.get()
        credentials = self.credentials[cred_key]
        # Screenshot path: save in Downloads folder (same as CSV)
        downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
        file_base = self.file_entry.get().strip()
        if file_base:
            base = file_base.rsplit(".", 1)[0] if "." in file_base else file_base
            fname = f"{base}.png"
        else:
            clean_inst = "".join(c for c in self.institute_entry.get().strip() if c.isalnum() or c in (' ', '-', '_')).rstrip()
            datetime_str = datetime.datetime.now().strftime("%d-%m-%y_%H-%M-%S")
            fname = f"{clean_inst}({datetime_str}).png"
        screenshot_path = os.path.join(downloads_path, fname)
        params = {
            "login_url": url,
            "email": credentials["email"],
            "password": credentials["password"],
            "institute": self.institute_entry.get().strip(),
            "source": self.source_entry.get().strip(),
            "from_date": from_date,
            "to_date": to_date,
            "instance": self.instance_var.get(),
            "advanced_filter_ids": list(self.selected_advanced_filters),
            "subfilter_options": dict(self.selected_subfilter_options),
            "screenshot_mode": True,
            "screenshot_path": screenshot_path,
        }
        self.scraping = True
        self.start_btn.config(state="disabled")
        self.screenshot_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.progress.start()
        self.status_var.set("Taking screenshot...")
        thread = threading.Thread(target=self.run_async_scraper, args=(params,))
        thread.daemon = True
        thread.start()

    def stop_scraping(self):
        self.scraping = False
        self.status_var.set("Stopping scraper...")
        if self._scraper_loop and self._scraper_task:
            self._scraper_loop.call_soon_threadsafe(self._scraper_task.cancel)
    
    def run_async_scraper(self, params):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._scraper_loop = loop
        self._scraper_task = loop.create_task(self.async_scraper(params))
        try:
            loop.run_until_complete(self._scraper_task)
        except asyncio.CancelledError:
            self.update_status("Stopped by user")
            self._schedule( self.scraping_complete)
        except ManualScrapeLeadsLimitExceeded:
            raise
        except Exception as e:
            self.update_status(f"❌ Error in async scraper: {str(e)}")
        finally:
            self._scraper_task = None
            self._scraper_loop = None
            loop.close()
    
    async def async_scraper(self, params):
        if not PLAYWRIGHT_AVAILABLE:
            self.update_status("❌ Playwright not available")
            if getattr(self, "headless", False):
                raise ManualScrapeLogicalError("PLAYWRIGHT_MISSING", "Playwright is not available.")
            return

        try:
            async with async_playwright() as playwright:
                await self.run_scraping(playwright, params)
        except ManualScrapeLeadsLimitExceeded:
            raise
        except ManualScrapeLogicalError:
            raise
        except ManualScrapeTransientError:
            raise
        except asyncio.TimeoutError:
            raise
        except Exception as e:
            self.update_status(f"❌ Error: {str(e)}")
            if getattr(self, "headless", False):
                if _is_retryable_error(e):
                    raise ManualScrapeTransientError("NETWORK_ERROR", str(e)) from e
                raise
    
    async def apply_instance_filter(self, page, instance_value):
        """
        Applies Instance dropdown filter (Primary / Secondary / Tertiary)
        """
        if not instance_value or instance_value == "All":
            self.update_status("Skipping Instance filter")
            return

        self.update_status(f"Applying Instance filter: {instance_value}")

        # Open Instance dropdown
        await page.locator(
            "xpath=/html/body/app-root/app-layout/section/app-detail/div/div[2]/div/app-fieldsearch/form/div[1]/div[3]/div/ng-select/div/div/div[2]/input"
        ).click()

        # Select option
        await page.get_by_text(instance_value, exact=True).click()

        await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
        self.update_status(f"✅ Instance filter applied: {instance_value}")

    async def apply_advanced_filters(self, page, filter_ids):
        """Check selected advanced filter checkboxes and click Apply. Retries on network error."""
        if not filter_ids:
            self.update_status("Skipping advanced filters (none selected)")
            return

        self.update_status(f"Applying {len(filter_ids)} advanced filter(s)...")
        last_error = None

        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                await page.wait_for_selector("app-advancefilter", timeout=15000)
                await page.wait_for_timeout(TIMEOUT_UI_SHORT)

                af_btn = page.locator("app-advancefilter button.dropdown-toggle, app-advancefilter button:has-text('Advance Filter')")
                await af_btn.first.click()

                dropdown = page.locator("app-advancefilter .dropdown-menu")
                await dropdown.wait_for(state="visible", timeout=5000)
                await page.wait_for_timeout(TIMEOUT_UI_SHORT)

                for fid in filter_ids:
                    try:
                        input_loc = page.locator(f"app-advancefilter input[id='{fid}']")
                        if await input_loc.count() == 0:
                            continue
                        if await input_loc.first.is_checked():
                            continue
                        label_loc = page.locator(f"app-advancefilter label[for='{fid}']")
                        if await label_loc.count() > 0:
                            await label_loc.first.scroll_into_view_if_needed()
                            await label_loc.first.click()
                        else:
                            await input_loc.first.scroll_into_view_if_needed()
                            await input_loc.first.click()
                        await page.wait_for_timeout(TIMEOUT_UI_SHORT)
                    except Exception as e:
                        if _is_retryable_error(e) and attempt < RETRY_MAX_ATTEMPTS:
                            raise
                        logger.warning(f"Could not check filter {fid}: {e}")

                apply_btn = page.locator("app-advancefilter .filtersbtn button.btn-success")
                await apply_btn.first.scroll_into_view_if_needed()
                await apply_btn.first.click()
                await page.wait_for_timeout(TIMEOUT_UI_SETTLE * 2)
                self.update_status(f"✅ Applied {len(filter_ids)} advanced filter(s)")
                return
            except Exception as e:
                last_error = e
                if _is_retryable_error(e) and attempt < RETRY_MAX_ATTEMPTS:
                    logger.warning(f"Advanced filter apply retry {attempt}/{RETRY_MAX_ATTEMPTS}: {e}")
                    await asyncio.sleep(RETRY_DELAY_MS / 1000)
                else:
                    break

        logger.warning(f"Advanced filter apply error after {RETRY_MAX_ATTEMPTS} attempts: {last_error}")
        self.update_status(f"⚠️ Advanced filter apply failed: {last_error}")

    async def apply_subfilter_options(self, page, subfilter_options):
        """
        Set sub-filter values. subfilter_options: {filter_id: [option1, option2, ...]}.
        Handles ng-select (single) and ng-multiselect-dropdown (multi).
        """
        if not subfilter_options:
            self.update_status("Skipping sub-filter options (none selected)")
            return

        self.update_status("Applying sub-filter options...")
        for fid, options in subfilter_options.items():
            if not options:
                continue
            cfg = SUBFILTER_CONFIG.get(fid)
            if not cfg:
                continue
            ftype = cfg.get("type", "ng-select") if isinstance(cfg, dict) else "ng-select"
            for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
                try:
                    if ftype == "ng-multiselect":
                        label_text = cfg.get("label_text") or cfg.get("label")
                        if not label_text:
                            continue
                        ms_dropdown = page.locator("ng-multiselect-dropdown").filter(has=page.locator(f".dropdown-btn:has-text('{label_text}')"))
                        if await ms_dropdown.count() == 0:
                            logger.warning(f"Sub-filter ng-multiselect not found: {label_text}")
                            continue
                        await ms_dropdown.first.scroll_into_view_if_needed()
                        await ms_dropdown.locator(".dropdown-btn").first.click()
                        await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

                        for opt in options:
                            try:
                                li_el = page.locator(f"ng-multiselect-dropdown .multiselect-item-checkbox:has(div:has-text('{opt}'))")
                                if await li_el.count() > 0:
                                    cb = li_el.first.locator("input[type=checkbox]")
                                    if await cb.count() > 0 and not await cb.is_checked():
                                        await li_el.first.click()
                                    await page.wait_for_timeout(TIMEOUT_UI_SHORT)
                            except Exception:
                                pass

                        await page.keyboard.press("Escape")
                    else:
                        placeholder = cfg.get("placeholder") if isinstance(cfg, dict) else cfg
                        if not placeholder:
                            continue
                        ng_select = page.locator("ng-select").filter(has=page.locator(f".ng-placeholder:has-text('{placeholder}')"))
                        if await ng_select.count() == 0:
                            logger.warning(f"Sub-filter ng-select not found: {placeholder}")
                            continue
                        await ng_select.first.scroll_into_view_if_needed()
                        await ng_select.first.click()
                        await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

                        is_single = "ng-select-single" in (await ng_select.first.get_attribute("class") or "")
                        for opt in options:
                            try:
                                option_el = page.locator(f"ng-dropdown-panel .ng-option .ng-option-label:has-text('{opt}')").first
                                if await option_el.count() > 0:
                                    await option_el.click()
                                    await page.wait_for_timeout(TIMEOUT_UI_SHORT)
                                    if is_single:
                                        break
                            except Exception:
                                pass

                        if not is_single:
                            await page.keyboard.press("Escape")
                    await page.wait_for_timeout(TIMEOUT_UI_SHORT)
                    break
                except Exception as e:
                    if _is_retryable_error(e) and attempt < RETRY_MAX_ATTEMPTS:
                        logger.warning(f"Sub-filter {fid} retry {attempt}/{RETRY_MAX_ATTEMPTS}: {e}")
                        await asyncio.sleep(RETRY_DELAY_MS / 1000)
                    else:
                        logger.warning(f"Could not apply sub-filter {fid}: {e}")
                        break

        self.update_status("✅ Applied sub-filter options")

    async def apply_date_filter(self, page, from_date, to_date, wait_after_apply_ms=0):
        """Select date range using login2.py logic (div.dateField, ngb-datepicker selectors).
        wait_after_apply_ms: extra wait after Apply, before Search (e.g. 5000 for screenshot)."""
        if not from_date or not to_date:
            self.update_status("Skipping date filter (no dates provided)")
            return

        self.update_status("Applying date filter...")
        try:
            start_year, start_month, start_day = self._parse_date(from_date)
            end_year, end_month, end_day = self._parse_date(to_date)
        except ValueError as e:
            logger.warning(f"Date parse error: {e}")
            self.update_status(f"⚠️ Invalid date format: {e}")
            return

        # Try login2 selectors first; fallback to XPath for Lead Details page
        try:
            await page.wait_for_selector("div.dateField", timeout=10000)
        except Exception:
            # Lead Details may use different structure - try XPath trigger
            trigger = page.locator("xpath=/html/body/app-root/app-layout/section/app-detail/div/div[2]/div/app-fieldsearch/form/div[1]/div[4]/div/div")
            await trigger.first.click()
            await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
        else:
            await page.wait_for_timeout(TIMEOUT_UI_SHORT)

        # ---- Select FROM DATE ----
        try:
            await page.click("div.dateField input:first-of-type", timeout=2000)
        except Exception:
            try:
                await page.click("div.dateField:first-of-type", timeout=2000)
            except Exception:
                try:
                    await page.click("div.dateField input[type='text']:first-of-type", timeout=2000)
                except Exception:
                    await page.locator("xpath=/html/body/app-root/app-layout/section/app-detail/div/div[2]/div/app-fieldsearch/form/div[1]/div[4]/div/div").first.click()

        await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
        await page.wait_for_selector("ngb-datepicker", timeout=10000)

        await page.select_option("ngb-datepicker-navigation-select select:first-of-type", value=str(start_month))
        await page.wait_for_timeout(TIMEOUT_UI_SHORT)
        await page.select_option("ngb-datepicker-navigation-select select:last-of-type", label=str(start_year))
        await page.wait_for_timeout(TIMEOUT_UI_SHORT)

        # Select start day
        await page.wait_for_selector("ngb-datepicker-month-view .ngb-dp-day", timeout=5000)
        await page.click(f'ngb-datepicker-month-view .ngb-dp-day span:has-text("{start_day}"):not(.text-muted)')
        await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

        # ---- Select TO DATE ----
        try:
            await page.wait_for_selector("ngb-datepicker", timeout=2000)
            if start_month != end_month or start_year != end_year:
                await page.select_option("ngb-datepicker-navigation-select select:first-of-type", value=str(end_month))
                await page.wait_for_timeout(TIMEOUT_UI_SHORT)
                await page.select_option("ngb-datepicker-navigation-select select:last-of-type", label=str(end_year))
                await page.wait_for_timeout(TIMEOUT_UI_SHORT)
        except Exception:
            try:
                await page.click("div.dateField input:last-of-type", timeout=2000)
            except Exception:
                try:
                    await page.click("div.dateField:last-of-type", timeout=2000)
                except Exception:
                    await page.click("div.dateField input[type='text']:last-of-type", timeout=2000)
            await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
            await page.wait_for_selector("ngb-datepicker", timeout=10000)
            await page.select_option("ngb-datepicker-navigation-select select:first-of-type", value=str(end_month))
            await page.wait_for_timeout(TIMEOUT_UI_SHORT)
            await page.select_option("ngb-datepicker-navigation-select select:last-of-type", label=str(end_year))
            await page.wait_for_timeout(TIMEOUT_UI_SHORT)

        # Select end day
        await page.wait_for_selector(f'ngb-datepicker-month-view .ngb-dp-day span:has-text("{end_day}")', timeout=5000)
        await page.click(f'ngb-datepicker-month-view .ngb-dp-day span:has-text("{end_day}"):not(.text-muted)')
        await page.wait_for_timeout(TIMEOUT_UI_SHORT)

        logger.info(f"Date range selected: {from_date} - {to_date}")

        # Apply and Search (Script_Scraper specific - date picker Apply + form Search)
        try:
            apply_btn = page.locator("xpath=/html/body/app-root/app-layout/section/app-detail/div/div[2]/div/app-fieldsearch/form/div[1]/div[4]/div/div/div[2]/div/div[2]/div[3]/button[1]")
            await apply_btn.click()
            await page.wait_for_timeout(TIMEOUT_UI_SHORT)
        except Exception:
            pass
        try:
            search_btn = page.locator("xpath=/html/body/app-root/app-layout/section/app-detail/div/div[2]/div/app-fieldsearch/form/div[2]/button[1]")
            await search_btn.click()
        except Exception:
            await page.get_by_role("button", name="Search").click()

        if wait_after_apply_ms > 0:
            self.update_status(f"Waiting {wait_after_apply_ms // 1000}s for page to load...")
            await page.wait_for_timeout(wait_after_apply_ms)
        await page.wait_for_timeout(TIMEOUT_UI_SETTLE * 2)
        self.update_status("✅ Date filter applied successfully")
    
    def _is_leads_response(self, res):
        """Flexible matcher for leads/campaign API – URL may vary across environments."""
        url_lower = (res.url or "").lower()
        return 200 <= res.status < 300 and any(
            x in url_lower for x in [
                "lead", "viewlist", "leaddetails", "campaigndetails",
                "getlead", "getcampaign", "detailsviewlist"
            ]
        )

    def _extract_records(self, obj):
        """Extract list of records from API response (handles various structures). Same as login2.py"""
        if obj is None:
            return []
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            for key in ("data", "records", "items", "result", "list", "leads"):
                val = obj.get(key)
                if isinstance(val, list):
                    return val
                if isinstance(val, dict):
                    for k in ("data", "records", "items", "list", "leads"):
                        if isinstance(val.get(k), list):
                            return val[k]
        return []

    def _flatten_record(self, rec):
        """Flatten nested dict/list values for CSV (convert to string)."""
        if not isinstance(rec, dict):
            return {"value": str(rec)}
        out = {}
        for k, v in rec.items():
            if isinstance(v, (dict, list)):
                try:
                    out[k] = json.dumps(v, ensure_ascii=False) if v is not None else ""
                except (TypeError, ValueError):
                    out[k] = str(v)
            else:
                out[k] = "" if v is None else str(v)
        return out

    def _is_generic_column_key(self, key):
        """Check if key looks like a generic column name (Column 1, col_1, Col_1, etc.)."""
        key = str(key).strip()
        if not key:
            return False
        if re.match(r"^Column\s+\d+$", key, re.I):
            return True
        if re.match(r"^col_?\d+$", key, re.I):
            return True
        return False

    def _generic_key_index(self, key):
        """Extract 0-based index from generic column key."""
        key = str(key).strip()
        m = re.match(r"^Column\s+(\d+)$", key, re.I)
        if m:
            return int(m.group(1)) - 1
        m = re.match(r"^col_?(\d+)$", key, re.I)
        if m:
            return int(m.group(1)) - 1
        return -1

    def _extract_columns_from_response(self, data):
        """Extract column names from API response. getLeadDetailsViewList returns data.data.headers."""
        if not isinstance(data, dict):
            return None
        # Check top-level keys
        for key in ("columns", "Columns", "columnNames", "headers", "Headers"):
            val = data.get(key)
            if isinstance(val, list) and val and all(isinstance(x, str) for x in val):
                return val
        # Check nested data (getLeadDetailsViewList: data.data.headers)
        inner = data.get("data") or data.get("Data")
        if isinstance(inner, dict):
            for key in ("columns", "Columns", "columnNames", "headers", "Headers"):
                val = inner.get(key)
                if isinstance(val, list) and val and all(isinstance(x, str) for x in val):
                    return val
        return None

    def _record_to_row(self, rec, headers):
        """Convert a record (list or dict) to a flat dict for CSV – one value per column."""
        if isinstance(rec, list):
            row = {}
            for i, h in enumerate(headers):
                row[h] = "" if i >= len(rec) else ("" if rec[i] is None else str(rec[i]))
            for i in range(len(headers), len(rec)):
                row[f"Col_{i+1}"] = "" if rec[i] is None else str(rec[i])
            return row
        if isinstance(rec, dict):
            flat = self._flatten_record(rec)
            keys = list(flat.keys())
            # If all keys are generic (Column 1, col_1, etc.) and we have headers, map by index
            if headers and keys and all(self._is_generic_column_key(k) for k in keys):
                sorted_keys = sorted(keys, key=lambda k: self._generic_key_index(k))
                row = {}
                for i, h in enumerate(headers):
                    row[h] = flat.get(sorted_keys[i], "") if i < len(sorted_keys) else ""
                for i in range(len(headers), len(sorted_keys)):
                    row[f"Col_{i+1}"] = flat.get(sorted_keys[i], "")
                return row
            return flat
        return {"value": str(rec)}

    async def _scrape_table_via_dom(self, page, params):
        """Fallback: scrape table from DOM when API capture fails. Returns list of dicts."""
        try:
            await page.wait_for_selector("app-leadsdatatable table thead tr th", timeout=10000)
            headers = await page.locator("app-leadsdatatable table thead tr th").all_text_contents()
            order = params.get("order", "Ascending")
            all_rows = []
            page_num = 1

            while self.scraping and not self._manual_stop_requested():
                self.update_status(f"DOM fallback: scraping page {page_num}...")
                await page.wait_for_selector("app-leadsdatatable table tbody tr", timeout=15000)
                page_data = await page.evaluate("""() => {
                    const rows = document.querySelectorAll("app-leadsdatatable table tbody tr");
                    return Array.from(rows).map(row =>
                        Array.from(row.querySelectorAll("td")).map(td => td.innerText.trim())
                    );
                }""")
                for row in page_data:
                    all_rows.append(dict(zip(headers, row)))

                if order == "Ascending":
                    nav = page.locator("a[aria-label='Next']").nth(1)
                    if await nav.is_disabled():
                        break
                    await nav.click()
                else:
                    prev = page.locator("a[aria-label='Previous']:visible").first
                    if not await prev.is_enabled():
                        break
                    await prev.click()
                page_num += 1
                await page.wait_for_timeout(TIMEOUT_PAGINATION)

            return all_rows
        except Exception as e:
            self.update_status(f"DOM fallback error: {e}")
            return []

    def _manual_stop_requested(self) -> bool:
        """Per-job cancel (RQ meta) or legacy global stop (single in-process manual scrape)."""
        cc = getattr(self, "_cancel_check", None)
        if cc is not None and callable(cc):
            try:
                if cc():
                    self.headless_stopped_by_user = True
                    return True
            except Exception:
                pass
            return False
        if _web_manual_stop_requested():
            self.headless_stopped_by_user = True
            _set_headless_stopped_by_user()
            return True
        return False

    def _chromium_launch_args_for_manual(self, params) -> list:
        args = []
        if getattr(self, "skip_campaign_detailed_view", False) or params.get("chromium_no_proxy", True):
            args.append("--no-proxy-server")
        return args

    async def _start_browser_session(self, playwright, params):
        """
        Isolated browser: optional persistent user_data_dir (per RQ job).
        Returns (browser, context, page). browser is None when using launch_persistent_context.
        """
        ud = (params.get("browser_user_data_dir") or "").strip()
        hl = params.get("headless_browser")
        if hl is None:
            hl = False
        args = self._chromium_launch_args_for_manual(params)
        if ud:
            os.makedirs(ud, exist_ok=True)
            context = await playwright.chromium.launch_persistent_context(
                user_data_dir=ud,
                headless=bool(hl),
                args=args,
            )
            page = context.pages[0] if context.pages else await context.new_page()
            return None, context, page
        browser = await playwright.chromium.launch(headless=bool(hl), args=args)
        context = await browser.new_context()
        page = await context.new_page()
        return browser, context, page

    async def _cleanup_browser_session(self, browser, context):
        if context is None:
            return
        try:
            await context.close()
        except Exception:
            pass
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass

    async def run_scraping(self, playwright, params):
        browser = None
        context = None
        page = None
        try:
            browser, context, page = await self._start_browser_session(playwright, params)
            # Step 1: Login (with retry on network error)
            self.update_status("Step 1: Logging in...")
            for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
                try:
                    await page.goto(
                        params['login_url'],
                        wait_until="domcontentloaded",
                        timeout=60000
                    )
                    await page.get_by_role("textbox", name="name").fill(params['email'])
                    await page.get_by_role("textbox", name="password").fill(params['password'])
                    await page.get_by_role("button", name="Log In").click()
                    await page.wait_for_timeout(500)
                    if getattr(self, "skip_campaign_detailed_view", False):
                        self.update_status("Step 1b: Post-login (skipping Campaign Detailed View)…")
                        logger.info("Manual web app: skipping ensure_campaign_detailed_view after login")
                    else:
                        self.update_status("Step 1b: Campaign Detailed View (/campaign/details)…")
                        await ensure_campaign_detailed_view(
                            page,
                            log_fn=lambda m: logger.info(m),
                            timeout_goto=60000,
                            timeout_network=15000,
                            timeout_combobox=TIMEOUT_LOGIN,
                        )
                    self.update_status("Step 2: Selecting institute and source...")
                    break
                except Exception as e:
                    if _is_retryable_error(e) and attempt < RETRY_MAX_ATTEMPTS:
                        logger.warning(f"Login retry {attempt}/{RETRY_MAX_ATTEMPTS}: {e}")
                        await asyncio.sleep(RETRY_DELAY_MS / 1000)
                    else:
                        raise
            if self._manual_stop_requested():
                self.update_status("Stopped by user")
                return
            # Select institute – stop if not found
            selected = await _select_institute_option(page, params["institute"])
            if not selected:
                msg = f"Institute '{params['institute']}' not found."
                self.update_status(msg)
                if getattr(self, "headless", False):
                    raise ManualScrapeLogicalError("INSTITUTE_NOT_FOUND", msg)
                self._schedule(lambda m=msg: messagebox.showerror("Not Found", m))
                return

            # Select source – stop if not found
            try:
                await page.get_by_role("listbox").filter(
                    has_text="Select Source×Select Source"
                ).locator("div").nth(4).click()
                await page.get_by_role("option", name=params['source'], exact=True).click(timeout=8000)
            except Exception:
                msg = f"Source '{params['source']}' not found."
                self.update_status(msg)
                if getattr(self, "headless", False):
                    raise ManualScrapeLogicalError("SOURCE_NOT_FOUND", msg)
                self._schedule(lambda m=msg: messagebox.showerror("Not Found", m))
                return
            
            # Apply Instance filter
            await self.apply_instance_filter(page, params.get('instance'))

            # Step 2a: Apply advanced filters (user-selected from Load Advanced Filters)
            await self.apply_advanced_filters(page, params.get('advanced_filter_ids', []))

            # Step 2a2: Apply sub-filter options (e.g. Lead Status = Verified)
            await self.apply_subfilter_options(page, params.get('subfilter_options', {}))

            # Step 2b: Apply date filter if provided
            if params['from_date'] and params['to_date']:
                wait_after = 5000 if params.get("screenshot_mode") else 0
                await self.apply_date_filter(page, params['from_date'], params['to_date'], wait_after_apply_ms=wait_after)
            else:
                await page.get_by_role("button", name="Search").click()

            # Wait for data/table or "No Record Found" to load
            await page.wait_for_timeout(TIMEOUT_UI_SETTLE * 2)
            await page.wait_for_selector("app-leadsdatatable, app-advanceview, .card-body, .card", timeout=TIMEOUT_TABLE_LOAD)

            # Check for "No Record Found" (filter combination returns no data)
            no_records = page.get_by_text("No Record Found", exact=False)
            if await no_records.count() > 0:
                try:
                    if await no_records.first.is_visible():
                        if not params.get("screenshot_mode"):
                            self.update_status("No records for the selected filters")
                            if getattr(self, "headless", False):
                                raise ManualScrapeLogicalError("NO_RECORDS", "No records for the selected filters.")
                            self._schedule(
                                lambda: messagebox.showinfo("No Records", "No records found for the selected filters.")
                            )
                            return
                except Exception:
                    pass

            # Screenshot mode: capture only when "No Record Found" OR "Total ... Primary Leads" is visible
            if params.get("screenshot_mode"):
                has_no_records = False
                has_primary_leads = False
                try:
                    if await no_records.count() > 0 and await no_records.first.is_visible():
                        has_no_records = True
                except Exception:
                    pass
                try:
                    primary_leads_el = page.locator("div.pull-left, div.pt-2.pb-1").filter(has_text=re.compile(r"Primary Leads", re.I)).first
                    if await primary_leads_el.count() > 0 and await primary_leads_el.is_visible():
                        has_primary_leads = True
                except Exception:
                    pass
                if not has_no_records and not has_primary_leads:
                    self.update_status("Page not ready – neither 'No Record Found' nor 'Total ... Primary Leads' detected")
                    self._schedule(
                        lambda: messagebox.showerror(
                            "Screenshot Aborted",
                            "Screenshot can only be taken when the page shows either:\n\n• No Record Found\n• Total ... Primary Leads\n\nPlease wait for the page to load fully and try again.",
                        )
                    )
                    if getattr(self, "headless", False):
                        raise ManualScrapeLogicalError(
                            "PAGE_NOT_READY",
                            "Screenshot page not ready (no Primary Leads / No Record Found).",
                        )
                    return
                try:
                    self.update_status("Capturing screenshot...")
                    await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
                    screenshot_path = params.get("screenshot_path", "screenshot.png")
                    if not os.path.isabs(screenshot_path):
                        screenshot_path = os.path.abspath(screenshot_path)
                    # Crop screenshot before table rows to avoid exposing lead data.
                    clip = None
                    try:
                        table_sel = "app-leadsdatatable table, app-leadsdatatable"
                        table_loc = page.locator(table_sel).first
                        if await table_loc.count() > 0:
                            box = await table_loc.bounding_box()
                            vp = page.viewport_size or {"width": 1366, "height": 768}
                            if box and box.get("y") is not None:
                                crop_h = max(120, int(box["y"]) - 8)  # keep filters + KPI blocks
                                clip = {
                                    "x": 0,
                                    "y": 0,
                                    "width": int(vp.get("width", 1366)),
                                    "height": min(int(vp.get("height", 768)), crop_h),
                                }
                    except Exception:
                        clip = None
                    if clip:
                        await page.screenshot(path=screenshot_path, clip=clip)
                    else:
                        await page.screenshot(path=screenshot_path, full_page=True)
                    self.last_output_path = screenshot_path
                    self.update_status(f"✅ Screenshot saved to {screenshot_path}")
                    self._schedule( lambda p=screenshot_path: messagebox.showinfo("Screenshot Saved", f"Screenshot saved to:\n{p}"))
                except Exception as e:
                    self.update_status(f"❌ Screenshot failed: {e}")
                    self._schedule(lambda err=str(e): messagebox.showerror("Screenshot Error", f"Failed to save screenshot:\n{err}"))
                return

            # Check total leads – if > 1 lakh, prompt to reduce date range
            total_leads = None
            try:
                # "Total 594568 Primary Leads" – number in strong or in div.pull-left text
                for sel in ["div.pull-left strong", "div.pt-2.pb-1 strong", ".pull-left strong"]:
                    el = page.locator(sel)
                    if await el.count() > 0:
                        txt = await el.first.text_content()
                        if txt:
                            txt = txt.replace(",", "").strip()
                            if txt.isdigit():
                                total_leads = int(txt)
                                break
                if total_leads is None:
                    row = page.locator("div.pull-left, div.pt-2.pb-1").filter(has_text=re.compile(r"Primary Leads", re.I)).first
                    if await row.count() > 0:
                        content = await row.text_content()
                        m = re.search(r"Total\s+([\d,]+)\s+Primary", content or "")
                        if m:
                            total_leads = int(m.group(1).replace(",", ""))
            except Exception as e:
                logger.debug(f"Could not read total leads count: {e}")
            max_leads = manual_scrape_max_leads_limit()
            if max_leads is not None and total_leads is not None and total_leads > max_leads:
                msg_ui = (
                    f"There are {total_leads:,} records. The limit is {max_leads:,} (1 lakh).\n\n"
                    "Please reduce the date range (From / To) and try again."
                )
                msg_headless = (
                    f"Too many records ({total_leads:,} > limit {max_leads:,}). "
                    "Reduce the From/To date range and run again."
                )
                self.update_status("Too many leads – reduce date range")
                if getattr(self, "headless", False):
                    logger.warning("Manual scrape: %s", msg_headless)
                    raise ManualScrapeLeadsLimitExceeded(msg_headless)
                self._schedule(lambda m=msg_ui: messagebox.showwarning("Too Many Records", m))
                return

            # Step 3: Edit Columns – Select All (multiple selectors for different institute layouts)
            self.update_status("Step 3: Configuring columns (Select All)...")
            # Wait for Edit Column button to appear (some institutes load it late)
            for wait_sel in ['button:has-text("Edit Column")', 'button:has(i.icon-editColumn)', 'button.btn-outline-success']:
                try:
                    await page.wait_for_selector(wait_sel, timeout=5000)
                    await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
                    break
                except Exception:
                    continue
            edit_done = False
            edit_btn_selectors = [
                'button:has(i.icon-editColumn)',
                'button:has(.icon-editColumn)',
                'button.btn-outline-success.withoutarrow',
                'button.btn-outline-success:has-text("Edit Column")',
                'button[ngbdropdowntoggle]:has-text("Edit Column")',
                'button.withoutarrow:has-text("Edit Column")',
                'button:has-text("Edit Column")',
                'button:has-text("Edit Columns")',
                '[ngbdropdowntoggle]:has-text("Edit Column")',
                'app-advanceview button:has-text("Edit Column")',
                'app-leadsdatatable button:has-text("Edit Column")',
            ]
            for btn_sel in edit_btn_selectors:
                if edit_done:
                    break
                try:
                    btn = page.locator(btn_sel)
                    if await btn.count() == 0:
                        continue
                    await btn.first.scroll_into_view_if_needed()
                    await btn.first.click(timeout=8000)
                    await page.wait_for_timeout(800)  # Allow dropdown to render (some institutes are slow)
                    select_all_selectors = [
                        '[cdk-overlay-container] .dropdown-menu label',
                        '[cdk-overlay-container] label:has-text("Select All")',
                        '.dropdown-menu label:has-text("Select All")',
                        '[ngbdropdownmenu] label:has-text("Select All")',
                        'label:has-text("Select All")',
                        'label:has-text("select all")',
                        'label:has-text("SelectAll")',
                        '.custom-control-label:has-text("Select All")',
                        'input[type="checkbox"] + label',
                        'label',
                    ]
                    for lbl_sel in select_all_selectors:
                        try:
                            sel = page.locator(lbl_sel).filter(has_text=re.compile(r"select\s*all", re.I))
                            if await sel.count() == 0:
                                continue
                            await sel.first.scroll_into_view_if_needed()
                            await sel.first.click(timeout=5000)
                            await page.wait_for_timeout(TIMEOUT_UI_SHORT)
                            ap = page.get_by_role("button", name="Apply")
                            if await ap.count() > 0:
                                await ap.first.click(timeout=5000)
                            edit_done = True
                            break
                        except Exception:
                            continue
                    if edit_done:
                        break
                except Exception as e:
                    logger.warning(f"Edit Columns ({btn_sel}): {e}")
                    continue
            try:
                await page.wait_for_selector("app-leadsdatatable table thead tr th, app-leadsdatatable table tbody tr, app-leadsdatatable", timeout=15000)
            except Exception:
                pass

            # Get table headers from HTML thead (multiple selectors for different layouts)
            headers = []
            header_selectors = [
                "app-leadsdatatable table thead tr th",
                "app-leadsdatatable thead tr th",
                "app-leadsdatatable thead th",
                "table thead tr th",
                "thead tr th",
                "thead th",
            ]
            for sel in header_selectors:
                try:
                    await page.wait_for_selector(sel, timeout=5000)
                    headers = await page.locator(sel).all_text_contents()
                    headers = [h.strip() for h in headers if h.strip()]
                    if headers:
                        logger.info(f"Captured {len(headers)} headers from table: {headers[:5]}...")
                        break
                except Exception:
                    continue

            # Step 4: Set rows per page + API capture setup (login2.py style)
            self.update_status("Step 4: Setting rows per page & capturing API...")
            rows_value = params['rows_per_page']
            order = params.get("order", "Ascending")
            # Match multiple possible API endpoints (lead + campaign views)
            API_KEYWORDS = ["leaddetails", "campaigndetails", "viewlist", "getlead", "getcampaign"]

            last_captured = [None]  # (url, method, headers, body)

            async def capture_route(route):
                req = route.request
                url_lower = (req.url or "").lower()
                if any(kw in url_lower for kw in API_KEYWORDS):
                    body = None
                    try:
                        body = req.post_data
                        if not body and hasattr(req, "post_data_buffer"):
                            buf = req.post_data_buffer
                            if buf:
                                body = buf.decode("utf-8", errors="replace") if isinstance(buf, bytes) else str(buf)
                    except Exception:
                        pass
                    last_captured[0] = (req.url, req.method, dict(req.headers or {}), body or "")
                    self.update_status(f"📡 Captured API: {req.url[:80]}...")
                await route.continue_()

            # Intercept all requests; capture_route filters by API keywords in URL
            await page.route("**/*", capture_route)

            async def fetch_with_captured(cap):
                """Re-fetch API with captured request (fallback). Same as login2.py"""
                if not cap:
                    return None
                url, method, headers, body = cap
                headers = {k: v for k, v in headers.items() if k.lower() != "content-length"}
                try:
                    api_resp = await page.request.fetch(url, method=method or "POST", headers=headers, data=body or "")
                    if api_resp.ok:
                        return await api_resp.json()
                except Exception:
                    pass
                return None

            # Select rows per page – triggers first API call
            await page.evaluate(f"""
                const dropdown = document.querySelectorAll('select')[2];
                if (dropdown && !Array.from(dropdown.options).some(o => o.value === '{rows_value}')) {{
                    const option = document.createElement('option');
                    option.value = '{rows_value}';
                    option.text = '{rows_value}';
                    dropdown.appendChild(option);
                }}
                dropdown.value = '{rows_value}';
                dropdown.dispatchEvent(new Event('change', {{ bubbles: true }}));
            """)
            rpp_select = page.locator("select").nth(2)

            collected = []
            last_captured[0] = None
            async with page.expect_response(self._is_leads_response, timeout=25000) as resp_info:
                await rpp_select.select_option(rows_value)
            await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
            cap = last_captured[0]
            data = await fetch_with_captured(cap)
            if not data:
                try:
                    resp = await resp_info.value
                    data = await resp.json()
                except Exception:
                    pass
            if data:
                records = self._extract_records(data)
                # Only add page 1 for Ascending; for Descending we'll get last page next
                if order == "Ascending":
                    collected.append({"data": data, "records": records})
                    self.update_status(f"Page 1: {len(records)} records (API)")

            await page.wait_for_timeout(TIMEOUT_PAGINATION)

            # Step 4b: Handle descending order – go to last page first
            if order == "Descending":
                self.update_status("🔽 Switching to last page for descending order...")
                last_button = page.locator("a[aria-label='last']")
                if await last_button.is_enabled():
                    try:
                        last_captured[0] = None
                        async with page.expect_response(self._is_leads_response, timeout=60000) as resp_info:
                            await last_button.click()
                        resp = await resp_info.value
                        await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
                        cap = last_captured[0]
                        data = await fetch_with_captured(cap)
                        if not data and resp:
                            try:
                                data = await resp.json()
                            except Exception:
                                pass
                        if data:
                            records = self._extract_records(data)
                            collected = [{"data": data, "records": records}]
                            self.update_status(f"Last page: {len(records)} records (API)")
                    except Exception:
                        await page.wait_for_timeout(TIMEOUT_PAGINATION)
                        await page.wait_for_selector("app-leadsdatatable table tbody tr", timeout=15000)
                else:
                    self.update_status("⚠️ Last button is disabled, starting from first page")
                    if data and not collected:
                        collected.append({"data": data, "records": self._extract_records(data)})
                await page.wait_for_timeout(TIMEOUT_PAGINATION)

            # Step 5: Paginate and capture API responses (login2.py style)
            self.update_status("Step 5: Paginating and capturing API data...")
            page_num = 1

            while self.scraping and not self._manual_stop_requested():
                if order == "Ascending":
                    nav_button = page.locator("a[aria-label='Next']").nth(1)
                    parent = nav_button.locator("xpath=..")
                    try:
                        cls = await parent.get_attribute("class") or ""
                        if "disabled" in cls:
                            self.update_status("✅ Reached last page")
                            break
                    except Exception:
                        break
                else:
                    prev_button = page.locator("a[aria-label='Previous']:visible").first
                    try:
                        if not await prev_button.is_enabled():
                            self.update_status("✅ First page reached.")
                            break
                    except Exception:
                        break

                page_num += 1
                self.update_status(f"📄 Fetching Page {page_num} (API)...")

                last_captured[0] = None
                try:
                    async with page.expect_response(self._is_leads_response, timeout=30000) as resp_info:
                        if order == "Ascending":
                            await nav_button.click()
                        else:
                            await prev_button.click()
                    await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
                    cap = last_captured[0]
                    data = await fetch_with_captured(cap)
                    if not data:
                        try:
                            resp = await resp_info.value
                            data = await resp.json()
                        except Exception:
                            pass
                    if data:
                        records = self._extract_records(data)
                        collected.append({"data": data, "records": records})
                        self.update_status(f"Page {page_num}: {len(records)} records (API)")

                        # Last page detection (ascending)
                        if order == "Ascending" and len(records) < int(rows_value):
                            self.update_status("✅ Last page detected from API.")
                            break
                except Exception as e:
                    self.update_status(f"⚠️ Error on page {page_num}: {e}")
                    break
                await page.wait_for_timeout(TIMEOUT_UI_SETTLE)

            if self._manual_stop_requested():
                self.update_status("Stopped by user")
                try:
                    await page.unroute("**/*")
                except Exception:
                    pass
                return

            # Unroute and write CSV
            try:
                await page.unroute("**/*")
            except Exception:
                pass

            all_records = []
            for item in collected:
                all_records.extend(item.get("records", []))

            if not all_records:
                self.update_status("⚠️ No API data – falling back to DOM scraping...")
                all_records = await self._scrape_table_via_dom(page, params)
                if not all_records:
                    self.update_status("No records for the selected filters")
                    self._schedule( lambda: messagebox.showinfo("No Records", "No records found for the selected filters."))

            if all_records:
                self.update_status(f"✅ Collected {len(all_records)} records. Writing CSV...")
                # Prefer API headers – they have the complete list (avoids Col_11..Col_20 for extras)
                api_headers = None
                if collected:
                    for item in collected:
                        api_data = item.get("data")
                        if api_data:
                            api_headers = self._extract_columns_from_response(api_data)
                            if api_headers:
                                break
                if api_headers:
                    headers = api_headers
                    logger.info(f"Using {len(headers)} headers from API")
                elif not headers:
                    for sel in ["app-leadsdatatable table thead tr th", "app-leadsdatatable thead th", "thead tr th", "thead th"]:
                        try:
                            ths = await page.locator(sel).all_text_contents()
                            headers = [h.strip() for h in ths if h.strip()]
                            if headers:
                                logger.info(f"Captured {len(headers)} headers from HTML thead")
                                break
                        except Exception:
                            continue
                if not headers and all_records:
                    sample = all_records[0]
                    if isinstance(sample, dict):
                        keys = list(sample.keys())
                        # Only use sample.keys() if they're NOT generic (avoid Column 1, etc.)
                        if keys and not all(self._is_generic_column_key(k) for k in keys):
                            headers = keys
                        else:
                            headers = [f"Col_{i+1}" for i in range(len(keys))]
                    elif isinstance(sample, list):
                        headers = [f"Col_{i+1}" for i in range(len(sample))]
                rows = [self._record_to_row(r, headers) if headers else self._flatten_record(r) for r in all_records]
                df = pd.DataFrame(rows)
                _cols_before = list(df.columns)
                df = drop_phone_mobile_columns(df)
                _dropped = [c for c in _cols_before if c not in df.columns]
                if _dropped:
                    logger.info(f"Dropped phone/mobile columns: {_dropped}")
                downloads_path = params.get("output_dir") or os.path.join(os.path.expanduser("~"), "Downloads")
                if downloads_path and not os.path.isdir(downloads_path):
                    os.makedirs(downloads_path, exist_ok=True)
                fname = params["filename"]
                if not fname.lower().endswith(".csv"):
                    base, _ = os.path.splitext(fname)
                    fname = (base or "export").rstrip(". ") + ".csv"
                file_path = os.path.join(downloads_path, fname)
                df.to_csv(file_path, index=False, encoding="utf-8", header=True)
                self.last_output_path = file_path
                self.update_status(f"✅ Saved to {file_path}")
                logger.info(f"Scrape complete: {len(all_records)} records saved to {file_path}")

        except ManualScrapeLeadsLimitExceeded:
            raise
        except ManualScrapeLogicalError:
            raise
        except Exception as e:
            error_msg = f"Script failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.update_status(f"❌ {error_msg}")
            if getattr(self, "headless", False):
                if _is_retryable_error(e):
                    raise ManualScrapeTransientError("NETWORK_ERROR", str(e)) from e
                raise
            messagebox.showerror("Error", error_msg)

        finally:
            await self._cleanup_browser_session(browser, context)
            self.scraping_complete()
    
    def _schedule(self, f):
        """Run on main thread (GUI) or immediately (headless)."""
        if getattr(self, "headless", False):
            f()
        else:
            self.root.after(0, f)

    def update_status(self, message):
        logger.info(message)
        if getattr(self, "headless", False):
            if getattr(self, "status_callback", None):
                self.status_callback(message)
            return
        def update():
            self.status_var.set(message)
            self.root.update()
        self._schedule( update)

    def scraping_complete(self):
        if getattr(self, "headless", False):
            self.scraping = False
            return
        def complete():
            self.scraping = False
            self.start_btn.config(state="normal")
            self.screenshot_btn.config(state="normal")
            self.stop_btn.config(state="disabled")
            self.progress.stop()
        self._schedule( complete)

def main():
    # Check if we need to show installation prompt
    if not TKINTER_AVAILABLE:
        logger.error("tkinter is not installed. GUI mode is unavailable on this machine.")
        return
    if not PLAYWRIGHT_AVAILABLE:
        root = tk.Tk()
        root.withdraw()  # Hide main window
        
        response = messagebox.askyesno(
            "Playwright Setup Required", 
            "Playwright browsers are not installed.\n\n"
            "This application requires Playwright to function properly.\n"
            "Would you like to install it now?\n\n"
            "This will install Chromium browser and may take a few minutes."
        )
        
        if response:
            # Show progress window
            progress_window = tk.Toplevel()
            progress_window.title("Installing Playwright")
            progress_window.geometry("300x100")
            
            label = ttk.Label(progress_window, text="Installing Playwright browsers...\nThis may take a few minutes.")
            label.pack(pady=10)
            
            progress = ttk.Progressbar(progress_window, mode='indeterminate')
            progress.pack(pady=10)
            progress.start()
            
            def install():
                success = install_playwright_browsers()
                progress_window.destroy()
                if success:
                    messagebox.showinfo("Success", "Playwright installed successfully! Please restart the application.")
                else:
                    messagebox.showerror("Error", "Failed to install Playwright. Please install manually.")
                root.destroy()
            
            threading.Thread(target=install, daemon=True).start()
            progress_window.mainloop()
        else:
            root.destroy()
            return
    
    # Start main application
    root = tk.Tk()
    app = ScraperApp(root)
    root.mainloop()

def _stub_messagebox():
    # Server/headless environments may not have tkinter installed.
    if not TKINTER_AVAILABLE:
        return
    import tkinter.messagebox as _mb
    _mb.showinfo = lambda *a, **k: None
    _mb.showerror = lambda *a, **k: None
    _mb.showwarning = lambda *a, **k: None
    _mb.askyesno = lambda *a, **k: True


def fetch_advanced_filters(params):
    """
    Sync wrapper: fetch advanced filter list for given params (login_url, email, password, institute, source).
    Returns list of {"id": str, "label": str}. Used by webapp.
    """
    _stub_messagebox()
    app = ScraperApp(root=None)
    app.headless = False
    return asyncio.run(app._fetch_advanced_filters(params))


def fetch_subfilter_options(params, filter_ids):
    """
    Sync wrapper: fetch subfilter options for given params and filter ids.
    Returns dict {filter_id: [option_label, ...]}. Used by webapp.
    """
    _stub_messagebox()
    app = ScraperApp(root=None)
    app.headless = False
    return asyncio.run(app._fetch_subfilter_options(params, filter_ids))


def run_headless(
    params,
    status_callback=None,
    cancel_event=None,
    cancel_check=None,
    browser_user_data_dir=None,
    headless_browser=None,
    job_timeout_sec=None,
    out_flags=None,
):
    """
    Run the scraper headless (no GUI). Used by RQ workers and tests.
    cancel_check: optional callable() -> bool; if True, scrape exits cooperatively (per-job cancel).
    browser_user_data_dir: optional Playwright persistent profile directory (job isolation).
    headless_browser: if None, defaults to False (legacy); workers pass True.
    job_timeout_sec: optional asyncio timeout around the full scrape.
    out_flags: optional dict; receives keys stopped_by_user (bool), timeout (bool).
    """
    _stub_messagebox()
    if cancel_check is None and cancel_event is None:
        clear_web_manual_stop()
    flags = out_flags if isinstance(out_flags, dict) else {}
    app = ScraperApp(root=None)
    app.headless_stopped_by_user = False
    app.status_callback = status_callback
    if cancel_event is not None:
        app._cancel_check = lambda: cancel_event.is_set()
    elif cancel_check is not None:
        app._cancel_check = cancel_check
    else:
        app._cancel_check = None
    p = dict(params)
    if browser_user_data_dir:
        p["browser_user_data_dir"] = browser_user_data_dir
    if headless_browser is not None:
        p["headless_browser"] = headless_browser
    try:

        async def _runner():
            coro = app.async_scraper(p)
            if job_timeout_sec is not None and float(job_timeout_sec) > 0:
                await asyncio.wait_for(coro, timeout=float(job_timeout_sec))
            else:
                await coro

        asyncio.run(_runner())
        return getattr(app, "last_output_path", None)
    except asyncio.TimeoutError:
        flags["timeout"] = True
        raise ManualScrapeTransientError("TIMEOUT", "Job exceeded time limit") from None
    finally:
        app.scraping = False
        flags["stopped_by_user"] = bool(getattr(app, "headless_stopped_by_user", False))


if __name__ == "__main__":
    main()