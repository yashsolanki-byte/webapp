"""
Batch scraper for NPF Paid Applications (self-contained copy for web app).
Logic adapted from: NPF paid application/Batch_Scraper.py
Flow: open URL -> login -> Campaigns → Detailed View (/campaign/details) -> fill institute -> source -> date range -> Apply
-> Advance Filter (Paid Applications = Yes) -> Search -> scrape leads, save CSV.
No Google Sheets; used by scraper_runner with scrape_list.json and DATA_Scraped output.
"""

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, date

_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRAPERS_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import project_paths as paths
from credential_env import ensure_row_password
from scrapers.export_columns import drop_phone_mobile_columns
from scrapers.npf_post_login import ensure_campaign_detailed_view

ROOT = paths.ROOT


# Must match scraper_runner / run_single_scrape_worker: they attach per-institute FileHandlers
# to logging.getLogger("batch_scraper"). If we used __name__ here, scrape_college logs would
# never reach logs/runs/<date>/<institute>.log.
BATCH_LOGGER_NAME = "batch_scraper"


def _setup_logging():
    os.makedirs(paths.LOGS_APP_DIR, exist_ok=True)
    log_file = paths.BATCH_SCRAPER_LOG_FILE
    logger = logging.getLogger(BATCH_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    abs_batch = os.path.abspath(log_file)
    has_batch_file = any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == abs_batch
        for h in logger.handlers
    )
    if not has_batch_file:
        handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)
    return logger


logger = _setup_logging()


def _setup_playwright():
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
    try:
        import playwright  # noqa: F401
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return False
    if getattr(sys, "frozen", False):
        base_path = getattr(sys, "_MEIPASS", ROOT)
        for name in ["playwright/browsers", "ms-playwright", "playwright"]:
            bundled = os.path.join(base_path, name)
            user_path = os.path.join(os.path.expanduser("~"), "AppData", "Local", "ms-playwright")
            if os.path.exists(bundled) and not os.path.exists(user_path):
                try:
                    shutil.copytree(bundled, user_path)
                    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = user_path
                    break
                except Exception as e:
                    logger.warning(f"Could not copy browsers: {e}")
    return True


def _verify_playwright():
    """Use headless=True so verification works even without a display. Actual scrape still uses headless=False when you run it."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
        return True
    except Exception as e:
        logger.error(f"Playwright verification failed: {e}")
        return False


def _install_playwright_browsers():
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True, text=True, timeout=300000,
        )
        return result.returncode == 0
    except Exception as e:
        logger.error(f"Browser install error: {e}")
        return False


PLAYWRIGHT_AVAILABLE = False
if _setup_playwright():
    PLAYWRIGHT_AVAILABLE = _verify_playwright()
    if not PLAYWRIGHT_AVAILABLE:
        PLAYWRIGHT_AVAILABLE = _install_playwright_browsers()

import pandas as pd
if PLAYWRIGHT_AVAILABLE:
    from playwright.async_api import async_playwright

# Timeouts and config
TIMEOUT_UI_SETTLE = 250
TIMEOUT_TABLE_LOAD = 8000
DROPDOWN_LOAD_MS = 800
TIMEOUT_PAGINATION = 450
ROWS_PER_PAGE = "1000"

PAID_APP_NOT_FOUND = "Paid application filter not found"
INSTITUTE_NOT_FOUND = "Institute not found"
SOURCE_NOT_FOUND = "Source not found"

# Bright Data Residential Proxy — used for batch / Jobs paid scrape (Playwright context).
# Override with env BRIGHT_DATA_PROXY_SERVER, BRIGHT_DATA_PROXY_USERNAME, BRIGHT_DATA_PROXY_PASSWORD (all three required to override).


def _proxy_config():
    server = (os.environ.get("BRIGHT_DATA_PROXY_SERVER") or "").strip()
    user = (os.environ.get("BRIGHT_DATA_PROXY_USERNAME") or "").strip()
    pwd = (os.environ.get("BRIGHT_DATA_PROXY_PASSWORD") or "").strip()
    if server and user and pwd:
        return {"server": server, "username": user, "password": pwd}
    return {
        "server": "http://brd.superproxy.io:33335",
        "username": "brd-customer-hl_a4a3b5b0-zone-npf_daily_reporting",
        "password": "p4zbp6ny1es5",
    }


PROXY = _proxy_config()

TIMEOUT_GOTO = 90000 if PROXY else 45000
TIMEOUT_NETWORK_IDLE = 30000 if PROXY else 15000
TIMEOUT_COMBOBOX = 35000 if PROXY else 18000
RETRY_MAX_ATTEMPTS = 5 if PROXY else 3
RETRY_DELAY_BASE_MS = 2000 if PROXY else 1000


def _is_proxy_network_error(err):
    if not err:
        return False
    s = str(err).lower()
    return any(x in s for x in ("tunnel", "net::", "timeout", "connection", "econnreset", "network", "intercepts pointer"))


def _is_retryable_error(e):
    if e is None:
        return False
    msg = str(e).lower()
    err_type = type(e).__name__.lower()
    return (
        "timeout" in msg or "timeout" in err_type or "net::" in msg or "network" in msg
        or "connection" in msg or "econnreset" in msg or "target closed" in msg or "page closed" in msg
    )


def _extract_records(obj):
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


def _flatten_record(rec):
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


def _record_to_row(rec, headers):
    if isinstance(rec, list):
        row = {}
        for i, h in enumerate(headers):
            row[h] = "" if i >= len(rec) else ("" if rec[i] is None else str(rec[i]))
        for i in range(len(headers), len(rec)):
            row[f"Col_{i+1}"] = "" if rec[i] is None else str(rec[i])
        return row
    if isinstance(rec, dict):
        return _flatten_record(rec)
    return {"value": str(rec)}


def _parse_status_date(status_val):
    if status_val is None:
        return None
    if isinstance(status_val, datetime):
        return status_val.date()
    if isinstance(status_val, date):
        return status_val
    if not str(status_val).strip():
        return None
    s = str(status_val).strip()
    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            pass
    parts = re.split(r"[/\-.]", s)
    if len(parts) == 3:
        try:
            p = [int(x) for x in parts]
            if p[2] > 31:
                return date(p[2], p[0], p[1])
            return date(p[2], p[0], p[1])
        except (ValueError, TypeError):
            pass
    return None


def _get_date_range():
    today = date.today()
    start_date_obj = date(2025, 10, 1)
    end_date_obj = today
    return start_date_obj.strftime("%d-%m-%Y"), end_date_obj.strftime("%d-%m-%Y")


def _parse_date_for_filter(date_str):
    s = str(date_str).strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            d = datetime.strptime(s, fmt).date()
            return d.year, d.month, d.day
        except (ValueError, TypeError):
            continue
    raise ValueError(f"Could not parse date '{date_str}'")


def _safe_filename(college_name):
    unsafe = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
    s = str(college_name).strip()
    for c in unsafe:
        s = s.replace(c, '_')
    return s[:100] if s else "export"


def _get_university_data_path(university, start_date, end_date, run_date=None, output_base=None):
    """Return (folder, filename). If output_base set: output_base/dd-mm-yy/university_name/."""
    run_date = run_date or date.today()
    safe_uni = _safe_filename(university)
    safe_start = str(start_date).replace("/", "-").replace("\\", "-")
    safe_end = str(end_date).replace("/", "-").replace("\\", "-")
    filename = f"{safe_uni}_{safe_start}_{safe_end}.csv"
    if output_base:
        date_folder = run_date.strftime("%d-%m-%y")
        day_folder = os.path.join(output_base, date_folder, safe_uni)
        return day_folder, filename
    data_root = paths.DATA_EXPORTS_FALLBACK
    date_folder = run_date.strftime("%Y-%m-%d")
    day_folder = os.path.join(data_root, date_folder)
    return day_folder, filename


class BatchScraper:
    """Scraper for NPF Paid Applications. No Google Sheets; used with scrape_list + output_base."""

    def __init__(self):
        self.scraping = True

    def log(self, msg):
        logger.info(msg)

    async def _select_ng_combobox(self, page, combobox_locator, search_text, err_if_empty):
        await combobox_locator.click()
        await page.wait_for_timeout(200)
        await combobox_locator.fill(search_text)
        await page.wait_for_timeout(DROPDOWN_LOAD_MS)
        selectable = await page.locator("ng-dropdown-panel .ng-option:not(.ng-option-disabled)").count()
        no_items = await page.locator("ng-dropdown-panel .ng-option-disabled:has-text('No items found')").count() > 0
        if selectable == 0 or no_items:
            raise Exception(err_if_empty)
        await combobox_locator.press("ArrowDown")
        await page.wait_for_timeout(100)
        await combobox_locator.press("Enter")
        await page.wait_for_timeout(350)

    async def select_date_range(self, page, start_date_str, end_date_str):
        start_year, start_month, start_day = _parse_date_for_filter(start_date_str)
        end_year, end_month, end_day = _parse_date_for_filter(end_date_str)
        await page.wait_for_selector("div.dateField", timeout=10000)
        loader = page.locator("div.fixed-loader")
        if await loader.count() > 0:
            try:
                if await loader.first.is_visible():
                    await loader.first.wait_for(state="hidden", timeout=12000)
                    await page.wait_for_timeout(200)
            except Exception:
                pass
        try:
            await page.click("div.dateField .dropdown-toggle", timeout=10000)
        except Exception:
            await page.click("div.dateField", timeout=10000)
        await page.wait_for_timeout(400)
        await page.wait_for_selector("ngb-datepicker", timeout=10000)
        await page.select_option("ngb-datepicker-navigation-select select:first-of-type", value=str(start_month))
        await page.wait_for_timeout(200)
        await page.select_option("ngb-datepicker-navigation-select select:last-of-type", label=str(start_year))
        await page.wait_for_timeout(200)
        await page.click(f'ngb-datepicker-month-view .ngb-dp-day span:has-text("{start_day}"):not(.text-muted)')
        await page.wait_for_timeout(400)
        if start_month != end_month or start_year != end_year:
            await page.select_option("ngb-datepicker-navigation-select select:first-of-type", value=str(end_month))
            await page.wait_for_timeout(200)
            await page.select_option("ngb-datepicker-navigation-select select:last-of-type", label=str(end_year))
            await page.wait_for_timeout(200)
        await page.click(f'ngb-datepicker-month-view .ngb-dp-day span:has-text("{end_day}"):not(.text-muted)')
        await page.wait_for_timeout(300)
        self.log(f"Date range: {start_date_str} - {end_date_str}")

    async def apply_paid_application_filter(self, page):
        await page.click("button:has-text('Advance Filter')", timeout=5000)
        await page.wait_for_timeout(350)
        try:
            await page.locator('label[for="u_payment_approved"]').click(timeout=4000)
        except Exception:
            try:
                await page.check("#u_payment_approved", timeout=4000)
            except Exception:
                raise Exception(PAID_APP_NOT_FOUND)
        await page.wait_for_timeout(200)
        await page.locator("div.filtersbtn button.btn-success:has-text('Apply')").click(timeout=5000)
        await page.wait_for_timeout(400)
        await page.locator(".ng-select-container input[role='combobox']").last.click(timeout=4000)
        await page.wait_for_timeout(300)
        await page.locator(".ng-option-label:has-text('Yes')").first.click(timeout=4000)
        await page.wait_for_timeout(200)

    def _is_leads_response(self, res):
        url_lower = (res.url or "").lower()
        return 200 <= res.status < 300 and any(
            x in url_lower for x in [
                "lead", "viewlist", "leaddetails", "campaigndetails",
                "getlead", "getcampaign", "detailsviewlist"
            ]
        )

    async def _scrape_table_via_dom(self, page):
        try:
            await page.wait_for_selector("app-leadsdatatable table thead tr th", timeout=8000)
            headers = await page.locator("app-leadsdatatable table thead tr th").all_text_contents()
            headers = [h.strip() for h in headers if h.strip()]
            all_rows = []
            page_num = 1
            while self.scraping:
                self.log(f"DOM fallback: scraping page {page_num}...")
                await page.wait_for_selector("app-leadsdatatable table tbody tr", timeout=10000)
                page_data = await page.evaluate("""() => {
                    const rows = document.querySelectorAll("app-leadsdatatable table tbody tr");
                    return Array.from(rows).map(row =>
                        Array.from(row.querySelectorAll("td")).map(td => td.innerText.trim())
                    );
                }""")
                for row in page_data:
                    all_rows.append(dict(zip(headers, row)))
                nav = page.locator("a[aria-label='Next']").nth(1)
                try:
                    parent = nav.locator("xpath=..")
                    cls = await parent.get_attribute("class") or ""
                    if "disabled" in cls:
                        break
                except Exception:
                    break
                await nav.click()
                page_num += 1
                await page.wait_for_timeout(TIMEOUT_PAGINATION)
            return all_rows
        except Exception as e:
            self.log(f"DOM fallback error: {e}")
            return []

    async def scrape_college(self, row, headless=False, output_base=None):
        """
        Scrape one college. Returns (success, records_count, error_msg, saved_filename).
        output_base: if set, save to output_base/dd-mm-yy/university_name/file.csv.
        """
        ensure_row_password(row)
        url = (row.get("url") or "").strip()
        email = (row.get("email") or "").strip()
        password = (row.get("pass") or "").strip()
        university = (row.get("university") or row.get("college") or "").strip()
        source = (row.get("source") or "").strip()
        file_name = (row.get("File_name") or "").strip()
        if file_name and _parse_status_date(file_name):
            file_name = ""

        if not university or university.strip() in ("-", ""):
            self.log(f"[{university or '?'}] SKIP: {INSTITUTE_NOT_FOUND}")
            return False, 0, INSTITUTE_NOT_FOUND, None

        if not all([url, email, password, university, source]):
            err = "Missing url, email, pass, university, or source"
            self.log(f"[{university or '?'}] FAIL: {err}")
            return False, 0, err, None

        if not url.startswith(("http://", "https://")):
            err = "URL must start with http:// or https://"
            self.log(f"[{university}] FAIL: {err}")
            return False, 0, err, None

        base_url = url.rstrip("/")
        if "/lead/details" not in base_url:
            if "/login" in base_url:
                base_url = base_url.replace("/login", "/lead/details")
            else:
                base_url = base_url + "/lead/details"

        params = {
            "login_url": base_url,
            "email": email,
            "password": password,
            "institute": university,
            "source": source,
            "filename": file_name or f"{_safe_filename(university)}_paid_applications.csv",
        }

        if not PLAYWRIGHT_AVAILABLE:
            return False, 0, "Playwright not available", None

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(proxy=PROXY if PROXY else None)
            page = await context.new_page()

            try:
                self.log(f"[{university}] Open URL, login...")
                for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
                    try:
                        await page.goto(params["login_url"], timeout=TIMEOUT_GOTO)
                        await page.fill('input[formcontrolname="email"]', params["email"])
                        await page.fill('input[formcontrolname="password"]', params["password"])
                        await page.click('button:has-text("Log In")')
                        await page.wait_for_load_state("networkidle", timeout=TIMEOUT_NETWORK_IDLE)
                        await page.wait_for_timeout(1200 if PROXY else 600)
                        break
                    except Exception as e:
                        if _is_retryable_error(e) and attempt < RETRY_MAX_ATTEMPTS:
                            delay_sec = min(RETRY_DELAY_BASE_MS * (2 ** (attempt - 1)) / 1000, 12)
                            self.log(f"[{university}] Login retry {attempt}/{RETRY_MAX_ATTEMPTS}: {e}")
                            await asyncio.sleep(delay_sec)
                        else:
                            raise

                self.log(f"[{university}] Campaign Detailed View (/campaign/details)…")
                await ensure_campaign_detailed_view(
                    page,
                    log_fn=lambda m: self.log(f"[{university}] {m}"),
                    timeout_goto=TIMEOUT_GOTO,
                    timeout_network=min(TIMEOUT_NETWORK_IDLE, 25000),
                    timeout_combobox=TIMEOUT_COMBOBOX,
                )
                await page.wait_for_timeout(300)

                self.log(f"[{university}] Fill institute...")
                try:
                    inst_cb = page.locator('input[role="combobox"]').first
                    await self._select_ng_combobox(page, inst_cb, params["institute"], INSTITUTE_NOT_FOUND)
                    await page.wait_for_timeout(300)
                except Exception as e:
                    if str(e) != INSTITUTE_NOT_FOUND:
                        self.log(f"[{university}] Institute: {e}")
                    raise Exception(INSTITUTE_NOT_FOUND)

                self.log(f"[{university}] Fill source...")
                try:
                    src_cb = page.locator('.ng-select-container input[role="combobox"]').nth(1)
                    await self._select_ng_combobox(page, src_cb, params["source"], SOURCE_NOT_FOUND)
                    await page.wait_for_timeout(300)
                except Exception as e:
                    if str(e) != SOURCE_NOT_FOUND:
                        self.log(f"[{university}] Source: {e}")
                    raise Exception(SOURCE_NOT_FOUND)

                self.log(f"[{university}] Fill date range...")
                start_date, end_date = _get_date_range()
                await self.select_date_range(page, start_date, end_date)

                await page.click('button.btn-success.btn-sm.border-0.mr-2:has-text("Apply")', timeout=8000)
                await page.wait_for_timeout(500)

                self.log(f"[{university}] Apply Paid Applications = Yes...")
                await self.apply_paid_application_filter(page)

                self.log(f"[{university}] Search...")
                await page.click('button.btn-success.btn-c-size.border-0.mr-2.mt-2:has-text("Search")', timeout=8000)
                await page.wait_for_timeout(1200)
                await page.wait_for_selector("app-leadsdatatable, app-advanceview, .card-body, .card", timeout=TIMEOUT_TABLE_LOAD)

                no_records = page.get_by_text("No Record Found", exact=False)
                if await no_records.count() > 0:
                    try:
                        if await no_records.first.is_visible():
                            self.log(f"[{university}] No records for the selected filters")
                            await context.close()
                            await browser.close()
                            return True, 0, "", None
                    except Exception:
                        pass

                try:
                    edit_btn = page.get_by_role("button", name=re.compile(r"edit|column", re.I))
                    if await edit_btn.count() > 0:
                        await edit_btn.first.click(timeout=5000)
                        await page.wait_for_timeout(200)
                        select_all = page.locator("label").filter(has_text=re.compile(r"select all|selectall", re.I))
                        if await select_all.count() > 0:
                            await select_all.first.click(timeout=3000)
                            ap = page.get_by_role("button", name="Apply")
                            if await ap.count() > 0:
                                await ap.first.click(timeout=3000)
                        await page.wait_for_timeout(200)
                except Exception:
                    pass

                headers = []
                try:
                    await page.wait_for_selector("app-leadsdatatable table thead tr th", timeout=8000)
                    headers = await page.locator("app-leadsdatatable table thead tr th").all_text_contents()
                    headers = [h.strip() for h in headers if h.strip()]
                except Exception:
                    pass

                API_KEYWORDS = ["leaddetails", "campaigndetails", "viewlist", "getlead", "getcampaign"]
                last_captured = [None]

                async def capture_route(route):
                    req = route.request
                    if any(kw in (req.url or "").lower() for kw in API_KEYWORDS):
                        body = getattr(req, "post_data", None) or ""
                        last_captured[0] = (req.url, req.method, dict(req.headers or {}), body or "")
                    await route.continue_()

                await page.route("**/*", capture_route)

                async def fetch_with_captured(cap):
                    if not cap:
                        return None
                    url_u, method, headers_h, body = cap
                    headers_h = {k: v for k, v in headers_h.items() if k.lower() != "content-length"}
                    try:
                        api_resp = await page.request.fetch(url_u, method=method or "POST", headers=headers_h, data=body or "")
                        if api_resp.ok:
                            return await api_resp.json()
                    except Exception:
                        pass
                    return None

                rows_value = ROWS_PER_PAGE
                collected = []
                last_captured[0] = None
                rpp_resp_ctx = None
                try:
                    rpp_select = page.locator("select").nth(2)
                    if await rpp_select.count() > 0:
                        await page.evaluate(f"""
                            const dropdown = document.querySelectorAll('select')[2];
                            if (dropdown) {{
                                if (!Array.from(dropdown.options).some(o => o.value === '{rows_value}')) {{
                                    const opt = document.createElement('option');
                                    opt.value = '{rows_value}';
                                    opt.text = '{rows_value}';
                                    dropdown.appendChild(opt);
                                }}
                                dropdown.value = '{rows_value}';
                                dropdown.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            }}
                        """)
                        async with page.expect_response(self._is_leads_response, timeout=15000) as rpp_resp_ctx:
                            await rpp_select.select_option(rows_value, timeout=8000)
                    else:
                        self.log("Rows-per-page select not found - using default")
                except Exception as e:
                    self.log(f"Rows-per-page select failed: {e}")
                await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
                cap = last_captured[0]
                data = await fetch_with_captured(cap)
                if not data and rpp_resp_ctx is not None:
                    try:
                        resp = await rpp_resp_ctx.value
                        data = await resp.json()
                    except (NameError, Exception):
                        pass
                if data:
                    records = _extract_records(data)
                    collected.append({"data": data, "records": records})
                    self.log(f"[{university}] Page 1: {len(records)} records")

                await page.wait_for_timeout(TIMEOUT_PAGINATION)

                page_num = 1
                while self.scraping:
                    nav_button = page.locator("a[aria-label='Next']").nth(1)
                    try:
                        parent = nav_button.locator("xpath=..")
                        cls = await parent.get_attribute("class") or ""
                        if "disabled" in cls:
                            break
                    except Exception:
                        break
                    page_num += 1
                    last_captured[0] = None
                    try:
                        async with page.expect_response(self._is_leads_response, timeout=15000) as nav_resp_ctx:
                            await nav_button.click()
                        await page.wait_for_timeout(TIMEOUT_UI_SETTLE)
                        cap = last_captured[0]
                        data = await fetch_with_captured(cap)
                        if not data:
                            try:
                                resp = await nav_resp_ctx.value
                                data = await resp.json()
                            except Exception:
                                pass
                        if data:
                            records = _extract_records(data)
                            collected.append({"data": data, "records": records})
                            if len(records) < int(rows_value):
                                break
                    except Exception as ex:
                        self.log(f"[{university}] Pagination error on page {page_num}: {ex}")
                        break
                    await page.wait_for_timeout(TIMEOUT_PAGINATION)

                try:
                    await page.unroute("**/*")
                except Exception:
                    pass

                all_records = []
                for item in collected:
                    all_records.extend(item.get("records", []))

                if not all_records:
                    self.log(f"[{university}] No API data – falling back to DOM scraping")
                    all_records = await self._scrape_table_via_dom(page)

                if all_records:
                    if not headers:
                        sample = all_records[0]
                        headers = list(sample.keys()) if isinstance(sample, dict) else [f"Col_{i+1}" for i in range(len(sample))]
                    rows = [_record_to_row(r, headers) if headers else _flatten_record(r) for r in all_records]
                    df = pd.DataFrame(rows)
                    df = drop_phone_mobile_columns(df)
                    df.insert(0, "pcid", row.get("pcid") or "")
                    df.insert(1, "FI", row.get("FI") or "")
                    day_folder, fname = _get_university_data_path(
                        university, start_date, end_date, output_base=output_base
                    )
                    os.makedirs(day_folder, exist_ok=True)
                    file_path = os.path.join(day_folder, fname)
                    df.to_csv(file_path, index=False, encoding="utf-8", header=True)
                    self.log(f"[{university}] Saved {len(all_records)} records to {file_path}")
                    await context.close()
                    await browser.close()
                    return True, len(all_records), "", fname
                else:
                    self.log(f"[{university}] No records captured")
                    await context.close()
                    await browser.close()
                    return True, 0, "", None

            except Exception as e:
                self.log(f"[{university}] FAIL: {e}")
                try:
                    await context.close()
                    await browser.close()
                except Exception:
                    pass
                return False, 0, str(e), None
