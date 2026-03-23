"""
After NPF login: open Campaigns → Detailed View (/campaign/details), then filters apply.

Sidebar (Angular) — match publisher UI:
  <a class="list-group-item campaignsClass">Campaigns</a>
  <ul class="submenu" data-name="Campaigns">
    <a class="list-group-item detailed view" href="/campaign/details">Detailed View</a>

Flow: login → Campaigns → Detailed View → wait for combobox → short settle so institute
ng-select can load options before the scraper searches.

Settle time: kwarg settle_after_detailed_ms, or env NPF_DETAILED_VIEW_SETTLE_MS (default 2500 ms).
Fallback: direct navigation to {origin}/campaign/details if the sidebar path fails.
"""
from __future__ import annotations

import os
from typing import Callable, Optional
from urllib.parse import urlparse, urljoin


def _default_settle_ms() -> int:
    raw = os.getenv("NPF_DETAILED_VIEW_SETTLE_MS", "2500").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 2500


def _path_is_campaign_details(path: str) -> bool:
    p = (path or "").rstrip("/")
    return p.endswith("/campaign/details") or p == "/campaign/details"


async def _combobox_ready(page, timeout_ms: int) -> bool:
    try:
        await page.wait_for_selector(
            'input[role="combobox"], .ng-input',
            timeout=timeout_ms,
        )
        return True
    except Exception:
        return False


async def _settle_after_detailed_view(
    page,
    settle_ms: int,
    log_fn: Optional[Callable[[str], None]],
) -> None:
    """Give the institute (and related) dropdowns time to fetch options after route load."""
    if settle_ms <= 0:
        return
    if log_fn:
        log_fn(
            f"Waiting {settle_ms}ms after Detailed View for institute list / filters to load…"
        )
    try:
        await page.wait_for_load_state("networkidle", timeout=min(10000, settle_ms + 5000))
    except Exception:
        pass
    await page.wait_for_timeout(settle_ms)


async def ensure_campaign_detailed_view(
    page,
    log_fn: Optional[Callable[[str], None]] = None,
    *,
    timeout_goto: int = 45000,
    timeout_network: int = 20000,
    timeout_combobox: int = 20000,
    timeout_sidebar: int = 20000,
    settle_after_detailed_ms: Optional[int] = None,
) -> None:
    """After login: open Campaigns → Detailed View, wait for UI, then settle before institute pick."""

    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)

    settle_ms = (
        settle_after_detailed_ms
        if settle_after_detailed_ms is not None
        else _default_settle_ms()
    )

    cur = (page.url or "").split("#")[0]
    parsed = urlparse(cur)
    if not parsed.scheme or not parsed.netloc:
        log("ensure_campaign_detailed_view: skip (no origin in URL)")
        return

    path = parsed.path or ""
    if _path_is_campaign_details(path):
        log("Already on Campaign Detailed View")
        await page.wait_for_timeout(400)
        await page.wait_for_selector(
            'input[role="combobox"], .ng-input',
            timeout=timeout_combobox,
        )
        await _settle_after_detailed_view(page, settle_ms, log_fn)
        return

    origin = f"{parsed.scheme}://{parsed.netloc}"
    target = urljoin(origin + "/", "campaign/details")

    await page.wait_for_timeout(400)

    # --- Sidebar: Campaigns → Detailed View ---
    sidebar_ok = False
    try:
        log("Sidebar: Campaigns → Detailed View")
        await page.wait_for_selector(
            'a.campaignsClass, a.list-group-item.campaignsClass, a[href="/campaign/details"], '
            'a[href*="/campaign/details"], .nested-menu',
            timeout=timeout_sidebar,
        )

        detailed_selectors = (
            'ul.submenu[data-name="Campaigns"] a[href="/campaign/details"]',
            'ul.submenu[data-name="Campaigns"] a[href*="/campaign/details"]',
            'a.list-group-item.detailed.view',
            'a.detailed.view',
            'a[href="/campaign/details"]',
            'a[href*="/campaign/details"]',
        )

        def _visible_detailed():
            return page.locator(
                'ul.submenu[data-name="Campaigns"] a[href="/campaign/details"], '
                'a.list-group-item.detailed.view, a[href="/campaign/details"]'
            ).first

        clicked = False
        det = _visible_detailed()
        if await det.count() > 0:
            try:
                if await det.is_visible():
                    await det.click(timeout=10000)
                    clicked = True
                    log("Clicked Detailed View (visible without expanding)")
            except Exception:
                clicked = False

        if not clicked:
            camp = page.locator("a.list-group-item.campaignsClass, a.campaignsClass").first
            if await camp.count() > 0:
                await camp.click(timeout=12000)
                await page.wait_for_timeout(500)

            try:
                await page.get_by_role("link", name="Detailed View").click(timeout=10000)
                clicked = True
                log("Clicked Detailed View (accessible name)")
            except Exception:
                for sel in detailed_selectors:
                    loc = page.locator(sel).first
                    if await loc.count() == 0:
                        continue
                    try:
                        await loc.click(timeout=12000)
                        clicked = True
                        log(f"Clicked Detailed View ({sel})")
                        break
                    except Exception:
                        continue

        if clicked:
            try:
                await page.wait_for_url("**/campaign/details**", timeout=min(timeout_network, 25000))
            except Exception:
                pass
            try:
                await page.wait_for_load_state("networkidle", timeout=min(timeout_network, 15000))
            except Exception:
                pass
            await page.wait_for_timeout(800)

        # Success if filter UI is ready (URL may lag in some builds)
        if await _combobox_ready(page, min(12000, timeout_combobox)):
            sidebar_ok = True
            log("Campaign Detailed View UI ready (after sidebar)")
    except Exception as e:
        log(f"Sidebar navigation failed ({e})")

    if not sidebar_ok:
        log(f"Fallback: goto {target}")
        try:
            await page.goto(target, wait_until="domcontentloaded", timeout=timeout_goto)
            try:
                await page.wait_for_load_state("networkidle", timeout=timeout_network)
            except Exception:
                pass
            await page.wait_for_timeout(800)
        except Exception as e2:
            raise RuntimeError(
                "Could not open Campaign Detailed View (/campaign/details). "
                f"Sidebar and direct URL both failed: {e2}"
            ) from e2

    await page.wait_for_selector(
        'input[role="combobox"], .ng-input',
        timeout=timeout_combobox,
    )
    await _settle_after_detailed_view(page, settle_ms, log_fn)
