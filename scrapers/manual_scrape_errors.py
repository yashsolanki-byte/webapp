"""Controlled errors for manual scrape (RQ workers + headless)."""

from __future__ import annotations


class ManualScrapeLogicalError(Exception):
    """
    Non-retryable failure (bad institute, missing filter, etc.).
    `code` is a short machine string e.g. INSTITUTE_NOT_FOUND.
    """

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


class ManualScrapeTransientError(Exception):
    """Retryable failure (timeout, network). Worker may retry a few times."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)
