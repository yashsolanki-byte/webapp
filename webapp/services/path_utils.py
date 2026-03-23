"""Safe path helpers for logs and manual scrape output."""
import os
from datetime import datetime


def get_user_downloads_dir():
    """
    OS Downloads folder for the user running the Flask process (local app = your PC).
    On Windows, prefers the localized path from the registry when available.
    """
    if os.name == "nt":
        try:
            import winreg

            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders",
            ) as k:
                path, _ = winreg.QueryValueEx(k, "{374DE290-123F-4565-9164-39C4925E467B}")
                expanded = os.path.normpath(os.path.expandvars(path))
                if expanded and os.path.isdir(expanded):
                    return expanded
        except OSError:
            pass
    d = os.path.normpath(os.path.join(os.path.expanduser("~"), "Downloads"))
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        pass
    return d


def unique_path_in_dir(directory: str, filename: str) -> str:
    """If filename exists in directory, append _HHMMSS before extension."""
    directory = os.path.abspath(directory)
    base, ext = os.path.splitext(filename or "export")
    base = base or "export"
    ext = ext or ".csv"
    candidate = os.path.join(directory, base + ext)
    if not os.path.exists(candidate):
        return candidate
    ts = datetime.now().strftime("%H%M%S")
    return os.path.join(directory, f"{base}_{ts}{ext}")


def safe_manual_output_name(name):
    """Safe folder/filename for manual scrape output."""
    s = (name or "").strip()
    for c in ("\\", "/", ":", "*", "?", '"', "<", ">", "|"):
        s = s.replace(c, "_")
    return s[:80] if len(s) > 80 else s or "unknown"


def safe_log_subpath(date_str, file_name):
    """Validate date and file name for logs/runs/<date>/<file> (no path traversal). Returns None if invalid."""
    if not date_str or not file_name:
        return None
    for x in (date_str, file_name):
        if ".." in x or os.path.sep in x or "/" in x or "\\" in x:
            return None
    if not file_name.endswith(".log"):
        return None
    return os.path.join(date_str, file_name)
