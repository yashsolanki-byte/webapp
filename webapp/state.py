"""In-memory state shared by route handlers (manual scrape UI)."""

manual_scrape_status = {
    "running": False,
    "job_id": "",
    "status": "",
    "output_path": "",
    "output_path_relative": "",
    "output_download_file": "",
    "error": None,
}
