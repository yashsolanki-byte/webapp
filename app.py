"""
NPF Scraper Web App — entry point.

Run from this folder:
  python app.py

Or:
  set FLASK_APP=app:app
  flask run --no-reload

Layout: project_paths.py + data/{history,reference,runtime} + logs/{app,runs,script} + scrapers/ + webapp/ (see README.md).
"""
import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from webapp import create_app

app = create_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
