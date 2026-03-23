"""
NPF Scraper Web App factory.
Keeps all data paths under the project root (npf-scraper-webapp/); only Python code is split by section.
"""
import os

from flask import Flask

import project_paths
from credential_env import load_npf_dotenv
from webapp.config import TEMPLATES_DIR
from webapp.routes import register_blueprints


def create_app():
    project_paths.ensure_layout_migrated()
    load_npf_dotenv()
    # Disable Flask reloader so scrape threads are not killed when Playwright touches files
    os.environ["FLASK_RUN_RELOAD"] = "0"

    app = Flask(__name__, template_folder=TEMPLATES_DIR)
    register_blueprints(app)
    return app
