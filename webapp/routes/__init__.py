"""Register Flask blueprints (one module per UI section)."""


def register_blueprints(app):
    from webapp.routes.dashboard import dashboard_bp
    from webapp.routes.feedback_ready import feedback_ready_bp
    from webapp.routes.institutes import institutes_bp
    from webapp.routes.logs import logs_bp
    from webapp.routes.manual_scrape import manual_scrape_bp
    from webapp.routes.pages import pages_bp
    from webapp.routes.scrape_job import scrape_job_bp
    from webapp.routes.upload import upload_bp
    from webapp.routes.stop_jobs import stop_jobs_bp
    from webapp.routes.settings_info import settings_info_bp

    app.register_blueprint(pages_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(feedback_ready_bp)
    app.register_blueprint(institutes_bp)
    app.register_blueprint(manual_scrape_bp)
    app.register_blueprint(scrape_job_bp)
    app.register_blueprint(logs_bp)
    app.register_blueprint(upload_bp)
    app.register_blueprint(stop_jobs_bp)
    app.register_blueprint(settings_info_bp)
