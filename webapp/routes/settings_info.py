"""Settings UI: non-secret auth / credentials summary."""

import os

from flask import Blueprint, jsonify

from webapp.config import APP_DIR

settings_info_bp = Blueprint("settings_info", __name__, url_prefix="")


@settings_info_bp.route("/api/settings/auth-summary")
def api_settings_auth_summary():
    """Paths and flags only — never returns passwords or secret keys."""
    try:
        import project_paths as paths
    except ImportError:
        paths = None

    cred_path = getattr(paths, "CREDENTIALS_JSON", "") if paths else ""
    cred_exists = bool(cred_path and os.path.isfile(cred_path))

    env_path = os.path.join(APP_DIR, ".env")
    env_exists = os.path.isfile(env_path)

    profiles = []
    try:
        from credential_env import PROFILE_EMAILS, password_for_profile

        for profile, email in PROFILE_EMAILS.items():
            profiles.append(
                {
                    "profile": profile,
                    "email": email,
                    "passwordConfigured": bool(password_for_profile(profile)),
                }
            )
    except Exception:
        pass

    drive_folder = (os.environ.get("DRIVE_FOLDER_ID") or "").strip()

    return jsonify(
        {
            "credentialsJsonPath": cred_path,
            "credentialsJsonExists": cred_exists,
            "envFilePath": env_path,
            "envFileExists": env_exists,
            "npfProfiles": profiles,
            "driveFolderIdConfigured": bool(drive_folder),
            "driveFolderIdNote": "Set DRIVE_FOLDER_ID in .env to override the default Shared Drive folder (see upload code).",
        }
    )
