"""
Taken from https://airflow.apache.org/docs/apache-airflow/stable/security/webserver.html#example-using-team-based-authorization-with-github-oauth
and adapted for organisations
"""
import os

from flask_appbuilder.const import AUTH_DB, AUTH_OAUTH


if os.environ.get("OAUTH_APP_ID") and os.environ.get("OAUTH_APP_SECRET"):
    AUTH_TYPE = AUTH_OAUTH
    FAB_SECURITY_MANAGER_CLASS = (
        "digital_land_airflow.security_manager.GithubOrgAuthorizer"
    )
else:
    AUTH_TYPE = AUTH_DB
AUTH_ROLES_SYNC_AT_LOGIN = True  # Checks roles on every login
AUTH_USER_REGISTRATION = (
    True  # allow users who are not already in the FAB DB to register
)

# Make sure to replace this with the path to your security manager class
AUTH_ROLES_MAPPING = {
    "Viewer": ["Viewer"],
    "Admin": ["Admin"],
}

# If you wish, you can add multiple OAuth providers.
OAUTH_PROVIDERS = [
    {
        "name": "github",
        "icon": "fa-github",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.getenv("OAUTH_APP_ID"),
            "client_secret": os.getenv("OAUTH_APP_SECRET"),
            "api_base_url": "https://api.github.com",
            "client_kwargs": {"scope": "read:user, read:org"},
            "access_token_url": "https://github.com/login/oauth/access_token",
            "authorize_url": "https://github.com/login/oauth/authorize",
            "request_token_url": None,
        },
    },
]
