import logging
import os
from typing import Dict, Any, List, Union

from airflow.www.security import AirflowSecurityManager


log = logging.getLogger(__name__)
log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))

FAB_ADMIN_ROLE = "Admin"
FAB_VIEWER_ROLE = "Viewer"
FAB_PUBLIC_ROLE = "Public"  # The "Public" role is given no permissions
ORG_ID_FROM_GITHUB = os.getenv("GITHUB_ORG_ID")


def org_parser(org_payload: Dict[str, Any]) -> List[int]:
    # Parse the org payload from Github however you want here.
    return [org["id"] for org in org_payload]


def map_roles(org_list: List[int]) -> List[str]:
    # Associate the org IDs with Roles here.
    # The expected output is a list of roles that FAB will use to Authorize the user.

    if not ORG_ID_FROM_GITHUB:
        raise Exception("Missing GITHUB_ORG_ID env var!")
    org_role_map = {
        int(ORG_ID_FROM_GITHUB): FAB_ADMIN_ROLE,
    }
    log.debug(f"org_role_map: {org_role_map}")
    return list(set(org_role_map.get(org, FAB_PUBLIC_ROLE) for org in org_list))


class GithubOrgAuthorizer(AirflowSecurityManager):

    # In this example, the oauth provider == 'github'.
    # If you ever want to support other providers, see how it is done here:
    # https://github.com/dpgaspar/Flask-AppBuilder/blob/master/flask_appbuilder/security/manager.py#L550

    def get_oauth_user_info(
        self, provider: str, resp: Any
    ) -> Dict[str, Union[str, List[str]]]:

        # Creates the user info payload from Github.
        # The user previously allowed your app to act on their behalf,
        #   so now we can query the user and orgs endpoints for their data.
        # Username and org membership are added to the payload and returned to FAB.

        try:
            remote_app = self.appbuilder.sm.oauth_remotes[provider]
            me = remote_app.get("user")
            user_data = me.json()
            org_data = remote_app.get("user/orgs")
            orgs = org_parser(org_data.json())
            roles = map_roles(orgs)
            if user_data.get("name"):
                name_list = user_data["name"].split(" ")
            else:
                name_list = ["Anony", "mouse"]
            if len(name_list) >= 2:
                first_name = name_list[0]
                last_name = name_list[-1]
            elif len(name_list) == 1:
                first_name = name_list[0]
                last_name = ""
            else:
                first_name = ""
                last_name = ""
            log.debug(
                f"User info from Github: {user_data}\n"
                f"Org info from Github: {orgs}\n"
                f"Org ID granting access {ORG_ID_FROM_GITHUB}"
            )
            return {
                "username": "github_" + user_data.get("login"),
                "first_name": first_name,
                "last_name": last_name,
                "role_keys": roles,
                "email": user_data.get("email"),
            }
        except Exception:
            log.exception(
                "Exception in processing oauth user info, defaulting to public role",
                exc_info=True,
            )
            return {"username": "Anonymous", "role_keys": [FAB_PUBLIC_ROLE]}
