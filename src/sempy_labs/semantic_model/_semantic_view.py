from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_lakehouse_name_and_id,
    resolve_workspace_name_and_id,
    _base_api,
)
from sempy._utils._log import log
import sempy_labs._icons as icons
from urllib.parse import quote


@log
def publish_semantic_view(
    semantic_view: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Publishes a semantic view in a lakehouse.

    Publishing compiles the view's YAML definition (the files in the
    lakehouse's Context/Semantic Views/<semantic_view> folder) into a
    read-only Direct Lake semantic model. Republishing the same view updates
    the same semantic model.

    This is a wrapper function for the following API: `Semantic Views - Publish (beta) <https://learn.microsoft.com/rest/api/fabric/lakehouse/semantic-views/publish(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    semantic_view : str
        Name of the semantic view (its folder in Context/Semantic Views).
    lakehouse : str | uuid.UUID, default=None
        Name or ID of the lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The publish result returned by the service: the generated semantic
        model ("semanticModel") and any non-fatal "warnings".
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    lakehouse_name, lakehouse_id = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/semanticViews/{quote(semantic_view, safe='')}/publish?beta=True",
        method="post",
        status_codes=[200, 202],
        lro_return_json=True,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{semantic_view}' semantic view in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been published."
    )

    return result if isinstance(result, dict) else {}
