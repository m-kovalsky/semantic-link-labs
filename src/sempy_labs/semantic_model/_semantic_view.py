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

    Publishing validates the view's YAML definition (the files in the
    lakehouse's Context/Semantic Views/<semantic_view> folder), writes the
    compiled TMDL to SystemContext/Semantic Views/<semantic_view> and creates or
    updates the semantic model generated from the view.

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
        The publish result returned by the service (e.g. the generated semantic
        model and any warnings), or an empty dictionary if there is none.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    lakehouse_name, lakehouse_id = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    response = _base_api(
        request=f"modeling/workspaces/{workspace_id}/items/{lakehouse_id}/semanticViews/{quote(semantic_view)}/publish",
        method="post",
        status_codes=[200, 201, 202],
        client="internal",
    )

    print(
        f"{icons.green_dot} The '{semantic_view}' semantic view in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been published."
    )

    try:
        result = response.json()
    except ValueError:
        result = {}
    return result if isinstance(result, dict) else {"result": result}
