import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _conv_b64,
)
from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


def create_dataflow(dataflow_name: str, mashup_document: str, description: Optional[str] = None, workspace: Optional[str] = None):

    """
    Creates a Dataflow Gen2 based on a mashup document.

    Parameters
    ----------
    dataflow_name : str
        Name of the dataflow.
    mashup_docment : str
        The mashup document (use the '' function).
    description : str, default=None
        The description of the dataflow.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        'displayName': dataflow_name,
        'type': 'Dataflow',
    }

    if description is not None:
        payload['description'] = description

    dataflow_def_payload = {
        'editingSessionMashup': {
            'mashupName': '',
            'mashupDocument': mashup_document,
            'queryGroups': [],
            'documentLocale': 'en-US',
            'gatewayObjectId': None,
            'queriesMetadata': None,
            'connectionOverrides': [],
            'trustedConnections': None,
            'useHostConnectionProvider': False,
            'fastCombine': False,
            'allowNativeQueries': True,
            'allowedModules': None,
            'skipAutomaticTypeAndHeaderDetection': False,
            'disableAutoAnonymousConnectionUpsert': None,
            'hostProperties': {
                'DataflowRefreshOutpuFileFormat': 'Parquet',
                'EnableDataTimeFieldsForStaging': True,
                'EnablePublishWithoutLoadedQueries': True,
            },
            'defaultOutputDestinationConfiguration': None,
            'stagingDefinition': None,
        }
    }

    dataflow_def_payload_conv = _conv_b64(dataflow_def_payload)

    payload['definition'] = {
        'parts': [
            {
                'path': 'dataflow-content.json',
                'payload': dataflow_def_payload_conv,
            }
        ]
    }

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/items", json=payload
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{dataflow_name}' has been created within the '{workspace}' workspace.")
