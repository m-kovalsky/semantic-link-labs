import sempy
import sempy.fabric as fabric
from typing import Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_id,
    resolve_lakehouse_id,
)
import sempy_labs._icons as icons
from sempy_labs.lakehouse import lakehouse_attached
from sempy_labs.tom import connect_semantic_model
from sempy.fabric.exceptions import FabricHTTPException


@log
def export_model_to_onelake(
    dataset: str,
    workspace: Optional[str] = None,
    destination_lakehouse: Optional[str] = None,
    destination_workspace: Optional[str] = None,
    create_shortcuts: Optional[bool] = True,
):
    """
    Exports a semantic model's tables to delta tables in the lakehouse using the `OneLake Integration technique <https://learn.microsoft.com/power-bi/enterprise/onelake-integration-overview>`_.
    Creates shortcuts to the tables if the create_shortcuts parameter is set to True.

    Requirements:
    * `Enable OneLake Integration <https://learn.microsoft.com/power-bi/enterprise/onelake-integration-overview#enable-onelake-integration>`_.
    * `Enable XMLA Read/Write <https://learn.microsoft.com/power-bi/enterprise/service-premium-connect-tools#enable-xmla-read-write>`_.


    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    destination_lakehouse : str, default=None
        The name of the Fabric lakehouse where shortcuts will be created to access the delta tables created by the export.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    destination_workspace : str, default=None
        The name of the Fabric workspace in which the lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    create_shortcuts : bool, default=True
        If True, creates shortcuts for all of the tables.
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    (destination_workspace, destination_workspace_id) = resolve_workspace_name_and_id(
        destination_workspace
    )

    dataset_id = resolve_dataset_id(dataset, workspace)

    tmsl = f"""
        {{
        'export': {{
        'layout': 'delta',
        'type': 'full',
        'objects': [
            {{
            'database': '{dataset}'
            }}
        ]
        }}
        }}
    """

    # Export model's tables as delta tables
    fabric.execute_tmsl(script=tmsl, workspace=workspace)
    print(
        f"{icons.green_dot} The '{dataset}' semantic model's tables have been exported as delta tables to the '{workspace}' workspace.\n"
    )

    if create_shortcuts:
        # Create shortcuts if destination lakehouse is specified
        if destination_lakehouse is None:
            if not lakehouse_attached():
                raise ValueError(
                    f"{icons.red_dot} In order to create shortcuts, a lakehouse must be attached to the notebook or a lakehouse must be specified in the parameters of this function."
                )
            destination_lakehouse_id = fabric.get_lakehouse_id()
            destination_workspace_id = fabric.get_workspace_id()
        else:
            destination_lakehouse_id = resolve_lakehouse_id(
                destination_lakehouse, destination_workspace
            )

        # Source...
        dfI_Source = fabric.list_items(workspace=workspace, type="SemanticModel")
        dfI_filtSource = dfI_Source[(dfI_Source["Display Name"] == dataset)]
        sourceLakehouseId = dfI_filtSource["Id"].iloc[0]

        client = fabric.FabricRestClient()

        print(f"{icons.in_progress} Creating shortcuts...\n")
        with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
            for t in tom.model.Tables:
                tableName = t.Name
                if (
                    t.CalculationGroup is None
                    and t.SystemManaged is False
                    and all(p.Mode == TOM.ModeType.Import for p in t.Partitions)
                    and t.Columns.Count > 0
                ):
                    tablePath = f"Tables/{tableName}"
                    shortcutName = tableName.replace(" ", "")
                    request_body = {
                        "path": "Tables",
                        "name": shortcutName,
                        "target": {
                            "oneLake": {
                                "workspaceId": workspace_id,
                                "itemId": sourceLakehouseId,
                                "path": tablePath,
                            }
                        },
                    }

                    response = client.post(
                        f"/v1/workspaces/{destination_workspace_id}/items/{destination_lakehouse_id}/shortcuts",
                        json=request_body,
                    )

                    if response.status_code != 201:
                        raise FabricHTTPException(response)
                    print(
                        f"{icons.green_dot} The shortcut '{shortcutName}' was created in the '{destination_lakehouse}' lakehouse within the '{destination_workspace}' workspace. It is based on the '{tableName}' table in the '{dataset}' semantic model within the '{workspace}' workspace.\n"
                    )
