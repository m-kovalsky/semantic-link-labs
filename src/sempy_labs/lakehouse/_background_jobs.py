import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
)
import sempy_labs._icons as icons
import time
from sempy_labs._job_scheduler import _get_item_job_instance
import re


@log
def refresh_materialized_lake_views(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Refreshes materialized lake views in a lakehouse (`see more <https://learn.microsoft.com/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view>`_).

    This is a wrapper function for the following API: `Background Jobs - Run On Demand Refresh Materialized Lake Views <https://learn.microsoft.com/rest/api/fabric/lakehouse/background-jobs/run-on-demand-refresh-materialized-lake-views>`_.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the job instance details of the table maintenance operation.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/jobs/instances?jobType=RefreshMaterializedLakeViews",
        method="post",
        status_codes=202,
    )

    print(
        f"{icons.in_progress} The materialized lake views refresh for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been initiated."
    )

    status_url = response.headers.get("Location").split("fabric.microsoft.com")[1]
    status = None
    while status not in ["Completed", "Failed"]:
        response = _base_api(request=status_url)
        status = response.json().get("status")
        time.sleep(3)

    df = _get_item_job_instance(url=status_url)

    if status == "Completed":
        print(
            f"{icons.green_dot} The materialized lake views refresh for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has succeeded."
        )
    else:
        print(
            f"{icons.red_dot} The materialized lake views refresh for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has failed."
        )

    return df


@log
def run_table_maintenance(
    table_name: str,
    optimize: bool = False,
    v_order: bool = False,
    vacuum: bool = False,
    retention_period: Optional[str] = None,
    schema: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Runs table maintenance operations on the specified table within the lakehouse.

    This is a wrapper function for the following API: `Background Jobs - Run On Demand Table Maintenance <https://learn.microsoft.com/rest/api/fabric/lakehouse/background-jobs/run-on-demand-table-maintenance>`_.

    Parameters
    ----------
    table_name : str
        Name of the delta table on which to run maintenance operations.
    optimize : bool, default=False
        If True, the `OPTIMIZE <https://docs.delta.io/latest/optimizations-oss.html>`_ function will be run on the table.
    v_order : bool, default=False
        If True, v-order will be enabled for the table.
    vacuum : bool, default=False
        If True, the `VACUUM <https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table>`_ function will be run on the table.
    retention_period : str, default=None
        If specified, the retention period for the vacuum operation. Must be in the 'd:hh:mm:ss' format.
    schema : str, default=None
        The schema of the tables within the lakehouse.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the job instance details of the table maintenance operation.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    if not optimize and not vacuum:
        raise ValueError(
            f"{icons.warning} At least one of 'optimize' or 'vacuum' must be set to True."
        )
    if not vacuum and retention_period is not None:
        raise ValueError(
            f"{icons.warning} The 'retention_period' parameter can only be set if 'vacuum' is set to True."
        )
    if retention_period is not None:

        def is_valid_format(time_string):
            pattern = r"^\d+:[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$"
            return bool(re.match(pattern, time_string))

        if not is_valid_format(retention_period):
            raise ValueError(
                f"{icons.red_dot} The 'retention_period' parameter must be in the 'd:hh:mm:ss' format."
            )

    payload = {
        "executionData": {
            "tableName": table_name,
        }
    }
    if schema is not None:
        payload["executionData"]["schemaName"] = schema
    if optimize:
        payload["executionData"]["optimizeSettings"] = {}
    if v_order:
        payload["executionData"]["optimizeSettings"] = {"vOrder": True}
    if vacuum:
        payload["executionData"]["vacuumSettings"] = {}
    if vacuum and retention_period is not None:
        payload["executionData"]["vacuumSettings"]["retentionPeriod"] = retention_period

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/jobs/instances?jobType=TableMaintenance",
        method="post",
        payload=payload,
        status_codes=202,
        client="fabric_sp",
    )

    print(
        f"{icons.in_progress} The table maintenance job for the '{table_name}' table in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been initiated."
    )

    status_url = response.headers.get("Location").split("fabric.microsoft.com")[1]
    status = None
    while status not in ["Completed", "Failed"]:
        response = _base_api(request=status_url)
        status = response.json().get("status")
        time.sleep(3)

    df = _get_item_job_instance(url=status_url)

    if status == "Completed":
        print(
            f"{icons.green_dot} The table maintenance job for the '{table_name}' table in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has succeeded."
        )
    else:
        print(
            f"{icons.red_dot} The table maintenance job for the '{table_name}' table in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has failed."
        )

    return df
