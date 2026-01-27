from sempy_labs._helper_functions import (
    get_pbi_token_headers,
    _get_url_prefix,
    _create_dataframe,
    _update_dataframe_datatypes,
    _is_valid_uuid,
)
import requests
from datetime import datetime, timezone
from urllib.parse import quote
import pandas as pd
from uuid import UUID


def monitor(
    limit: int = 50,
    start_time: str = None,
    end_time: str = None,
    submitted_by: str = None,
    workspace: str | UUID = None,
) -> pd.DataFrame:
    """
    View and track the status of the activities across all the workspaces for which you have permissions within Microsoft Fabric.

    Parameters
    ----------
    limit : int, default=50
        The maximum number of activity records to retrieve.
    start_time : str, default=None
        The start time for filtering activities in ISO 8601 format (e.g., '2026-01-01T00:00:00.000Z').
        Defaults to None which resolves to the earliest possible time.
    end_time : str, default=None
        The end time for filtering activities in ISO 8601 format (e.g., '2026-01-31T23:59:59.999Z').
        Defaults to None which resolves to the current time.
    submitted_by: str, default=None
        Filter activities by the user who submitted them. Can be either the user's display name or email address.

        *Note that this filter is applied after retrieving the data and may conflict with the limit parameter.
    workspace: str | uuid.UUID, default=None
        Filter activities by the workspace they belong to. Can be either the workspace name or its ID.

        *Note that this filter is applied after retrieving the data and may conflict with the limit parameter.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe containing the activity monitoring data.
    """

    columns = {
        "Activity Name": "str",
        "Activity Id": "str",
        "Status": "str",
        "Is Successful": "bool",
        "Item Type": "str",
        "Scheduled Time": "str",
        "Start Time": "str",
        "End Time": "str",
        "Duration (seconds)": "float",
        "Submitted By": "str",
        "Submitted User Principal Name": "str",
        "Workspace Name": "str",
        "Workspace Id": "str",
    }

    df = _create_dataframe(columns)
    headers = get_pbi_token_headers()
    prefix = _get_url_prefix()

    def to_query_time(t):
        if isinstance(t, datetime):
            t = (
                t.astimezone(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
        return quote(t)

    if not end_time:
        end_time = to_query_time(datetime.now(timezone.utc))
    else:
        end_time = to_query_time(end_time)

    if not start_time:
        start_time = "1970-01-01T00%3A00%3A00.000Z"
    else:
        start_time = to_query_time(start_time)

    url = f"{prefix}/metadata/monitoringhub/histories?&endTime={end_time}&startTime={start_time}&usePublicName=true&limit={limit}"
    response = requests.get(url, headers=headers)

    rows = []
    for v in response.json():
        artifact_name = v.get("artifactName")
        activity_name = (
            v.get("artifactJobHistoryProperties", {}).get("1", {}) or artifact_name
        )
        start = v.get("jobStartTimeUtc")
        end = v.get("jobEndTimeUtc")
        duration_seconds = (
            (pd.to_datetime(end) - pd.to_datetime(start)).total_seconds()
            if start and end
            else None
        )
        owner_user = v.get("ownerUser", {})
        rows.append(
            {
                "Activity Name": activity_name,
                "Activity Id": v.get("artifactJobInstanceId"),
                "Status": v.get("statusString"),
                "Is Successful": v.get("isSuccessful"),
                "Item Name": artifact_name,
                "Item Type": v.get("artifactType"),
                "Item Id": v.get("artifactObjectId"),
                "Scheduled Time": v.get("jobScheduleTimeUtc"),
                "Start Time": v.get("jobStartTimeUtc"),
                "End Time": v.get("jobEndTimeUtc"),
                "Duration (seconds)": duration_seconds,
                "Submitted By": owner_user.get("name") if owner_user else None,
                "Submitted User Principal Name": (
                    owner_user.get("userPrincipalName") if owner_user else None
                ),
                "Workspace Name": v.get("workspaceName"),
                "Workspace Id": v.get("workspaceObjectId"),
            }
        )

    if rows:
        df = pd.DataFrame(rows)
        _update_dataframe_datatypes(df, columns)

    if submitted_by:
        mask = (df["Submitted By"] == submitted_by) | (
            df["Submitted User Principal Name"] == submitted_by
        )
        df = df[mask]

    if workspace:
        if _is_valid_uuid(workspace):
            mask = df["Workspace Id"] == str(workspace)
        else:
            mask = df["Workspace Name"] == workspace
        df = df[mask]

    return df
