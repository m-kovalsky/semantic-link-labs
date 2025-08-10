from tqdm.auto import tqdm
from typing import List, Optional, Union
from sempy._utils._log import log
from uuid import UUID
from sempy_labs._helper_functions import (
    _create_spark_session,
    _pure_python_notebook,
)


@log
def lakehouse_attached() -> bool:
    """
    Identifies if a lakehouse is attached to the notebook.

    Returns
    -------
    bool
        Returns True if a lakehouse is attached to the notebook.
    """

    from sempy_labs._helper_functions import _get_fabric_context_setting

    lake_id = _get_fabric_context_setting(name="trident.lakehouse.id")

    if len(lake_id) > 0:
        return True
    else:
        return False


@log
def _optimize_table(path):

    if _pure_python_notebook():
        from deltalake import DeltaTable

        DeltaTable(path).optimize.compact()
    else:
        from delta import DeltaTable

        spark = _create_spark_session()
        DeltaTable.forPath(spark, path).optimize().executeCompaction()


@log
def _vacuum_table(path, retain_n_hours):

    if _pure_python_notebook():
        from deltalake import DeltaTable

        DeltaTable(path).vacuum(retention_hours=retain_n_hours)
    else:
        from delta import DeltaTable

        spark = _create_spark_session()
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        DeltaTable.forPath(spark, path).vacuum(retain_n_hours)


@log
def optimize_lakehouse_tables(
    tables: Optional[Union[str, List[str]]] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Runs the `OPTIMIZE <https://docs.delta.io/latest/optimizations-oss.html>`_ function over the specified lakehouse tables.

    Parameters
    ----------
    tables : str | List[str], default=None
        The table(s) to optimize.
        Defaults to None which resovles to optimizing all tables within the lakehouse.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables

    df = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace)
    df_delta = df[df["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    df_tables = df_delta[df_delta["Table Name"].isin(tables)] if tables else df_delta
    df_tables.reset_index(drop=True, inplace=True)

    total = len(df_tables)
    for idx, r in (bar := tqdm(df_tables.iterrows(), total=total, bar_format="{desc}")):
        table_name = r["Table Name"]
        path = r["Location"]
        bar.set_description(
            f"Optimizing the '{table_name}' table ({idx + 1}/{total})..."
        )
        _optimize_table(path=path)


@log
def vacuum_lakehouse_tables(
    tables: Optional[Union[str, List[str]]] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    retain_n_hours: Optional[int] = None,
):
    """
    Runs the `VACUUM <https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table>`_ function over the specified lakehouse tables.

    Parameters
    ----------
    tables : str | List[str] | None
        The table(s) to vacuum. If no tables are specified, all tables in the lakehouse will be vacuumed.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    retain_n_hours : int, default=None
        The number of hours to retain historical versions of Delta table files.
        Files older than this retention period will be deleted during the vacuum operation.
        If not specified, the default retention period configured for the Delta table will be used.
        The default retention period is 168 hours (7 days) unless manually configured via table properties.
    """

    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables

    df = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace)
    df_delta = df[df["Format"] == "delta"]

    if isinstance(tables, str):
        tables = [tables]

    df_tables = df_delta[df_delta["Table Name"].isin(tables)] if tables else df_delta
    df_tables.reset_index(drop=True, inplace=True)

    total = len(df_tables)
    for idx, r in (bar := tqdm(df_tables.iterrows(), total=total, bar_format="{desc}")):
        table_name = r["Table Name"]
        path = r["Location"]
        bar.set_description(f"Vacuuming the '{table_name}' table ({idx}/{total})...")
        _vacuum_table(path=path, retain_n_hours=retain_n_hours)
