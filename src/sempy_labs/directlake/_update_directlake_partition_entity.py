import sempy
from sempy_labs.tom import connect_semantic_model
from sempy_labs._refresh_semantic_model import refresh_semantic_model
from sempy_labs.directlake._dl_helper import get_direct_lake_source
from sempy_labs._helper_functions import (
    _convert_data_type,
    resolve_dataset_name_and_id,
    resolve_workspace_name_and_id,
    resolve_workspace_name,
    resolve_lakehouse_name_and_id,
    resolve_item_name_and_id,
)
from sempy_labs.directlake._update_directlake_model_lakehouse_connection import _get_direct_lake_expressions
from sempy._utils._log import log
from typing import List, Optional, Union
import sempy_labs._icons as icons
from uuid import UUID


@log
def update_direct_lake_partition_entity(
    dataset: str | UUID,
    table_name: Union[str, List[str]],
    entity_name: Union[str, List[str]],
    schema: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Remaps a table (or tables) in a Direct Lake semantic model to a table in a lakehouse.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    table_name : str, List[str]
        Name of the table(s) in the semantic model.
    entity_name : str, List[str]
        Name of the lakehouse table to be mapped to the semantic model table.
    schema : str, default=None
        The schema of the lakehouse table to be mapped to the semantic model table.
        Defaults to None which resolves to the existing schema of the lakehouse table.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    # Support both str & list types
    if isinstance(table_name, str):
        table_name = [table_name]
    if isinstance(entity_name, str):
        entity_name = [entity_name]

    if len(table_name) != len(entity_name):
        raise ValueError(
            f"{icons.red_dot} The 'table_name' and 'entity_name' arrays must be of equal length."
        )

    icons.sll_tags.append("UpdateDLPartition")

    with connect_semantic_model(
        dataset=dataset_id, readonly=False, workspace=workspace_id
    ) as tom:

        if not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace is not in Direct Lake mode."
            )

        for tName in table_name:
            i = table_name.index(tName)
            eName = entity_name[i]
            part_name = next(
                p.Name
                for t in tom.model.Tables
                for p in t.Partitions
                if t.Name == tName
            )

            if part_name is None:
                raise ValueError(
                    f"{icons.red_dot} The '{tName}' table in the '{dataset_name}' semantic model has not been updated."
                )

            tom.model.Tables[tName].Partitions[part_name].Source.EntityName = eName

            # Update source lineage tag
            existing_schema = (
                tom.model.Tables[tName].Partitions[part_name].Source.SchemaName or "dbo"
            )
            if schema is None:
                schema = existing_schema

            tom.model.Tables[tName].Partitions[part_name].Source.SchemaName = schema
            tom.model.Tables[tName].SourceLineageTag = f"[{schema}].[{eName}]"
            print(
                f"{icons.green_dot} The '{tName}' table in the '{dataset_name}' semantic model within the '{workspace_name}' workspace has been updated to point to the '{eName}' table."
            )


@log
def add_table_to_direct_lake_semantic_model(
    dataset: str | UUID,
    table_name: str,
    lakehouse_table_name: str,
    refresh: bool = True,
    workspace: Optional[str | UUID] = None,
):
    """
    Adds a table and all of its columns to a Direct Lake semantic model, based on a Fabric lakehouse table.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    table_name : str, List[str]
        Name of the table in the semantic model.
    lakehouse_table_name : str
        The name of the Fabric lakehouse table.
    refresh : bool, default=True
        Refreshes the table after it is added to the semantic model.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM
    from sempy_labs.lakehouse._get_lakehouse_columns import get_lakehouse_columns
    from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    artifact_type, lakehouse_name, lakehouse_id, lakehouse_workspace_id = (
        get_direct_lake_source(dataset=dataset_id, workspace=workspace_id)
    )

    if artifact_type == "Warehouse":
        raise ValueError(
            f"{icons.red_dot} This function is only valid for Direct Lake semantic models which source from Fabric lakehouses (not warehouses)."
        )

    if artifact_type is None:
        raise ValueError(
            f"{icons.red_dot} This function only supports Direct Lake semantic models where the source lakehouse resides in the same workpace as the semantic model."
        )

    lakehouse_workspace = resolve_workspace_name(workspace_id=lakehouse_workspace_id)

    with connect_semantic_model(
        dataset=dataset_id, readonly=False, workspace=workspace_id
    ) as tom:

        table_count = tom.model.Tables.Count

        if not tom.is_direct_lake() and table_count > 0:
            raise ValueError(
                f"{icons.red_dot} This function is only valid for Direct Lake semantic models or semantic models with no tables."
            )

        if any(
            p.Name == lakehouse_table_name
            for p in tom.all_partitions()
            if p.SourceType == TOM.PartitionSourceType.Entity
        ):
            t_name = next(
                p.Parent.Name
                for p in tom.all_partitions()
                if p.Name
                == lakehouse_table_name & p.SourceType
                == TOM.PartitionSourceType.Entity
            )
            raise ValueError(
                f"The '{lakehouse_table_name}' table already exists in the '{dataset_name}' semantic model within the '{workspace_name}' workspace as the '{t_name}' table."
            )

        if any(t.Name == table_name for t in tom.model.Tables):
            raise ValueError(
                f"The '{table_name}' table already exists in the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
            )

        dfL = get_lakehouse_tables(
            lakehouse=lakehouse_name, workspace=lakehouse_workspace
        )
        dfL_filt = dfL[dfL["Table Name"] == lakehouse_table_name]

        if len(dfL_filt) == 0:
            raise ValueError(
                f"The '{lakehouse_table_name}' table does not exist in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace}' workspace."
            )

        dfLC = get_lakehouse_columns(
            lakehouse=lakehouse_name, workspace=lakehouse_workspace
        )
        dfLC_filt = dfLC[dfLC["Table Name"] == lakehouse_table_name]

        tom.add_table(name=table_name)
        print(
            f"{icons.green_dot} The '{table_name}' table has been added to the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
        )
        tom.add_entity_partition(
            table_name=table_name, entity_name=lakehouse_table_name
        )
        print(
            f"{icons.green_dot} The '{lakehouse_table_name}' partition has been added to the '{table_name}' table in the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
        )

        for i, r in dfLC_filt.iterrows():
            lakeCName = r["Column Name"]
            dType = r["Data Type"]
            dt = _convert_data_type(dType)
            tom.add_data_column(
                table_name=table_name,
                column_name=lakeCName,
                source_column=lakeCName,
                data_type=dt,
            )
            print(
                f"{icons.green_dot} The '{lakeCName}' column has been added to the '{table_name}' table as a '{dt}' data type in the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
            )

        if refresh:
            refresh_semantic_model(
                dataset=dataset_id, tables=table_name, workspace=workspace_id
            )



@log
def add_direct_lake_table(
    dataset: str | UUID,
    table_name: str,
    source_table_name: str,
    workspace: Optional[str | UUID] = None,
    source: Optional[str | UUID] = None,
    source_type: Optional[str] = None,
    source_workspace: Optional[str | UUID] = None,
    schema: str = "dbo",
    refresh: bool = True,
):
    """
    Adds a table and all of its columns to a Direct Lake semantic model, based on a Fabric lakehouse table.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    table_name : str, List[str]
        Name of the table in the semantic model.
    lakehouse_table_name : str
        The name of the Fabric lakehouse table.
    refresh : bool, default=True
        Refreshes the table after it is added to the semantic model.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs.lakehouse._get_lakehouse_columns import get_lakehouse_columns
    from sempy_labs._warehouses import get_warehouse_columns

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)
    if source_type not in ['Lakehouse', 'Workspace']:
        raise ValueError(f"{icons.red_dot} The 'source_type' must be either 'Lakehouse' or 'Workspace'.")
    (source_workspace_name, source_workspace_id) = resolve_workspace_name_and_id(source_workspace)
    if source_type == 'Lakehouse':
        (source_name, source_id) = resolve_lakehouse_name_and_id(
            lakehouse=source, workspace=workspace_id
        )
    else:
        (source_name, source_id) = resolve_item_name_and_id(item=source, type=source_type, workspace=source_workspace_id)

    with connect_semantic_model(
        dataset=dataset_id, readonly=False, workspace=workspace_id
    ) as tom:

        if not tom.is_direct_lake() and tom.model.Tables.Count > 0:
            raise ValueError(
                f"{icons.red_dot} This function is only valid for Direct Lake semantic models or semantic models with no tables."
            )

        if any(t.Name == table_name for t in tom.model.Tables):
            raise ValueError(f"{icons.red_dot} The '{table_name}' table already exists in the '{dataset_name}' semantic model within the '{workspace_name}' workspace.")


        expression_dict = _get_direct_lake_expressions(dataset=dataset, workspace=workspace)
        expressions = list(expression_dict.keys())
        if source_type == "Lakehouse":
            dfC = get_lakehouse_columns(lakehouse=source, workspace=source_workspace)
        else:
            dfC = get_warehouse_columns(warehouse=source, workspace=source_workspace)

        dfC_filt = dfC[dfC['Table Name'] == source_table_name]
        if dfC_filt.empty:
            raise ValueError(f"{icons.red_dot} The '{source_table_name}' table does not exist within the specified source.")

        tom.add_table(name=table_name)
        tom.add_entity_partition(table_name=table_name, entity_name=source_table_name, expression=expression, schema_name=schema)

        for _, r in dfC_filt.iterrows():
            source_column = r["Column Name"]
            data_type = r["Data Type"]
            dt = _convert_data_type(data_type)
            tom.add_data_column(
                table_name=table_name,
                column_name=source_column,
                source_column=source_column,
                data_type=dt,
            )
            print(
                f"{icons.green_dot} The '{source_column}' column has been added to the '{table_name}' table as a '{dt}' data type in the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
            )
       

        

        if refresh:
            refresh_semantic_model(
                dataset=dataset_id, tables=table_name, workspace=workspace_id
            )
