import sempy.fabric as fabric
import pandas as pd
import datetime
from pyspark.sql import SparkSession
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    save_as_delta_table,
    resolve_workspace_capacity,
    retry,
)
from sempy_labs.lakehouse import get_lakehouse_tables, lakehouse_attached
from sempy_labs._model_bpa import run_model_bpa
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def vertipaq_analyzer_bulk(
    workspace: Optional[str | List[str]] = None,
    read_stats_from_data: Optional[bool] = False,
):
    """
    Runs the semantic model Best Practice Analyzer across all semantic models in a workspace (or all accessible workspaces).
    Saves (appends) the results to the 'modelbparesults' delta table in the lakehouse attached to the notebook.
    Default semantic models are skipped in this analysis.

    Parameters
    ----------


    """

    import pyspark.sql.functions as F
    from sempy_labs._vertipaq import vertipaq_analyzer

    if not lakehouse_attached():
        raise ValueError(
            f"{icons.red_dot} No lakehouse is attached to this notebook. Must attach a lakehouse to the notebook."
        )

    cols = [
        "Capacity Name",
        "Capacity Id",
        "Workspace Name",
        "Workspace Id",
        "Dataset Name",
        "Dataset Id",
        "Configured By",
    ]
    now = datetime.datetime.now()
    spark = SparkSession.builder.getOrCreate()
    lakehouse_workspace = fabric.resolve_workspace_name()
    lakehouse_id = fabric.get_lakehouse_id()
    lakehouse = resolve_lakehouse_name(
        lakehouse_id=lakehouse_id, workspace=lakehouse_workspace
    )
    lakeTName = "vertipaq_analyzer_model"
    lakeT = get_lakehouse_tables(lakehouse=lakehouse, workspace=lakehouse_workspace)
    lakeT_filt = lakeT[lakeT["Table Name"] == lakeTName]
    if len(lakeT_filt) == 0:
        runId = 1
    else:
        dfSpark = spark.table(f"`{lakehouse_id}`.{lakeTName}").select(F.max("RunId"))
        maxRunId = dfSpark.collect()[0][0]
        runId = maxRunId + 1

    if isinstance(workspace, str):
        workspace = [workspace]

    dfW = fabric.list_workspaces()
    if workspace is None:
        dfW_filt = dfW.copy()
    else:
        dfW_filt = dfW[dfW["Name"].isin(workspace)]

    for i, r in dfW_filt.iterrows():
        wksp = r["Name"]
        wksp_id = r["Id"]
        capacity_id, capacity_name = resolve_workspace_capacity(workspace=wksp)
        df = pd.DataFrame(columns=cols)
        dfD = fabric.list_datasets(workspace=wksp, mode="rest")

        # Exclude default semantic models
        if len(dfD) > 0:
            dfI = fabric.list_items(workspace=wksp)
            filtered_df = dfI.groupby("Display Name").filter(
                lambda x: set(["Warehouse", "SemanticModel"]).issubset(set(x["Type"]))
                or set(["Lakehouse", "SemanticModel"]).issubset(set(x["Type"]))
            )
            default_semantic_models = filtered_df["Display Name"].unique().tolist()
            # Skip ModelBPA :)
            skip_models = default_semantic_models + [icons.model_bpa_name]
            dfD_filt = dfD[~dfD["Dataset Name"].isin(skip_models)]

            if len(dfD_filt) > 0:
                for i2, r2 in dfD_filt.iterrows():
                    dataset_name = r2["Dataset Name"]
                    config_by = r2["Configured By"]
                    dataset_id = r2["Dataset Id"]
                    print(
                        f"{icons.in_progress} Collecting Vertipaq Analyzer stats for the '{dataset_name}' semantic model within the '{wksp}' workspace."
                    )
                    try:
                        m, t, c, h, p, rel = vertipaq_analyzer(
                            dataset=dataset_name,
                            workspace=wksp,
                            read_stats_from_data=read_stats_from_data,
                            return_dataframe=True,
                        )

                        for z in [m, t, c, h, p, rel]:
                            z["Capacity Id"] = capacity_id
                            z["Capacity Name"] = capacity_name
                            z["Workspace Name"] = wksp
                            z["Workspace Id"] = wksp_id
                            z["Dataset Name"] = dataset_name
                            z["Dataset Id"] = dataset_id
                            z["Configured By"] = config_by
                            z["Timestamp"] = now
                            z["RunId"] = runId
                            z = z[cols]

                            z["RunId"] = z["RunId"].astype("int")

                        df = pd.concat([df, bpa_df], ignore_index=True)
                        print(
                            f"{icons.green_dot} Collected Vertipaq Analyzer stats for the '{dataset_name}' semantic model within the '{wksp}' workspace."
                        )
                    except Exception as e:
                        print(
                            f"{icons.red_dot} Vertipaq Analyzer stat collection failed for the '{dataset_name}' semantic model within the '{wksp}' workspace."
                        )
                        print(e)

                df["Severity"].replace(icons.severity_mapping, inplace=True)

                # Append save results individually for each workspace (so as not to create a giant dataframe)
                print(
                    f"{icons.in_progress} Saving the Vertipaq Analyzer results of the '{wksp}' workspace to the '{output_table}' within the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace..."
                )
                save_as_delta_table(
                    dataframe=df,
                    delta_table_name=output_table,
                    write_mode="append",
                    merge_schema=True,
                )
                print(
                    f"{icons.green_dot} Saved BPA results to the '{output_table}' delta table."
                )

    print(f"{icons.green_dot} Bulk Vertipaq Analyzer scan complete.")