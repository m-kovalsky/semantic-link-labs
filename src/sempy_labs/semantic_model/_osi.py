import re
import yaml
from uuid import UUID
from typing import List, Optional, Union, IO, Any, Dict
from sempy_labs.semantic_model._helper import convert_sql_to_dax
from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_id,
)


def _get_ansi_expression(expression_obj: Optional[dict]) -> str:
    """Extract the ANSI_SQL dialect expression from an OSI expression object."""
    if not expression_obj:
        return ""
    dialects = expression_obj.get("dialects", []) or []
    for dialect in dialects:
        if dialect.get("dialect") == "ANSI_SQL":
            return dialect.get("expression", "") or ""
    if dialects:
        return dialects[0].get("expression", "") or ""
    return ""


def _get_synonyms(node: Optional[dict]) -> List[str]:
    """Extract synonyms from an ai_context node."""
    if not node:
        return []
    ai = node.get("ai_context") or {}
    syns = ai.get("synonyms") or []
    return [s for s in syns if isinstance(s, str)]


def _resolve_metric_table(expression: str, table_names: List[str]) -> Optional[str]:
    """Heuristically find the table a metric expression refers to by looking
    for ``table_name.`` references in the expression."""
    if not expression:
        return None
    for tbl in table_names:
        if re.search(rf"\b{re.escape(tbl)}\.", expression):
            return tbl
    return None


def convert_from_osi(
    yaml_file: Union[str, IO],
    name: Optional[str] = None,
    sources: Optional[List[dict]] = None,
    workspace: Optional[str | UUID] = None,
    resolve_sources: bool = True,
) -> Dict[str, Any]:
    """
    Convert an Open Semantic Interchange (OSI) YAML model into the model_map format.

    Parameters
    ----------
    yaml_file : str | typing.tyIO
        Either a YAML string or a file-like object containing an OSI semantic
        model definition.
    name : str, default=None
        The name to assign to the resulting model. If None, falls back to the
        ``name`` of the first ``semantic_model`` entry in the OSI document.
    sources : list[dict], default=None
        A list of dictionaries (matching ``sempy_labs._osi._model_map.source_map``)
        used to resolve each dataset's ``source`` string to a Fabric
        ``sourceItemId`` / ``sourceWorkspaceId``. Each entry must include
        ``sourceName``, ``sourceItem``, ``sourceItemType``, and ``sourceWorkspace``.
    workspace : str| uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    resolve_sources : bool, default=True
        When True, validate that each dataset's ``source`` is present in
        ``sources`` and resolve the workspace/item to Fabric IDs. When False,
        skip validation and resolution and leave ``sourceitemId`` /
        ``sourceworkspaceId`` as empty strings.

    Returns
    -------
    dict
        A dictionary structure of a Power BI semantic model.
    """

    if hasattr(yaml_file, "read"):
        data = yaml.safe_load(yaml_file)
    else:
        data = yaml.safe_load(yaml_file)

    # Resolve the ``sources`` list into a lookup keyed by source name.
    resolved_sources: Dict[str, Dict[str, str]] = {}
    if resolve_sources:
        for entry in sources or []:
            source_name = entry.get("sourceName")
            if not source_name:
                continue
            workspace = entry.get("sourceWorkspace") or None
            item = entry.get("sourceItem") or None
            item_type = entry.get("sourceItemType") or None

            workspace_id = resolve_workspace_id(workspace)
            item_id = resolve_item_id(item=item, type=item_type, workspace=workspace_id)

            resolved_sources[source_name] = {
                "sourceItemId": item_id,
                "sourceWorkspaceId": workspace_id,
            }

    semantic_models = (data or {}).get("semantic_model") or []
    if not semantic_models:
        raise ValueError("OSI document does not contain a 'semantic_model' entry.")

    osi_model = semantic_models[0]

    model_name = name or osi_model.get("name", "") or ""
    model_description = osi_model.get("description", "") or ""

    datasets = osi_model.get("datasets") or []
    table_names = [d.get("name", "") for d in datasets if d.get("name")]

    # Validate that every dataset's source is present in the ``sources`` list.
    if resolve_sources:
        dataset_sources = [ds.get("source", "") for ds in datasets if ds.get("source")]
        missing_sources = [s for s in dataset_sources if s not in resolved_sources]
        if missing_sources:
            raise ValueError(
                "The following dataset 'source' values are not present in the "
                f"'sources' parameter: {sorted(set(missing_sources))}"
            )

    # Build a column map of `table.column` -> `'table'[column]` (and bare
    # `column` -> `'table'[column]`) for use by ``convert_sql_to_dax``.
    column_map: Dict[str, str] = {}
    for ds in datasets:
        tbl = ds.get("name", "") or ""
        for field in ds.get("fields", []) or []:
            col = field.get("name", "") or ""
            if not tbl or not col:
                continue
            dax_ref = f"'{tbl}'[{col}]"
            column_map[f"{tbl}.{col}"] = dax_ref
            column_map.setdefault(col, dax_ref)

    tables: List[Dict[str, Any]] = []
    for ds in datasets:
        table_name = ds.get("name", "") or ""
        source_name = ds.get("source", "") or ""

        columns: List[Dict[str, Any]] = []
        for field in ds.get("fields", []) or []:
            col_name = field.get("name", "") or ""
            expression = _get_ansi_expression(field.get("expression"))
            # If the expression is just the column name itself, treat it as a
            # plain source column rather than a calculated column.
            is_calculated = bool(expression) and expression.strip() != col_name
            source_column = "" if is_calculated else (expression or col_name)

            columns.append(
                {
                    "name": col_name,
                    "sourceColumn": source_column,
                    "sourceDataType": "",
                    "pbiDataType": "",
                    "sourceFormat": None,
                    "pbiFormat": None,
                    "description": field.get("description", "") or "",
                    "expression": expression if is_calculated else "",
                    "synonyms": _get_synonyms(field),
                    "fullDAXObjectName": f"'{table_name}'[{col_name}]",
                    "isCalculated": is_calculated,
                }
            )

        tables.append(
            {
                "tableName": table_name,
                "description": ds.get("description", "") or "",
                "sourceName": source_name,
                "sourceitemId": resolved_sources.get(source_name, {}).get(
                    "sourceItemId", ""
                ),
                "sourceworkspaceId": resolved_sources.get(source_name, {}).get(
                    "sourceWorkspaceId", ""
                ),
                "columns": columns,
                "measures": [],
            }
        )

    # Map model-level metrics onto the table they reference.
    table_lookup = {t["tableName"]: t for t in tables}
    for metric in osi_model.get("metrics", []) or []:
        metric_name = metric.get("name", "") or ""
        expression = _get_ansi_expression(metric.get("expression"))
        target_table = _resolve_metric_table(expression, table_names)
        if target_table is None and tables:
            target_table = tables[0]["tableName"]
        if target_table is None:
            continue

        table_lookup[target_table]["measures"].append(
            {
                "name": metric_name,
                "sourceExpression": expression,
                "daxExpression": (
                    convert_sql_to_dax(
                        expression,
                        column_map=column_map,
                        default_table=target_table,
                    )
                    if expression
                    else ""
                ),
                "sourceFormat": None,
                "pbiFormat": None,
                "description": metric.get("description", "") or "",
                "synonyms": _get_synonyms(metric),
            }
        )

    relationships: List[Dict[str, Any]] = []
    for rel in osi_model.get("relationships", []) or []:
        from_columns = rel.get("from_columns") or []
        to_columns = rel.get("to_columns") or []
        if len(from_columns) > 1 or len(to_columns) > 1:
            raise ValueError(
                "Multi-column relationships are not supported. The relationship "
                f"from '{rel.get('from', '')}' to '{rel.get('to', '')}' has "
                f"from_columns={from_columns} and to_columns={to_columns}."
            )
        from_column = from_columns[0] if from_columns else ""
        to_column = to_columns[0] if to_columns else ""

        relationships.append(
            {
                "name": None,
                "fromTable": rel.get("from", "") or "",
                "fromColumn": from_column,
                "toTable": rel.get("to", "") or "",
                "toColumn": to_column,
                "fromCardinality": "Many",
                "toCardinality": "One",
            }
        )

    return {
        "model": {
            "name": model_name,
            "description": model_description,
            "tables": tables,
            "relationships": relationships,
        }
    }
