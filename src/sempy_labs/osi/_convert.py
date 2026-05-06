import re
import yaml
from typing import List, Optional, Union, IO, Any, Dict


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


def import_osi(
    name: str,
    yaml_file: Union[str, IO],
    sources: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Convert an Open Semantic Interchange (OSI) YAML model into the
    ``sempy_labs._osi._model_map`` ``model_map`` dictionary structure.

    Parameters
    ----------
    name : str
        The name to assign to the resulting model. If empty, falls back to the
        name of the first ``semantic_model`` entry in the OSI document.
    yaml_file : str | IO
        Either a YAML string or a file-like object containing an OSI semantic
        model definition.
    sources : list[str], default=None
        Reserved for future use. Will eventually be used to map dataset
        ``source`` strings to Fabric ``sourceItemId`` / ``sourceWorkspaceId``
        values. Currently ignored.

    Returns
    -------
    dict
        A dictionary matching the structure of
        ``sempy_labs._osi._model_map.model_map``.
    """

    if hasattr(yaml_file, "read"):
        data = yaml.safe_load(yaml_file)
    else:
        data = yaml.safe_load(yaml_file)

    semantic_models = (data or {}).get("semantic_model") or []
    if not semantic_models:
        raise ValueError("OSI document does not contain a 'semantic_model' entry.")

    osi_model = semantic_models[0]

    model_name = name or osi_model.get("name", "") or ""
    model_description = osi_model.get("description", "") or ""

    datasets = osi_model.get("datasets") or []
    table_names = [d.get("name", "") for d in datasets if d.get("name")]

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
                    "dataType": "",
                    "format_original": None,
                    "format_pbi": None,
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
                "sourceitemId": "",
                "sourceworkspaceId": "",
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
                "expression_original": expression,
                "expression_dax": "",
                "format_original": None,
                "format_pbi": None,
                "description": metric.get("description", "") or "",
                "synonyms": _get_synonyms(metric),
            }
        )

    relationships: List[Dict[str, Any]] = []
    for rel in osi_model.get("relationships", []) or []:
        from_columns = rel.get("from_columns") or []
        to_columns = rel.get("to_columns") or []
        from_column = from_columns[0] if from_columns else ""
        to_column = to_columns[0] if to_columns else ""

        relationships.append(
            {
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
