import re
import yaml
from uuid import UUID
from typing import List, Optional, Union, IO, Any, Dict
from sempy_labs.semantic_model._helper import convert_sql_to_dax
from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_id,
)


def _get_synonyms(node: Optional[dict]) -> List[str]:
    """Extract synonyms from a Snowflake semantic view node."""
    if not node:
        return []
    syns = node.get("synonyms") or []
    return [s for s in syns if isinstance(s, str)]


def _build_source_name(base_table: Optional[dict]) -> str:
    """Build a fully qualified ``database.schema.table`` source name."""
    if not base_table:
        return ""
    parts = [
        base_table.get("database", "") or "",
        base_table.get("schema", "") or "",
        base_table.get("table", "") or "",
    ]
    return ".".join([p for p in parts if p])


def _resolve_metric_table(expression: str, table_names: List[str]) -> Optional[str]:
    """Heuristically find the table a metric expression refers to by looking
    for ``table_name.`` references in the expression."""
    if not expression:
        return None
    for tbl in table_names:
        if re.search(rf"\b{re.escape(tbl)}\.", expression):
            return tbl
    return None


# Mapping of Snowflake data types to Power BI data types. Keys are normalized
# to upper-case base type names (no parameters/precision/scale).
# Reference:
#   * https://docs.snowflake.com/en/sql-reference/data-types
#   * https://learn.microsoft.com/analysis-services/tabular-models/data-types-supported-ssas-tabular
_SNOWFLAKE_TO_PBI_DATA_TYPE: Dict[str, str] = {
    # Numeric / fixed-point
    "NUMBER": "Decimal",
    "DECIMAL": "Decimal",
    "NUMERIC": "Decimal",
    "INT": "Int64",
    "INTEGER": "Int64",
    "BIGINT": "Int64",
    "SMALLINT": "Int64",
    "TINYINT": "Int64",
    "BYTEINT": "Int64",
    # Floating-point
    "FLOAT": "Double",
    "FLOAT4": "Double",
    "FLOAT8": "Double",
    "DOUBLE": "Double",
    "DOUBLE PRECISION": "Double",
    "REAL": "Double",
    # String
    "VARCHAR": "String",
    "CHAR": "String",
    "CHARACTER": "String",
    "STRING": "String",
    "TEXT": "String",
    # Boolean
    "BOOLEAN": "Boolean",
    "BOOL": "Boolean",
    # Date / time
    "DATE": "DateTime",
    "DATETIME": "DateTime",
    "TIME": "DateTime",
    "TIMESTAMP": "DateTime",
    "TIMESTAMP_LTZ": "DateTime",
    "TIMESTAMP_NTZ": "DateTime",
    "TIMESTAMP_TZ": "DateTime",
    # Binary
    "BINARY": "Binary",
    "VARBINARY": "Binary",
    # Semi-structured / other (best-effort)
    "VARIANT": "String",
    "OBJECT": "String",
    "ARRAY": "String",
    "GEOGRAPHY": "String",
    "GEOMETRY": "String",
}


def _convert_snowflake_data_type(data_type: Optional[str]) -> str:
    """Convert a Snowflake data type string to its Power BI equivalent.

    Strips any parameters (e.g. ``NUMBER(10,2)`` -> ``NUMBER``) before
    looking up the mapping. Returns an empty string if ``data_type`` is
    falsy and ``"String"`` as a safe default for unrecognized types.
    """
    if not data_type:
        return ""
    base = re.split(r"[\(\s]", str(data_type).strip(), maxsplit=1)[0].upper()
    return _SNOWFLAKE_TO_PBI_DATA_TYPE.get(base, "String")


def convert_from_snowflake(
    yaml_file: Union[str, IO],
    name: Optional[str] = None,
    sources: Optional[List[dict]] = None,
    workspace: Optional[str | UUID] = None,
    resolve_sources: bool = True,
) -> Dict[str, Any]:
    """
    Convert a Snowflake semantic view YAML definition into the model_map format.

    Parameters
    ----------
    yaml_file : str | typing.IO
        Either a YAML string or a file-like object containing a Snowflake
        semantic view definition (see
        ``sempy_labs.semantic_model._snowflake_schema.yaml``).
    name : str, default=None
        The name to assign to the resulting model. If None, falls back to the
        top-level ``name`` of the Snowflake semantic view.
    sources : list[dict], default=None
        A list of dictionaries (matching ``sempy_labs.semantic_model._model_map.source_map``)
        used to resolve each table's ``source`` string (``database.schema.table``)
        to a Fabric ``sourceItemId`` / ``sourceWorkspaceId``. Each entry must include
        ``sourceName``, ``sourceItem``, ``sourceItemType``, and ``sourceWorkspace``.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    resolve_sources : bool, default=True
        When True, validate that each table's source is present in
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

    data = data or {}

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

    model_name = name or data.get("name", "") or ""
    model_description = data.get("description", "") or ""

    sf_tables = data.get("tables") or []
    if not sf_tables:
        raise ValueError("Snowflake semantic view does not contain a 'tables' entry.")

    table_names = [t.get("name", "") for t in sf_tables if t.get("name")]

    # Validate that every table's source is present in the ``sources`` list.
    if resolve_sources:
        table_sources = [
            _build_source_name(t.get("base_table"))
            for t in sf_tables
            if t.get("base_table")
        ]
        missing_sources = [s for s in table_sources if s and s not in resolved_sources]
        if missing_sources:
            raise ValueError(
                "The following table 'source' values are not present in the "
                f"'sources' parameter: {sorted(set(missing_sources))}"
            )

    # Build column maps for use by ``convert_sql_to_dax``:
    #   * ``column_map`` (global): maps ``table.column`` and bare ``column``
    #     references (using the field name, the source column, and the raw
    #     ``expr`` identifier) to the DAX form ``'table'[column]``.
    #   * ``per_table_bare`` (per-table overlay): maps bare column references
    #     to the DAX form, scoped to a single table. Used to bias bare
    #     references inside a measure's expression toward the measure's own
    #     table when the same column name exists in multiple tables.
    column_map: Dict[str, str] = {}
    per_table_bare: Dict[str, Dict[str, str]] = {}

    def _bare_identifier(expr: str) -> Optional[str]:
        """Return the unquoted identifier if ``expr`` is a bare column
        reference, else None."""
        if not expr:
            return None
        s = expr.strip()
        if re.fullmatch(r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)', s):
            return s.strip('"').strip("`")
        return None

    for t in sf_tables:
        tbl = t.get("name", "") or ""
        if not tbl:
            continue
        bare_for_table: Dict[str, str] = {}
        for kind in ("dimensions", "time_dimensions", "facts"):
            for field in t.get(kind, []) or []:
                col = field.get("name", "") or ""
                if not col:
                    continue
                dax_ref = f"'{tbl}'[{col}]"
                # Logical/field name references.
                column_map[f"{tbl}.{col}"] = dax_ref
                column_map.setdefault(col, dax_ref)
                bare_for_table[col] = dax_ref
                # Source-column / expr-identifier references (e.g. measures
                # written against the underlying base table column name).
                src_id = _bare_identifier(field.get("expr", "") or "")
                if src_id and src_id != col:
                    column_map[f"{tbl}.{src_id}"] = dax_ref
                    column_map.setdefault(src_id, dax_ref)
                    bare_for_table[src_id] = dax_ref
        # Pre-register table-scoped metrics so that other measure expressions
        # (including view-level derived metrics) can reference them as
        # ``table.metric`` or bare ``metric`` and have them resolve to a DAX
        # measure reference ``[MetricName]``.
        for metric in t.get("metrics", []) or []:
            mname = metric.get("name", "") or ""
            if not mname:
                continue
            measure_ref = f"[{mname}]"
            column_map[f"{tbl}.{mname}"] = measure_ref
            column_map.setdefault(mname, measure_ref)
            bare_for_table[mname] = measure_ref
        per_table_bare[tbl] = bare_for_table

    # View-level (derived) metrics — also register them as measure references.
    for metric in data.get("metrics", []) or []:
        mname = metric.get("name", "") or ""
        if not mname:
            continue
        column_map.setdefault(mname, f"[{mname}]")

    # Pre-register columns referenced via ``table.column`` in metric
    # expressions but not declared as dimensions/facts. The Snowflake
    # semantic view often references the underlying base-table column names
    # directly (e.g. ``SUM(store_sales.ss_sales_price * store_sales.ss_quantity)``).
    # Resolving these to DAX column references makes the resulting metric
    # expression valid; the columns themselves still need to exist in the
    # final Power BI model.
    qualified_ref_re = re.compile(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b"
    )
    table_names_lower = {tn.lower(): tn for tn in table_names}

    def _scan_expr(expr: str, owning_table: Optional[str]) -> None:
        if not expr:
            return
        for m in qualified_ref_re.finditer(expr):
            tbl_ref, col_ref = m.group(1), m.group(2)
            tbl = table_names_lower.get(tbl_ref.lower())
            if not tbl:
                continue
            key = f"{tbl_ref}.{col_ref}"
            if key in column_map:
                continue
            dax_ref = f"'{tbl}'[{col_ref}]"
            column_map[key] = dax_ref
            column_map.setdefault(col_ref, dax_ref)
            if owning_table and owning_table == tbl:
                per_table_bare.setdefault(owning_table, {}).setdefault(
                    col_ref, dax_ref
                )

    for t in sf_tables:
        tbl_name = t.get("name", "") or ""
        for metric in t.get("metrics", []) or []:
            _scan_expr(metric.get("expr", "") or "", tbl_name)
    for metric in data.get("metrics", []) or []:
        _scan_expr(metric.get("expr", "") or "", None)

    def _column_map_for_table(table_name: str) -> Dict[str, str]:
        """Return a column map biased toward ``table_name`` for bare refs."""
        if not table_name or table_name not in per_table_bare:
            return column_map
        return {**column_map, **per_table_bare[table_name]}

    def _build_column(field: dict, table_name: str, pk_columns: set) -> Dict[str, Any]:
        col_name = field.get("name", "") or ""
        expression = field.get("expr", "") or ""
        # If the expression is just a bare column name (an identifier,
        # optionally quoted with double quotes or backticks), treat it as a
        # plain source column rather than a calculated column.
        expr_stripped = expression.strip()
        is_bare_identifier = (
            bool(expr_stripped)
            and re.fullmatch(
                r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)', expr_stripped
            )
            is not None
        )
        is_calculated = bool(expression) and not is_bare_identifier
        if is_calculated:
            source_column = ""
        elif is_bare_identifier:
            # Strip surrounding quotes/backticks if present.
            source_column = expr_stripped.strip('"').strip("`")
        else:
            source_column = col_name
        is_key = (col_name in pk_columns) or (
            bool(source_column) and source_column in pk_columns
        )
        return {
            "name": col_name,
            "sourceColumn": source_column,
            "sourceDataType": field.get("data_type", "") or "",
            "pbiDataType": _convert_snowflake_data_type(field.get("data_type")),
            "sourceFormat": None,
            "pbiFormat": None,
            "description": field.get("description", "") or "",
            "expression": expression if is_calculated else "",
            "synonyms": _get_synonyms(field),
            "fullDAXObjectName": f"'{table_name}'[{col_name}]",
            "isCalculated": is_calculated,
            "isKey": is_key,
        }

    tables: List[Dict[str, Any]] = []
    for t in sf_tables:
        table_name = t.get("name", "") or ""
        source_name = _build_source_name(t.get("base_table"))

        # Collect the set of primary key column names for this table. The
        # Snowflake semantic view schema declares them under
        # ``primary_key.columns``.
        pk_block = t.get("primary_key") or {}
        pk_columns = set(pk_block.get("columns") or [])

        columns: List[Dict[str, Any]] = []
        for kind in ("dimensions", "time_dimensions", "facts"):
            for field in t.get(kind, []) or []:
                columns.append(_build_column(field, table_name, pk_columns))

        measures: List[Dict[str, Any]] = []
        for metric in t.get("metrics", []) or []:
            metric_name = metric.get("name", "") or ""
            expression = metric.get("expr", "") or ""
            measures.append(
                {
                    "name": metric_name,
                    "sourceExpression": expression,
                    "daxExpression": (
                        convert_sql_to_dax(
                            expression,
                            column_map=_column_map_for_table(table_name),
                            default_table=table_name,
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

        tables.append(
            {
                "tableName": table_name,
                "description": t.get("description", "") or "",
                "sourceName": source_name,
                "sourceitemId": resolved_sources.get(source_name, {}).get(
                    "sourceItemId", ""
                ),
                "sourceworkspaceId": resolved_sources.get(source_name, {}).get(
                    "sourceWorkspaceId", ""
                ),
                "columns": columns,
                "measures": measures,
            }
        )

    # Map view-level (derived) metrics onto the table they reference.
    table_lookup = {t["tableName"]: t for t in tables}
    for metric in data.get("metrics", []) or []:
        metric_name = metric.get("name", "") or ""
        expression = metric.get("expr", "") or ""
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
                        column_map=_column_map_for_table(target_table),
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
    for rel in data.get("relationships", []) or []:
        rel_columns = rel.get("relationship_columns") or []
        if len(rel_columns) > 1:
            raise ValueError(
                "Multi-column relationships are not supported. The relationship "
                f"'{rel.get('name', '')}' from '{rel.get('left_table', '')}' to "
                f"'{rel.get('right_table', '')}' has "
                f"relationship_columns={rel_columns}."
            )
        left_column = rel_columns[0].get("left_column", "") if rel_columns else ""
        right_column = rel_columns[0].get("right_column", "") if rel_columns else ""

        relationships.append(
            {
                "name": rel.get("name") or None,
                "fromTable": rel.get("left_table", "") or "",
                "fromColumn": left_column,
                "toTable": rel.get("right_table", "") or "",
                "toColumn": right_column,
                "fromCardinality": "Many",
                "toCardinality": "One",
            }
        )

    # Composite primary keys (multiple columns flagged with isKey=True on the
    # same table) are not supported by the model_map format.
    composite_keys = {
        t["tableName"]: [c["name"] for c in t["columns"] if c.get("isKey")]
        for t in tables
        if sum(1 for c in t["columns"] if c.get("isKey")) > 1
    }
    if composite_keys:
        details = ", ".join(f"{tbl}: {cols}" for tbl, cols in composite_keys.items())
        raise ValueError(
            "Composite primary keys (multiple key columns on a single table) "
            f"are not supported. Offending tables: {details}."
        )

    return {
        "model": {
            "name": model_name,
            "description": model_description,
            "tables": tables,
            "relationships": relationships,
        }
    }
