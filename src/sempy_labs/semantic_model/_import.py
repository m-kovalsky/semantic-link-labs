from uuid import UUID
from typing import Optional, List, Dict
import yaml
from sempy_labs.tom import connect_semantic_model


class MetricsViewValidationError(Exception):
    pass


DATABRICKS_TO_TABULAR_TYPES = {
    "string": "String",
    "date": "DateTime",
    "timestamp": "DateTime",
    "bigint": "Int64",
    "int": "Int64",
    "decimal(18,2)": "Decimal",
    "double": "Double",
    "float": "Double",
    "boolean": "Boolean",
}


def map_data_type(db_type: str) -> str:
    return DATABRICKS_TO_TABULAR_TYPES.get(db_type.lower(), "String")


def validate_metrics_view(doc: Dict) -> None:
    if not isinstance(doc, dict):
        raise MetricsViewValidationError("YAML root must be a mapping")

    if "version" not in doc:
        raise MetricsViewValidationError("Missing required field: version")

    if "metrics_view" not in doc:
        raise MetricsViewValidationError("Missing required field: metrics_view")

    mv = doc["metrics_view"]

    for field in ["name", "catalog", "schema", "source"]:
        if field not in mv:
            raise MetricsViewValidationError(
                f"metrics_view missing required field: {field}"
            )

    # ---- Source validation
    source = mv["source"]

    if "type" not in source:
        raise MetricsViewValidationError("source missing required field: type")

    if source["type"] not in {"table", "query"}:
        raise MetricsViewValidationError(
            "source.type must be 'table' or 'query'"
        )

    if source["type"] == "table" and "table" not in source:
        raise MetricsViewValidationError(
            "source.type='table' requires field: table"
        )

    if source["type"] == "query" and "sql" not in source:
        raise MetricsViewValidationError(
            "source.type='query' requires field: sql"
        )

    # ---- Dimensions validation
    for dim in mv.get("dimensions", []):
        if "name" not in dim or "expr" not in dim:
            raise MetricsViewValidationError(
                "Each dimension must have 'name' and 'expr'"
            )

    # ---- Measures validation
    for measure in mv.get("measures", []):
        if "name" not in measure or "expr" not in measure:
            raise MetricsViewValidationError(
                "Each measure must have 'name' and 'expr'"
            )
        

def parse_databricks_metrics_yaml(path: str) -> Dict:
    """
    Validate and parse a Databricks Metrics View YAML file
    into Power BI semantic-model-ready components.
    """
    with open(path, "r") as f:
        doc = yaml.safe_load(f)

    # ✅ Validate first
    validate_metrics_view(doc)

    mv = doc["metrics_view"]
    table_name = mv["name"]

    # -----------------------------
    # Table
    # -----------------------------
    table = {
        "name": table_name,
        "description": mv.get("description", ""),
    }

    # -----------------------------
    # Partition
    # -----------------------------
    source = mv["source"]

    if source["type"] == "table":
        source_object = f'{mv["catalog"]}.{mv["schema"]}.{source["table"]}'
    else:
        source_object = "SQL_QUERY"

    partition = {
        "name": f"{table_name}_partition",
        "mode": "Import",
        "source_type": "M",
        "source_expression": f"-- Source: {source_object}",
    }

    # -----------------------------
    # Columns (Dimensions)
    # -----------------------------
    columns: List[Dict] = []

    for dim in mv.get("dimensions", []):
        columns.append(
            {
                "name": dim["name"],
                "data_type": map_data_type(dim.get("data_type", "string")),
                "source_expression": dim["expr"],
                "description": dim.get("description", ""),
                "is_hidden": False,
            }
        )

    # -----------------------------
    # Measures
    # -----------------------------
    measures: List[Dict] = []

    for m in mv.get("measures", []):
        dtype = m.get("data_type", "").lower()

        measures.append(
            {
                "name": m["name"],
                "dax_expression": m["expr"],
                "data_type": map_data_type(dtype),
                "format_string": "#,0.00" if "decimal" in dtype else None,
                "description": m.get("description", ""),
            }
        )

    return {
        "table": table,
        "partition": partition,
        "columns": columns,
        "measures": measures,
    }


def import_db(dataset: str | UUID, yaml_file_path: str, databricks_host: str, databricks_http_path: str, workspace: Optional[str | UUID] = None):

    """
    """

    parsed_file = parse_databricks_metrics_yaml(path=yaml_file_path)

    catalog = "main"
    database = "sales"
    table_name = "fact_sales"
    enable_automatic_proxy_discovery = "true"
    result = "Result"

    m_query = f"""let
        Source =
            Databricks.Catalogs(
                {databricks_host},
                {databricks_http_path},
                [
                    Catalog = {catalog},
                    Database = {database},
                    EnableAutomaticProxyDiscovery = {enable_automatic_proxy_discovery}
                ]
            ),

        {result} =
            Source{{[Name = {table_name}, Kind = "Table"]}}[Data]
    in
        {result}"""

    with connect_semantic_model(dataset=dataset, workspace=workspace, readonly=False) as tom:

        sm_table_name = parsed_file.get('table',{}).get('name')
        sm_table_desc = parsed_file.get('table',{}).get('description', '')
        sm_partition_name = parsed_file.get('partition',{}).get('name')
        sm_partition_source_expr = m_query  # parsed_file.get('partition',{}).get('source_expression')

        tom.add_table(name=sm_table_name, description=sm_table_desc)
        tom.add_m_partition(table_name=sm_table_name, partition_name=sm_partition_name, expression=sm_partition_source_expr)
        for c in parsed_file.get('columns', []):
            tom.add_data_column(
                table_name=sm_table_name,
                column_name=c['name'],
                data_type=c['data_type'],
                source_column=c.get('source_expression', ""),
                description=c.get('description', ''),
                is_hidden=c.get('is_hidden', False),
            )

        for m in parsed_file.get('measures', []):
            tom.add_measure(
                table_name=sm_table_name,
                measure_name=m['name'],
                expression=m['dax_expression'],
                data_type=m['data_type'],
                format_string=m.get('format_string'),
                description=m.get('description', ''),
            )
