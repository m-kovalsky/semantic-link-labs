from typing import Optional
import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)


def _get_snowflake_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def list_snowflake_tables(
    account: str,
    token: str,
    database: str,
    schema: str,
    table: Optional[str] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame:

    columns = {
        "Table Name": "str",
        "Kind": "str",
        "Table Type": "str",
    }
    df = _create_dataframe(columns=columns)

    headers = _get_snowflake_headers(token)
    url = f"{account}/api/v2/databases/{database}/schemas/{schema}/tables"
    if table:
        url += f"/{table}"
    result = _base_api(request=url, headers=headers, client="snowflake").json()

    if not return_dataframe:
        return result

    rows = []
    if isinstance(result, dict):
        rows.append(
            {
                "Table Name": result.get("name"),
                "Kind": result.get("kind"),
                "Table Type": result.get("table_type"),
            }
        )
    else:
        for x in result:
            rows.append(
                {
                    "Table Name": x.get("name"),
                    "Kind": x.get("kind"),
                    "Table Type": x.get("table_type"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


def list_snowflake_columns(
    account: str,
    token: str,
    database: str,
    schema: str,
    table: str,
    return_dataframe: bool = True,
) -> pd.DataFrame:

    columns = {
        "Column Name": "str",
        "Data Type": "str",
        "Nullable": "bool",
    }
    df = _create_dataframe(columns=columns)

    headers = _get_snowflake_headers(token)
    response = _base_api(
        request=f"{account}/api/v2/databases/{database}/schemas/{schema}/tables/{table}",
        headers=headers,
        client="snowflake",
    )

    if not return_dataframe:
        return response.json()

    rows = []
    for x in response.json().get('columns', []):
        rows.append(
            {
                "Column Name": x.get("name"),
                "Data Type": x.get("datatype"),
                "Nullable": x.get("nullable"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())
        _update_dataframe_datatypes(df, columns)

    return df
