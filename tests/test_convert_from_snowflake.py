"""Test ``convert_from_snowflake`` end-to-end against the example YAML file.

Skips the ``list_snowflake_columns`` call (no Snowflake credentials needed)
and skips source resolution, then prints every measure produced.

Run with::

    pytest -s tests/test_convert_from_snowflake.py
"""

from pathlib import Path
import pandas as pd
import pytest


EXAMPLE_YAML = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_snowflake_example_2.yaml"
)


@pytest.fixture
def patched_convert(monkeypatch):
    """Patch ``list_snowflake_columns`` to avoid hitting Snowflake."""
    from sempy_labs.semantic_model import _snowflake as snowflake_module

    def _fake_list_snowflake_columns(*args, **kwargs):
        return pd.DataFrame({"Column Name": [], "Data Type": []})

    monkeypatch.setattr(
        snowflake_module, "list_snowflake_columns", _fake_list_snowflake_columns
    )
    return snowflake_module.convert_from_snowflake


def test_convert_from_snowflake_measures(patched_convert):
    assert EXAMPLE_YAML.exists(), f"Example YAML not found at {EXAMPLE_YAML}"

    yaml_text = EXAMPLE_YAML.read_text()

    result = patched_convert(
        yaml_file=yaml_text,
        account="fake_account",
        token="fake_token",
        sources=None,
        resolve_sources=False,
    )

    tables = result["model"]["tables"]
    assert tables, "Expected at least one table in the converted model."

    total_measures = 0
    print("\n" + "=" * 80)
    print(f"Model: {result['model']['name']}")
    print("=" * 80)

    for table in tables:
        measures = table.get("measures") or []
        if not measures:
            continue
        print(f"\nTable: {table['tableName']}  ({len(measures)} measure(s))")
        print("-" * 80)
        for m in measures:
            total_measures += 1
            print(f"  Name:             {m['name']}")
            print(f"  sourceExpression: {m['sourceExpression']}")
            print(f"  daxExpression:    {m['daxExpression']}")
            print()

    print("=" * 80)
    print(f"Total measures: {total_measures}")
    print("=" * 80)

    assert total_measures > 0, "Expected at least one measure in the converted model."
