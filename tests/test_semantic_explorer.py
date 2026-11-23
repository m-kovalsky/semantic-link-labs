"""Tests for the Semantic Explorer backend (metadata payload and saving).

TOM / pythonnet are unavailable offline, so the tests use small stand-ins for
the TOM objects and for the ``System`` / ``Microsoft.AnalysisServices.Tabular``
modules the explorer imports.
"""

import sys
import types

import pytest

from sempy_labs.semantic_model import _explorer as sx


# ---------------------------------------------------------------------------
# Minimal pythonnet / TOM stand-ins
# ---------------------------------------------------------------------------
class _EnumBase:
    @staticmethod
    def GetNames(enum_type):
        return list(enum_type.names)

    @staticmethod
    def Parse(enum_type, value, ignore_case=False):
        for name in enum_type.names:
            if name == value or (ignore_case and name.lower() == str(value).lower()):
                return _Enum(enum_type, name)
        raise ValueError(value)


class _EnumType:
    IsEnum = True

    def __init__(self, name, names):
        self.Name = name
        self.FullName = f"TOM.{name}"
        self.names = names


class _Enum(_EnumBase):
    def __init__(self, enum_type, name):
        self.enum_type = enum_type
        self.name = name

    def GetType(self):
        return self.enum_type

    def __str__(self):
        return self.name


class _ScalarType:
    IsEnum = False

    def __init__(self, full_name):
        self.FullName = full_name


class _PropInfo:
    CanWrite = True

    def __init__(self, prop_type):
        self.PropertyType = prop_type


class _Type:
    def __init__(self, obj):
        self.obj = obj

    def GetProperty(self, name):
        if not hasattr(self.obj, name):
            return None
        current = getattr(self.obj, name)
        if isinstance(current, _Enum):
            return _PropInfo(current.enum_type)
        if isinstance(current, bool):
            return _PropInfo(_ScalarType("System.Boolean"))
        if isinstance(current, int):
            return _PropInfo(_ScalarType("System.Int32"))
        return _PropInfo(_ScalarType("System.String"))


class _Coll(list):
    @property
    def Count(self):
        return len(self)

    def ContainsName(self, name):
        return any(getattr(item, "Name", None) == name for item in self)

    def __getitem__(self, key):
        if isinstance(key, tuple):  # ObjectTranslations[obj, property]
            obj, prop = key
            return next(
                (t for t in self if t.Object is obj and t.Property == prop), None
            )
        if isinstance(key, str):
            for item in self:
                if getattr(item, "Name", None) == key:
                    return item
            raise KeyError(key)
        return super().__getitem__(key)

    def Add(self, item):
        self.append(item)

    def Remove(self, item):
        self.remove(item)

    def Clear(self):
        del self[:]


class _Obj:
    def __init__(self, object_type, **props):
        self.ObjectType = object_type
        self.Annotations = _Coll()
        self.ExtendedProperties = _Coll()
        for key, value in props.items():
            setattr(self, key, value)

    def GetType(self):
        return _Type(self)


class _PerspectiveMember:
    """PerspectiveColumn / PerspectiveMeasure / PerspectiveHierarchy."""

    def __init__(self, ref_attr):
        self.ref_attr = ref_attr

    @property
    def Name(self):
        return getattr(self, self.ref_attr).Name


class _PerspectiveTable:
    def __init__(self, table=None, include_all=False):
        self.Table = table
        self.IncludeAll = include_all
        self.PerspectiveColumns = _Coll()
        self.PerspectiveMeasures = _Coll()
        self.PerspectiveHierarchies = _Coll()

    @property
    def Name(self):
        return self.Table.Name


class _FakePartition(_Obj):
    """A Partition whose SourceType follows its Source, as in TOM."""

    def __init__(self):
        super().__init__("Partition", Source=None)

    @property
    def SourceType(self):
        return getattr(self.Source, "kind", "M")


DATA_TYPE = _EnumType("DataType", ["String", "Int64", "Double", "DateTime"])
AGGREGATE = _EnumType("AggregateFunction", ["Default", "None", "Sum", "Count"])
COLUMN_TYPE = _EnumType("ColumnType", ["Data", "Calculated", "CalculatedTableColumn"])
MODE = _EnumType("ModeType", ["Import", "DirectQuery", "DirectLake"])
SOURCE_TYPE = _EnumType("PartitionSourceType", ["M", "Calculated", "Entity"])


def _column(table, name, column_type="Data", data_type="String", **props):
    defaults = {
        "IsHidden": False,
        "Description": "",
        "FormatString": "",
        "SortByColumn": None,
        "SummarizeBy": _Enum(AGGREGATE, "Default"),
    }
    column = _Obj(
        "Column",
        Name=name,
        Table=table,
        Type=_Enum(COLUMN_TYPE, column_type),
        DataType=_Enum(DATA_TYPE, data_type),
        **{**defaults, **props},
    )
    table.Columns.Add(column)
    return column


def _measure(table, name, expression, **props):
    defaults = {
        "IsHidden": False,
        "Description": "",
        "FormatStringDefinition": None,
        "DetailRowsDefinition": None,
        "KPI": None,
    }
    measure = _Obj(
        "Measure",
        Name=name,
        Table=table,
        Expression=expression,
        **{**defaults, **props},
    )
    table.Measures.Add(measure)
    return measure


def _table(model, name):
    table = _Obj(
        "Table",
        Name=name,
        IsHidden=False,
        Description="",
        CalculationGroup=None,
        Columns=_Coll(),
        Measures=_Coll(),
        Hierarchies=_Coll(),
        Partitions=_Coll(),
    )
    model.Tables.Add(table)
    return table


def _partition(table, name, source_type, expression):
    partition = _Obj(
        "Partition",
        Name=name,
        Table=table,
        Mode=_Enum(MODE, "Import"),
        SourceType=_Enum(SOURCE_TYPE, source_type),
        Source=types.SimpleNamespace(Expression=expression),
    )
    table.Partitions.Add(partition)
    return partition


def _build_model():
    model = _Obj(
        "Model",
        Name="Model",
        Description="",
        Tables=_Coll(),
        Relationships=_Coll(),
        Expressions=_Coll(),
        Functions=_Coll(),
        Perspectives=_Coll(),
        Roles=_Coll(),
        Cultures=_Coll(),
        Database=_Obj("Database", CompatibilityLevel=1604, Name="Sales Model"),
    )

    sales = _table(model, "Sales")
    _column(sales, "Amount", data_type="Double")
    _column(sales, "Region", IsHidden=True)
    _column(sales, "Double Amount", column_type="Calculated", Expression="[Amount] * 2")
    total = _measure(sales, "Total", "SUM(Sales[Amount])")
    _measure(sales, "Broken", "[Missing]", State="SemanticError", ErrorMessage="bad")
    _partition(sales, "Sales", "M", "let Source = 1 in Source")

    dates = _table(model, "Dates")
    _column(dates, "Date", column_type="CalculatedTableColumn", data_type="DateTime")
    _partition(dates, "Dates", "Calculated", "CALENDARAUTO()")

    perspective = _Obj("Perspective", Name="Exec", PerspectiveTables=_Coll())
    perspective.PerspectiveTables.Add(
        types.SimpleNamespace(
            Table=sales,
            PerspectiveColumns=[],
            PerspectiveMeasures=[types.SimpleNamespace(Measure=total)],
            PerspectiveHierarchies=[],
        )
    )
    model.Perspectives.Add(perspective)

    culture = _Obj(
        "Culture",
        Name="fr-FR",
        ObjectTranslations=_Coll(
            [types.SimpleNamespace(Object=total, Property="Caption", Value="Total FR")]
        ),
    )
    model.Cultures.Add(culture)
    return model


@pytest.fixture
def fake_clr(monkeypatch):
    """Installs stand-ins for the ``System`` and TOM modules."""

    system = types.ModuleType("System")
    system.Enum = _EnumBase
    system.DateTime = type("DateTime", (), {})
    system.Nullable = types.SimpleNamespace(GetUnderlyingType=lambda t: None)
    tom = types.ModuleType("Microsoft.AnalysisServices.Tabular")
    tom.Annotation = lambda: _Obj("Annotation")
    tom.TranslatedProperty = types.SimpleNamespace(
        Caption="Caption", Description="Description", DisplayFolder="DisplayFolder"
    )
    tom.PerspectiveTable = _PerspectiveTable
    tom.Table = lambda: _Obj(
        "Table",
        Columns=_Coll(),
        Measures=_Coll(),
        Hierarchies=_Coll(),
        Partitions=_Coll(),
        CalculationGroup=None,
        Description="",
        IsHidden=False,
    )
    tom.Partition = _FakePartition
    tom.CalculatedPartitionSource = lambda: types.SimpleNamespace(
        Expression="", kind="Calculated"
    )
    tom.CalculatedTableColumn = lambda: _Obj(
        "Column", Type="CalculatedTableColumn", SortByColumn=None, IsHidden=False
    )
    tom.JsonExtendedProperty = lambda: types.SimpleNamespace(Name="", Value="")
    tom.RelatedColumnDetails = lambda: types.SimpleNamespace(GroupByColumns=_Coll())
    tom.GroupByColumn = lambda: types.SimpleNamespace(GroupingColumn=None)
    tom.ModeType = types.SimpleNamespace(Import="Import")
    tom.DataType = types.SimpleNamespace(
        String=_Enum(DATA_TYPE, "String"), Int64=_Enum(DATA_TYPE, "Int64")
    )
    tom.PerspectiveColumn = lambda: _PerspectiveMember("Column")
    tom.PerspectiveMeasure = lambda: _PerspectiveMember("Measure")
    tom.PerspectiveHierarchy = lambda: _PerspectiveMember("Hierarchy")
    microsoft = types.ModuleType("Microsoft")
    analysis_services = types.ModuleType("Microsoft.AnalysisServices")
    microsoft.AnalysisServices = analysis_services
    analysis_services.Tabular = tom
    monkeypatch.setitem(sys.modules, "System", system)
    monkeypatch.setitem(sys.modules, "Microsoft", microsoft)
    monkeypatch.setitem(sys.modules, "Microsoft.AnalysisServices", analysis_services)
    monkeypatch.setitem(sys.modules, "Microsoft.AnalysisServices.Tabular", tom)
    return tom


def _find(nodes, name):
    return next(n for n in nodes if n["name"] == name)


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------
def test_payload_sends_property_metadata_once_and_only_non_empty_values(fake_clr):
    payload = sx._model_payload(types.SimpleNamespace(model=_build_model()))

    column_specs = {spec[0]: spec for spec in payload["specs"]["column"]}
    assert column_specs["DataType"] == [
        "DataType",
        "Data type",
        "General",
        True,
        "enum",
        "DataType",
    ]
    assert column_specs["IsHidden"][4] == "bool"
    assert payload["enums"]["DataType"] == ["String", "Int64", "Double", "DateTime"]

    sales = _find(payload["tables"], "Sales")
    amount = _find(sales["columns"], "Amount")
    assert amount["v"] == {
        "Name": "Amount",
        "Type": "Data",
        "DataType": "Double",
        "SummarizeBy": "Default",
    }
    assert "hidden" not in amount and "annotations" not in amount
    assert _find(sales["columns"], "Region")["hidden"] is True
    # A regular table has no calculation group, so Precedence is not editable.
    assert "CalculationGroup.Precedence" in sales["x"]


def test_payload_maps_translations_perspectives_errors_and_expressions(fake_clr):
    payload = sx._model_payload(types.SimpleNamespace(model=_build_model()))

    sales = _find(payload["tables"], "Sales")
    total = _find(sales["measures"], "Total")
    assert total["translations"] == {"fr-FR": {"caption": "Total FR"}}
    assert total["perspectives"] == ["Exec"]
    assert "perspectives" not in _find(sales["measures"], "Broken")
    assert [(e["name"], e["state"], e["message"]) for e in payload["errors"]] == [
        ("Broken", "SemanticError", "bad")
    ]
    assert payload["errors"][0]["key"] == sx._explorer_key(
        "measure", "Sales", name="Broken"
    )

    # Only non-empty expressions are sent.
    assert total["e"] == {"Expression": "SUM(Sales[Amount])"}
    calc_column = _find(sales["columns"], "Double Amount")
    assert calc_column["e"] == {"Expression": "[Amount] * 2"}
    assert _find(sales["partitions"], "Sales")["e"] == {
        "Expression": "let Source = 1 in Source"
    }

    # The calculated table's DAX lives on (and is edited through) its partition.
    dates = _find(payload["tables"], "Dates")
    assert dates["calc_partition"] == "Dates"
    assert _find(dates["partitions"], "Dates")["e"] == {"Expression": "CALENDARAUTO()"}


# ---------------------------------------------------------------------------
# Saving
# ---------------------------------------------------------------------------
def _tom(model):
    model.UndoLocalChanges = lambda: setattr(model, "undone", True)
    return types.SimpleNamespace(model=model, _readonly=False)


def test_apply_changes_resolves_targets_before_renaming(fake_clr):
    model = _build_model()
    tom = _tom(model)
    edits = [
        {
            "target": {"type": "table", "name": "Sales"},
            "property": "Name",
            "value": "Revenue",
        },
        {
            "target": {"type": "column", "table": "Sales", "name": "Amount"},
            "property": "FormatString",
            "value": "#,0",
        },
        {
            "target": {"type": "column", "table": "Sales", "name": "Amount"},
            "property": "DataType",
            "value": "int64",
        },
        {
            "target": {"type": "measure", "table": "Sales", "name": "Total"},
            "property": "Annotations",
            "value": '{"Owner": "Finance"}',
        },
    ]

    assert sx._apply_changes(tom, [], edits, []) == 4

    revenue = model.Tables["Revenue"]
    amount = revenue.Columns["Amount"]
    assert amount.FormatString == "#,0"
    assert str(amount.DataType) == "Int64"
    annotation = revenue.Measures["Total"].Annotations[0]
    assert (annotation.Name, annotation.Value) == ("Owner", "Finance")


def test_apply_changes_discards_the_batch_when_an_edit_fails(fake_clr):
    model = _build_model()
    tom = _tom(model)
    edits = [
        {
            "target": {"type": "table", "name": "Sales"},
            "property": "Name",
            "value": "Revenue",
        },
        {
            "target": {"type": "measure", "table": "Sales", "name": "Nope"},
            "property": "Name",
            "value": "x",
        },
    ]

    with pytest.raises(ValueError, match="'Sales'\\[Nope\\] was not found"):
        sx._apply_changes(tom, [], edits, [])

    assert model.undone is True
    # Closing a read-write connection would save; it must not.
    assert tom._readonly is True
    assert model.Tables.ContainsName("Sales")


def test_apply_changes_applies_duplicates_before_deletes(fake_clr):
    model = _build_model()
    total = model.Tables["Sales"].Measures["Total"]
    total.Clone = lambda: _Obj("Measure", Name=total.Name, Table=total.Table)
    ops = [
        {
            "op": "delete",
            "target": {"type": "measure", "table": "Sales", "name": "Total"},
        },
        {
            "op": "duplicate",
            "target": {"type": "measure", "table": "Sales", "name": "Total"},
        },
    ]

    sx._apply_changes(_tom(model), [], [], ops)

    names = [m.Name for m in model.Tables["Sales"].Measures]
    assert names == ["Broken", "Total copy"]


def test_perspective_membership_is_idempotent_and_expands_include_all(fake_clr):
    model = _build_model()
    sales = model.Tables["Sales"]
    total = sales.Measures["Total"]
    model.Perspectives.Clear()
    exec_ = _Obj("Perspective", Name="Exec", PerspectiveTables=_Coll())
    exec_.PerspectiveTables.Add(_PerspectiveTable(sales, include_all=True))
    ops = _Obj("Perspective", Name="Ops", PerspectiveTables=_Coll())
    model.Perspectives.extend([exec_, ops])
    tom = types.SimpleNamespace(model=model)

    for _ in range(2):
        sx._apply_edit(tom, total, "Perspectives", '["Exec", "Ops"]')
    assert exec_.PerspectiveTables["Sales"].IncludeAll is True
    assert [m.Name for m in ops.PerspectiveTables["Sales"].PerspectiveMeasures] == [
        "Total"
    ]

    sx._apply_edit(tom, total, "Perspectives", '["Ops"]')
    exec_sales = exec_.PerspectiveTables["Sales"]
    assert exec_sales.IncludeAll is False
    assert [m.Name for m in exec_sales.PerspectiveMeasures] == ["Broken"]
    assert [c.Name for c in exec_sales.PerspectiveColumns] == [
        "Amount",
        "Region",
        "Double Amount",
    ]


def test_deleting_a_column_detaches_everything_which_references_it(fake_clr):
    model = _build_model()
    sales = model.Tables["Sales"]
    region, amount = sales.Columns["Region"], sales.Columns["Amount"]
    geography = _table(model, "Geography")
    model.Relationships.Add(
        _Obj(
            "Relationship",
            Name="r",
            FromTable=sales,
            FromColumn=region,
            ToTable=geography,
            ToColumn=_column(geography, "Region"),
        )
    )
    amount.SortByColumn = region
    hierarchy = _Obj("Hierarchy", Name="Geo", Table=sales, Levels=_Coll())
    hierarchy.Levels.Add(
        _Obj("Level", Name="Region", Ordinal=0, Column=region, Hierarchy=hierarchy)
    )
    sales.Hierarchies.Add(hierarchy)
    culture = model.Cultures["fr-FR"]
    culture.ObjectTranslations.Add(
        types.SimpleNamespace(Object=region, Property="Caption", Value="Region FR")
    )
    perspective_table = _PerspectiveTable(sales)
    member = _PerspectiveMember("Column")
    member.Column = region
    perspective_table.PerspectiveColumns.Add(member)
    model.Perspectives.Clear()
    model.Perspectives.Add(
        _Obj("Perspective", Name="Exec", PerspectiveTables=_Coll([perspective_table]))
    )
    permission = types.SimpleNamespace(
        Name="Sales",
        ColumnPermissions=_Coll([types.SimpleNamespace(Name="Region")]),
    )
    model.Roles.Add(_Obj("Role", Name="West", TablePermissions=_Coll([permission])))
    ops = [
        {
            "op": "delete",
            "target": {"type": "column", "table": "Sales", "name": "Region"},
        }
    ]

    sx._apply_changes(_tom(model), [], [], ops)

    assert not sales.Columns.ContainsName("Region")
    assert model.Relationships.Count == 0
    assert amount.SortByColumn is None
    # The hierarchy's only level was built on the column, so it goes too.
    assert not sales.Hierarchies.ContainsName("Geo")
    assert [t.Object for t in culture.ObjectTranslations] == [sales.Measures["Total"]]
    assert perspective_table.PerspectiveColumns.Count == 0
    assert permission.ColumnPermissions.Count == 0


def test_duplicating_a_table_gives_its_measures_unique_names(fake_clr):
    model = _build_model()
    sales = model.Tables["Sales"]

    def _clone():
        copy = _table(_Obj("Model", Tables=_Coll()), sales.Name)
        copy.LineageTag = "original"
        for measure in sales.Measures:
            _measure(copy, measure.Name, measure.Expression, LineageTag="original")
        return copy

    sales.Clone = _clone
    ops = [{"op": "duplicate", "target": {"type": "table", "name": "Sales"}}]

    sx._apply_changes(_tom(model), [], [], ops)

    copy = model.Tables["Sales copy"]
    assert [m.Name for m in copy.Measures] == ["Total copy", "Broken copy"]
    assert "original" not in {copy.LineageTag, *(m.LineageTag for m in copy.Measures)}


def test_field_parameter_create_and_group_column_follow_the_expression(fake_clr):
    model = _build_model()
    tom = _tom(model)
    expression = (
        "{\n"
        '    ( "Total", NAMEOF ( [Total] ), 0, "KPIs" ),\n'
        '    ( "Amount", NAMEOF ( \'Sales\'[Amount] ), 1, "KPIs" )\n'
        "}"
    )
    create = {
        "type": "fieldParameter",
        "name": "Metric",
        "table": "",
        "props": {"Expression": expression, "Description": "Pick a metric"},
    }

    sx._apply_changes(tom, [create], [], [])

    table = model.Tables["Metric"]
    value, fields, order, group = table.Columns
    assert [c.SourceColumn for c in table.Columns] == [
        "[Value1]",
        "[Value2]",
        "[Value3]",
        "[Value4]",
    ]
    assert [c.Name for c in (value, fields, order, group)] == [
        "Metric",
        "Metric Fields",
        "Metric Order",
        "Metric Group",
    ]
    assert table.Partitions[0].Source.Expression == expression
    assert table.Description == "Pick a metric"
    assert value.SortByColumn is order and fields.SortByColumn is order
    assert [g.GroupingColumn for g in value.RelatedColumnDetails.GroupByColumns] == [
        fields
    ]
    assert [(p.Name, p.Value) for p in fields.ExtendedProperties] == [
        ("ParameterMetadata", '{"version":3,"kind":2}')
    ]

    # Dropping the hierarchy values drops the group column again.
    partition = table.Partitions[0]
    partition.Table = table
    sx._apply_edit(tom, partition, "Expression", expression.replace(', "KPIs"', ""))
    assert [c.SourceColumn for c in table.Columns] == [
        "[Value1]",
        "[Value2]",
        "[Value3]",
    ]


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
def _dep(obj_type, table, obj, ref_type, ref_table, ref):
    return {
        "OBJECT_TYPE": obj_type,
        "TABLE": table,
        "OBJECT": obj,
        "REFERENCED_OBJECT_TYPE": ref_type,
        "REFERENCED_TABLE": ref_table,
        "REFERENCED_OBJECT": ref,
    }


def test_dependency_graph_dedupes_nodes_and_normalizes_object_types():
    rows = [
        _dep("MEASURE", "Sales", "A", "MEASURE", "Sales", "B"),
        _dep("MEASURE", "Sales", "B", "MEASURE", "Sales", "A"),
        _dep("MEASURE", "Sales", "B", "CALC_COLUMN", "Sales", "Amount"),
        _dep("CALC_TABLE", "Dates", "Dates", "COLUMN", "Sales", "Amount"),
        _dep("ROWS_ALLOWED", "Sales", "West", "COLUMN", "Sales", "Amount"),
        _dep("MEASURE", "Sales", "A", "MEASURE", "Sales", "A"),
    ]

    graph = sx._dependency_graph(rows)

    nodes = [(n["type"], n["table"], n["name"]) for n in graph["nodes"]]
    assert nodes == [
        ("measure", "Sales", "A"),
        ("measure", "Sales", "B"),
        # A calculated column and a column reference are the same object.
        ("column", "Sales", "Amount"),
        ("table", "", "Dates"),
        ("Rows Allowed", "Sales", "West"),
    ]
    assert graph["nodes"][2]["key"] == sx._explorer_key(
        "column", "Sales", name="Amount"
    )
    assert graph["nodes"][4]["key"] == ""
    # Self-references are dropped; the A <-> B cycle is kept for the frontend.
    assert graph["edges"] == [[0, 1], [1, 0], [1, 2], [3, 2], [4, 2]]
