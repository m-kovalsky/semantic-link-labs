"""Tests for the Semantic Explorer backend (metadata payload and saving).

TOM / pythonnet are unavailable offline, so the tests use small stand-ins for
the TOM objects and for the ``System`` / ``Microsoft.AnalysisServices.Tabular``
modules the explorer imports.
"""

import json
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


class _FakeRelationship(_Obj):
    """SingleColumnRelationship: the tables follow the columns."""

    def __init__(self):
        super().__init__("Relationship", FromColumn=None, ToColumn=None)

    @property
    def FromTable(self):
        return self.FromColumn.Table

    @property
    def ToTable(self):
        return self.ToColumn.Table


class _FakeTablePermission:
    def __init__(self):
        self.Table = None
        self.FilterExpression = None
        self.MetadataPermission = "Default"
        self.ColumnPermissions = _Coll()

    @property
    def Name(self):
        return self.Table.Name


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
    tom.Function = lambda: _Obj("Function", Expression="")
    tom.SingleColumnRelationship = _FakeRelationship
    tom.RelationshipEndCardinality = _EnumType(
        "RelationshipEndCardinality", ["None", "One", "Many"]
    )
    tom.CrossFilteringBehavior = _EnumType(
        "CrossFilteringBehavior", ["OneDirection", "BothDirections", "Automatic"]
    )
    tom.MetadataPermission = _EnumType(
        "MetadataPermission", ["Default", "None", "Read"]
    )
    tom.TablePermission = _FakeTablePermission
    tom.ColumnPermission = lambda: types.SimpleNamespace(
        Column=None, MetadataPermission=None
    )
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
# DAXLib packages
# ---------------------------------------------------------------------------
def _function(model, name, **annotations):
    fn = _Obj("Function", Name=name, Expression="old")
    for key, value in annotations.items():
        fn.Annotations.Add(_Obj("Annotation", Name=key, Value=value))
    model.Functions.Add(fn)
    return fn


def _annotations_of(fn):
    return {a.Name: a.Value for a in fn.Annotations}


def test_daxlib_package_install_replaces_only_that_packages_functions(fake_clr):
    model = _build_model()
    tom = _tom(model)
    _function(model, "Pkg.Old", DAXLIB_PackageId="Pkg", DAXLIB_PackageVersion="1.0")
    # Added by sempy_labs' add_package_to_semantic_model (matched by prefix).
    legacy = _function(model, "Pkg.Keep", Pkg="1.0")
    # Name looks like the package but it belongs to another one.
    _function(model, "Pkg.Foreign", DAXLIB_PackageId="Other")
    create = {
        "type": "daxlibPackage",
        "name": "Pkg",
        "table": "",
        "props": {
            "version": "2.0",
            "upgradeCompatibility": True,
            "functions": [
                {"name": "Pkg.Keep", "definition": "() => 1"},
                {"name": "Pkg.New", "definition": "() => 2"},
            ],
        },
    }

    sx._apply_changes(tom, [create], [], [])

    assert model.Database.CompatibilityLevel == 1702
    assert [f.Name for f in model.Functions] == ["Pkg.Keep", "Pkg.Foreign", "Pkg.New"]
    assert model.Functions["Pkg.Keep"] is legacy
    assert legacy.Expression == "() => 1"
    assert _annotations_of(legacy) == {
        "Pkg": "2.0",
        "DAXLIB_PackageId": "Pkg",
        "DAXLIB_PackageVersion": "2.0",
    }
    new = model.Functions["Pkg.New"]
    assert new.Expression == "() => 2" and new.LineageTag
    assert _annotations_of(new) == {
        "DAXLIB_PackageId": "Pkg",
        "DAXLIB_PackageVersion": "2.0",
    }


def test_daxlib_package_requires_a_confirmed_compatibility_upgrade(fake_clr):
    model = _build_model()
    tom = _tom(model)
    create = {
        "type": "daxlibPackage",
        "name": "Pkg",
        "props": {
            "version": "1.0",
            "functions": [{"name": "Pkg.A", "definition": "1"}],
        },
    }

    with pytest.raises(ValueError, match="compatibility level 1702"):
        sx._apply_changes(tom, [create], [], [])

    assert model.undone is True
    assert model.Database.CompatibilityLevel == 1604


def test_daxlib_versions_sort_releases_above_prereleases():
    versions = ["0.1.0-beta", "1.0.0", "0.10.0", "1.0.0-rc.2", "1.0.0-rc.10", "0.2"]

    ordered = sorted(versions, key=sx._daxlib_version_key, reverse=True)

    assert ordered == [
        "1.0.0",
        "1.0.0-rc.10",
        "1.0.0-rc.2",
        "0.10.0",
        "0.2",
        "0.1.0-beta",
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


@pytest.mark.parametrize(
    "body",
    [
        "(\n\t\t\tx : INT64\n\t\t) =>\n\t\t\tx * 2\n\n",  # unfenced
        "\n\t\t(\n\t\t\tx : INT64\n\t\t) =>\n\t\t\tx * 2\n\t\t",  # fenced
    ],
)
def test_daxlib_function_definitions_lose_their_tmdl_indentation(body):
    assert sx._daxlib_definition(body) == "(\n\tx : INT64\n) =>\n\tx * 2"


# ---------------------------------------------------------------------------
# Relationship and security managers
# ---------------------------------------------------------------------------
def test_relationship_create_and_column_edits_resolve_saved_names(fake_clr):
    model = _build_model()
    dates = model.Tables["Dates"]
    sales = model.Tables["Sales"]
    order_date = _column(sales, "OrderDate", data_type="DateTime")
    create = {
        "type": "relationship",
        "name": "Sales[Region] -> Dates[Date]",
        "table": "",
        "props": {
            "fromTable": "Sales",
            "fromColumn": "Region",
            "toTable": "Dates",
            "toColumn": "Date",
            "FromCardinality": "Many",
            "ToCardinality": "One",
            "CrossFilteringBehavior": "BothDirections",
            "IsActive": False,
        },
    }

    sx._apply_changes(_tom(model), [create], [], [])

    rel = model.Relationships[0]
    assert rel.FromColumn is sales.Columns["Region"]
    assert rel.ToColumn is dates.Columns["Date"]
    assert [str(rel.FromCardinality), str(rel.ToCardinality)] == ["Many", "One"]
    assert str(rel.CrossFilteringBehavior) == "BothDirections"
    assert rel.IsActive is False

    edit = {
        "target": {"type": "relationship", "name": rel.Name},
        "property": "FromColumn",
        "value": "OrderDate",
    }
    sx._apply_changes(_tom(model), [], [edit], [])
    assert rel.FromColumn is order_date

    bad = dict(edit, value="Nope")
    with pytest.raises(ValueError, match=r"'Sales'\[Nope\] column was not found"):
        sx._apply_changes(_tom(model), [], [bad], [])


def test_object_level_security_sets_and_resets_permissions(fake_clr):
    model = _build_model()
    role = _Obj("Role", Name="Reader", TablePermissions=_Coll())
    model.Roles.Add(role)
    tom = _tom(model)

    sx._apply_edit(
        tom,
        role,
        "Ols",
        json.dumps(
            [
                {"table": "Dates", "column": "", "permission": "None"},
                {"table": "Sales", "column": "Region", "permission": "None"},
                {"table": "Missing", "column": "", "permission": "None"},
            ]
        ),
    )

    dates, sales = role.TablePermissions["Dates"], role.TablePermissions["Sales"]
    assert str(dates.MetadataPermission) == "None"
    assert str(sales.MetadataPermission) == "Default"
    [region] = sales.ColumnPermissions
    assert region.Column is model.Tables["Sales"].Columns["Region"]
    assert str(region.MetadataPermission) == "None"

    # Keeping an RLS filter keeps its table permission; the rest is removed.
    sales.FilterExpression = '[Region] = "West"'
    sx._apply_edit(tom, role, "Ols", "[]")
    assert [tp.Name for tp in role.TablePermissions] == ["Sales"]
    assert sales.ColumnPermissions.Count == 0


@pytest.mark.parametrize(
    "stats, expected",
    [
        ({"a": (10, 4), "b": (4, 4)}, ("Sales", "Dates", "Many", "One", True)),
        ({"a": (4, 4), "b": (10, 4)}, ("Dates", "Sales", "Many", "One", True)),
        ({"a": (4, 4), "b": (4, 4)}, ("Sales", "Dates", "One", "One", True)),
        ({"a": (10, 4), "b": (10, 4)}, ("Sales", "Dates", "Many", "Many", False)),
    ],
)
def test_relationship_inference_puts_the_unique_column_on_the_one_side(stats, expected):
    pair = {
        "from_table": "Sales",
        "from_column": "Key",
        "to_table": "Dates",
        "to_column": "Key",
    }
    measured = {("Sales", "Key"): stats["a"], ("Dates", "Key"): stats["b"]}

    result = sx._relationship_inference(pair, measured)

    assert (
        result["from_table"],
        result["to_table"],
        result["from_cardinality"],
        result["to_cardinality"],
        result["confident"],
    ) == expected


# ---------------------------------------------------------------------------
# Query scale-out, cache warming and .vpax tools
# ---------------------------------------------------------------------------
def test_qso_sync_status_keeps_every_replica(monkeypatch):
    from sempy_labs import _query_scale_out as qso

    body = {
        "scaleOutStatus": "Enabled",
        "commitVersion": 3,
        "targetSyncVersion": 3,
        "minActiveReadVersion": 2,
        "scaleOutReplicas": [
            {"replicaId": "a", "replicaType": "Read", "replicaVersion": 3},
            {"replicaId": "b", "replicaType": "Read", "replicaVersion": 2},
        ],
    }
    monkeypatch.setattr(qso, "resolve_workspace_id", lambda w: "ws")
    monkeypatch.setattr(qso, "resolve_dataset_name_and_id", lambda d, w: ("M", "ds"))
    monkeypatch.setattr(
        qso, "_base_api", lambda **kw: types.SimpleNamespace(json=lambda: body)
    )

    _, replicas = qso.qso_sync_status("M", "ws")

    assert list(replicas["Replica ID"]) == ["a", "b"]


def test_qso_state_reports_disabled_models_without_sync_rows(monkeypatch):
    import pandas as pd
    from sempy_labs import _query_scale_out as qso

    settings = pd.DataFrame(
        [
            {
                "Storage Mode": "Small",
                "QSO Auto Sync Enabled": None,
                "QSO Max Read Only Replicas": 0,
            }
        ]
    )
    monkeypatch.setattr(qso, "list_qso_settings", lambda **kw: settings)
    monkeypatch.setattr(
        qso, "qso_sync_status", lambda **kw: (pd.DataFrame(), pd.DataFrame())
    )

    state = sx._qso_state("ds", "ws")

    assert state["settings"] == {
        "storageMode": "Small",
        "autoSync": True,
        "maxReplicas": 0,
    }
    assert state["status"] == {
        "scaleOutStatus": "StorageModeNotSupported",
        "replicas": [],
    }


def test_warming_columns_is_silent_and_rescans(monkeypatch):
    from sempy_labs.directlake import _warm_cache

    calls = {}

    def fake_put(dataset, workspace, col_df, return_dataframe=True):
        calls["df"] = col_df.to_dict("records")
        calls["progress"] = list(_warm_cache.tqdm(["T"]))
        print("noise")

    monkeypatch.setattr(_warm_cache, "_put_columns_into_memory", fake_put)
    monkeypatch.setattr(sx, "_column_residency", lambda d, w: [{"table": "T"}])
    original_tqdm = _warm_cache.tqdm

    result = sx._warm_cache(
        "ds",
        "ws",
        {"columns": [{"table": "T", "column": "C"}, {"table": "", "column": "X"}]},
    )

    assert calls["df"] == [{"Table Name": "T", "Column Name": "C"}]
    assert calls["progress"] == ["T"]
    assert _warm_cache.tqdm is original_tqdm
    assert result == {"warmed": 1, "columns": [{"table": "T"}]}


def test_create_vpax_surfaces_an_existing_file(monkeypatch):
    from sempy_labs import _vpax

    seen = {}

    def fake_create_vpax(**kwargs):
        seen.update(kwargs)
        print("The Files/Sales.vpax file already exists in the 'LH' lakehouse.")

    monkeypatch.setattr(_vpax, "create_vpax", fake_create_vpax)
    data = {"lakehouseId": "lh", "lakehouseWorkspaceId": "lw", "fileName": "Sales.vpax"}

    with pytest.raises(ValueError, match="Turn on 'Overwrite'"):
        sx._create_vpax_file("ds", "ws", data)

    assert seen["file_path"] == "Sales" and seen["overwrite"] is False


# ---------------------------------------------------------------------------
# New semantic models
# ---------------------------------------------------------------------------
def test_create_blank_model_validates_and_creates_at_level_1706(monkeypatch):
    from sempy_labs import _generate_semantic_model as gen
    from sempy_labs import _ui_components as ui

    created = {}

    def fake_create(**kwargs):
        created.update(kwargs)
        print("noise")
        return "new-id"

    monkeypatch.setattr(gen, "create_blank_semantic_model", fake_create)
    monkeypatch.setattr(
        ui, "list_picker_datasets", lambda ws: [{"id": "1", "name": "Sales"}]
    )

    with pytest.raises(ValueError, match="already exists"):
        sx._create_blank_model("ws", " sales ")
    with pytest.raises(ValueError, match="cannot contain quotes"):
        sx._create_blank_model("ws", "Bob's model")
    with pytest.raises(ValueError, match="Select a workspace"):
        sx._create_blank_model("", "Finance")
    assert not created

    assert sx._create_blank_model("ws", " Finance ") == "new-id"
    assert created == {
        "dataset": "Finance",
        "compatibility_level": 1706,
        "workspace": "ws",
        "overwrite": False,
    }


# ---------------------------------------------------------------------------
# Semantic views (OneLake DFS stand-in)
# ---------------------------------------------------------------------------
class _FakeResponse:
    def __init__(self, status_code=200, body=None, content=b"", headers=None):
        self.status_code = status_code
        self._body = body or {}
        self.content = content
        self.headers = headers or {}

    def json(self):
        return self._body


class _FakeOneLake:
    """An in-memory lakehouse (paths relative to the item root)."""

    def __init__(self, files=None, folders=()):
        self.files = {k: v.encode("utf-8") for k, v in (files or {}).items()}
        self.folders = set(folders)
        for path in self.files:
            parts = path.split("/")[:-1]
            for i in range(1, len(parts) + 1):
                self.folders.add("/".join(parts[:i]))
        self.calls = []

    def etag(self, name):
        return '"%s"' % hash(self.files.get(name, b""))

    def __call__(
        self, method, workspace_id, path="", params=None, data=None, headers=None
    ):
        params = params or {}
        self.calls.append((method, path, dict(params), data, dict(headers or {})))
        if method == "GET" and params.get("resource") == "filesystem":
            item, directory = params["directory"].split("/", 1)
            if directory not in self.folders:
                return _FakeResponse(404)
            recursive = params["recursive"] == "true"
            paths = []
            for folder in sorted(self.folders):
                rest = sx._relative_path(folder, directory)
                if rest and (recursive or "/" not in rest):
                    paths.append({"name": f"{item}/{folder}", "isDirectory": "true"})
            for name, content in sorted(self.files.items()):
                rest = sx._relative_path(name, directory)
                if rest and (recursive or "/" not in rest):
                    paths.append(
                        {
                            "name": f"{item}/{name}",
                            "contentLength": str(len(content)),
                            "etag": self.etag(name),
                        }
                    )
            return _FakeResponse(body={"paths": paths})
        rel = path.split("/", 1)[1]
        if method == "PUT" and "x-ms-rename-source" in (headers or {}):
            from urllib.parse import unquote

            # '/<workspace>/<item>/<path>'
            source = unquote(headers["x-ms-rename-source"]).split("/", 3)[3]
            if rel in self.files or rel in self.folders:
                return _FakeResponse(409)

            def moved(p):
                return p.replace(source, rel, 1)

            def inside(p):
                return p == source or p.startswith(source + "/")

            self.files = {
                (moved(k) if inside(k) else k): v for k, v in self.files.items()
            }
            self.folders = {moved(f) if inside(f) else f for f in self.folders}
            return _FakeResponse(201)
        if method == "GET":
            if rel not in self.files:
                return _FakeResponse(404)
            return _FakeResponse(content=self.files[rel])
        if method == "PUT":
            headers = headers or {}
            exists = rel in self.files or rel in self.folders
            if exists and headers.get("If-None-Match") == "*":
                return _FakeResponse(409)
            if "If-Match" in headers and headers["If-Match"] != self.etag(rel):
                return _FakeResponse(412)
            if params.get("resource") == "directory":
                self.folders.add(rel)
            else:
                self.files[rel] = b""
            return _FakeResponse(201)
        if method == "PATCH" and params.get("action") == "append":
            self.files[rel] += data
            return _FakeResponse(202)
        return _FakeResponse(200)


def test_list_semantic_views_lists_only_view_folders(monkeypatch):
    lake = _FakeOneLake(
        files={
            "Context/Semantic Views/sales/schema/schema.yaml": "x",
            "Context/Semantic Views/sales/metrics.yaml": "x",
            "Context/Semantic Views/Finance/finance.yaml": "x",
            "Context/Semantic Views/readme.txt": "x",
        }
    )
    monkeypatch.setattr(sx, "_onelake_request", lake)
    assert [v["name"] for v in sx._list_semantic_views("ws", "lh")] == [
        "Finance",
        "sales",
    ]

    monkeypatch.setattr(sx, "_onelake_request", _FakeOneLake())
    assert sx._list_semantic_views("ws", "lh") == []


def test_create_semantic_view_writes_a_valid_starter_definition(monkeypatch):
    import yaml

    lake = _FakeOneLake(files={"Context/Semantic Views/Sales/s.yaml": "x"})
    monkeypatch.setattr(sx, "_onelake_request", lake)

    assert sx._create_semantic_view("ws", "lh", " orders ") == "orders"
    path = "Context/Semantic Views/orders/orders_semantic_view.yaml"
    starter = yaml.safe_load(lake.files[path].decode("utf-8"))
    assert starter == {"version": "0.2.0.dev0", "name": "orders"}
    puts = [c for c in lake.calls if c[0] == "PUT"]
    assert all(c[4].get("If-None-Match") == "*" for c in puts)
    flush = [c for c in lake.calls if c[2].get("action") == "flush"][-1]
    assert flush[2]["position"] == str(len(lake.files[path]))

    with pytest.raises(ValueError, match="already exists"):
        sx._create_semantic_view("ws", "lh", "SALES")
    with pytest.raises(ValueError, match="cannot contain"):
        sx._create_semantic_view("ws", "lh", "a/b")


_VIEW_SCHEMA = """
version: 0.2.0.dev0
name: sales
description: Sales definitions.
datasets:
  - name: sales
    source: dbo.sales
    description: One row per sales line.
    primary_key: [sale_id]
  - name: customers
    source: dbo.customers
relationships:
  - name: sales_to_customers
    from: sales
    to: customers
    from_columns: [customer_id]
    to_columns: [customer_id]
"""

_VIEW_METRICS = """
version: 0.2.0.dev0
name: sales
metrics:
  - name: Total Net Sales
    description: Sales after discounts.
    expression:
      dialects:
        - dialect: DAX
          expression: "SUM('sales'[net_amount])"
        - dialect: ANSI_SQL
          expression: SUM(net_amount)
  - name: SQL only
    expression:
      dialects:
        - dialect: SNOWFLAKE
          expression: COUNT(*)
"""

_VIEW_TMDL = """model Model
\tculture: en-US

/// One row per sales line.
table sales
\tlineageTag: 1

\t/// Net amount after discounts.
\tcolumn net_amount
\t\tdataType: decimal
\t\tsourceColumn: net_amount

\tcolumn customer_id
\t\tdataType: int64
\t\tisHidden
\t\tsourceColumn: customer_id

\tpartition sales = entity
\t\tmode: directLake
\t\tsource
\t\t\tentityName: sales
\t\t\tschemaName: dbo

table customers

\tcolumn customer_id
\t\tdataType: int64

table date

\tcolumn date
\t\tdataType: dateTime

\tpartition date = entity
\t\tsource
\t\t\tentityName: date
\t\t\tschemaName: dbo

table Metrics

\t/// Sales after discounts.
\tmeasure 'Total Net Sales' =
\t\t\tSUM(
\t\t\t\t'sales'[net_amount]
\t\t\t)
\t\tformatString: #,0.00

relationship 4b1d
\tfromColumn: sales.customer_id
\ttoColumn: customers.customer_id
"""


def test_semantic_view_payload_merges_yaml_with_published_tmdl(monkeypatch):
    lake = _FakeOneLake(
        files={
            "Context/Semantic Views/sales/schema.yaml": _VIEW_SCHEMA,
            "Context/Semantic Views/sales/metrics/metrics.yaml": _VIEW_METRICS,
            "Context/Semantic Views/sales/notes.yml": "ignored",
            "SystemContext/Semantic Views/sales/definition/model.tmdl": _VIEW_TMDL,
        }
    )
    monkeypatch.setattr(sx, "_onelake_request", lake)
    monkeypatch.setattr(
        sx,
        "_lakehouse_table_columns",
        lambda *a: pytest.fail("published views use TMDL"),
    )

    payload = sx._semantic_view_payload("ws", "lh", "sales")

    assert payload["view"]["published"] is True
    assert payload["view"]["description"] == "Sales definitions."
    datasets = {d["name"]: d for d in payload["datasets"]}
    sales = datasets["sales"]
    assert sales["file"] == "schema.yaml" and sales["columns_from"] == "published"
    assert sales["ignored"] == ["primary_key"]
    assert [(c["name"], c["data_type"], c["is_hidden"]) for c in sales["columns"]] == [
        ("net_amount", "decimal", False),
        ("customer_id", "int64", True),
    ]
    assert sales["columns"][0]["description"] == "Net amount after discounts."
    # A table Fabric inferred is shown; the Metrics table isn't a dataset.
    assert datasets["date"]["inferred"] and datasets["date"]["source"] == "dbo.date"
    assert "Metrics" not in datasets
    rel = payload["relationships"][0]
    assert (rel["from_column"], rel["to_column"], rel["published"]) == (
        "customer_id",
        "customer_id",
        True,
    )
    metrics = {m["name"]: m for m in payload["metrics"]}
    total = metrics["Total Net Sales"]
    assert total["file"] == "metrics/metrics.yaml"
    assert total["dax"] == "SUM('sales'[net_amount])"
    assert total["published"] and total["format_string"] == "#,0.00"
    assert [d["dialect"] for d in total["dialects"]] == ["DAX", "ANSI_SQL"]
    assert metrics["SQL only"]["dax"] == "" and not metrics["SQL only"]["published"]
    messages = [i["message"] for i in payload["issues"]]
    assert any("SQL only" in m and "no DAX" in m for m in messages)
    assert any("primary_key" in m for m in messages)
    files = {(f["area"], f["path"]): f for f in payload["files"]}
    assert files[("definition", "notes.yml")]["ignored"]
    assert files[("generated", "definition/model.tmdl")]["language"] == "tmdl"


def test_unpublished_semantic_view_reads_columns_from_the_lakehouse(monkeypatch):
    lake = _FakeOneLake(
        files={
            "Context/Semantic Views/v/v.yaml": (
                "version: 0.2.0.dev0\nname: v\ndescription: A\n"
                "datasets:\n  - name: s\n    source: sales.orders\n"
            ),
            "Context/Semantic Views/v/w.yaml": (
                "version: 0.1\nname: v\ndescription: B\n"
                "relationships:\n  - name: r\n    from: s\n    to: c\n"
                "    from_columns: [a, b]\n    to_columns: [a, b]\n"
            ),
            "Context/Semantic Views/v/bad.yaml": "name: [unclosed",
        }
    )
    monkeypatch.setattr(sx, "_onelake_request", lake)
    monkeypatch.setattr(
        sx,
        "_lakehouse_table_columns",
        lambda ws, lh, source: [{"name": source, "data_type": "string"}],
    )

    payload = sx._semantic_view_payload("ws", "lh", "v")

    assert payload["view"]["published"] is False
    ds = payload["datasets"][0]
    assert (
        ds["columns_from"] == "lakehouse" and ds["columns"][0]["name"] == "sales.orders"
    )
    assert payload["relationships"][0]["published"] is False
    messages = " | ".join(i["message"] for i in payload["issues"])
    assert "Invalid YAML" in messages
    assert "Version '0.1' is not supported" in messages
    assert "exactly one column" in messages
    assert "description differs" in messages
    with pytest.raises(ValueError, match="not found"):
        sx._semantic_view_payload("ws", "lh", "missing")


def _saved_view_lake():
    return _FakeOneLake(
        files={
            "Context/Semantic Views/sales/schema.yaml": "# Shared schema\n"
            + _VIEW_SCHEMA,
            "Context/Semantic Views/sales/metrics/metrics.yaml": _VIEW_METRICS,
        }
    )


def test_save_semantic_view_writes_staged_edits_to_the_yaml_files(monkeypatch):
    import yaml

    lake = _saved_view_lake()
    monkeypatch.setattr(sx, "_onelake_request", lake)
    payload = sx._semantic_view_payload("ws", "lh", "sales")
    etags = {f["path"]: f["etag"] for f in payload["files"]}
    customers = next(d for d in payload["datasets"] if d["name"] == "customers")
    changes = {
        "description": "Sales, refreshed daily.",
        "updates": [
            {
                "kind": "datasets",
                "file": "schema.yaml",
                "index": 0,
                "name": "sales",
                "values": {"name": "orders", "description": ""},
            },
            {
                "kind": "relationships",
                "file": "schema.yaml",
                "index": 0,
                "name": "sales_to_customers",
                "values": {"from": "orders", "to_column": "id"},
            },
            {
                "kind": "metrics",
                "file": "metrics/metrics.yaml",
                "index": 1,
                "name": "SQL only",
                "values": {"dax": "COUNTROWS(\n    'orders'\n)"},
            },
        ],
        "deletes": [
            {
                "kind": "datasets",
                "file": customers["file"],
                "index": customers["index"],
                "name": "customers",
            }
        ],
        "creates": [
            {
                "kind": "metrics",
                "file": "",
                "values": {"name": "Orders", "description": "", "dax": "[SQL only]"},
            }
        ],
    }

    count, comments_kept = sx._save_semantic_view("ws", "lh", "sales", changes, etags)

    assert count == 6
    # Comments ('# Shared schema') are kept only with ruamel.yaml.
    assert comments_kept is sx._ViewYaml().keeps_comments

    def read(path):
        return yaml.safe_load(lake.files[f"Context/Semantic Views/sales/{path}"])

    schema, metrics = read("schema.yaml"), read("metrics/metrics.yaml")
    assert schema["description"] == "Sales, refreshed daily."
    assert schema["datasets"] == [
        {"name": "orders", "source": "dbo.sales", "primary_key": ["sale_id"]}
    ]
    rel = schema["relationships"][0]
    assert (rel["from"], rel["to"], rel["from_columns"], rel["to_columns"]) == (
        "orders",
        "customers",
        ["customer_id"],
        ["id"],
    )
    dialects = metrics["metrics"][1]["expression"]["dialects"]
    assert dialects[0] == {
        "dialect": "DAX",
        "expression": "COUNTROWS(\n    'orders'\n)",
    }
    assert dialects[1]["dialect"] == "SNOWFLAKE"
    assert metrics["metrics"][2] == {
        "name": "Orders",
        "expression": {"dialects": [{"dialect": "DAX", "expression": "[SQL only]"}]},
    }
    assert "description" not in metrics  # Only files defining it are changed.
    written = lake.files["Context/Semantic Views/sales/metrics/metrics.yaml"].decode()
    assert "expression: |" in written  # Multi-line DAX is a literal block.


def test_save_semantic_view_moves_objects_between_yaml_files(monkeypatch):
    import yaml

    lake = _saved_view_lake()
    monkeypatch.setattr(sx, "_onelake_request", lake)

    def at(kind, file, index, name):
        return {"kind": kind, "file": file, "index": index, "name": name}

    rel = at("relationships", "schema.yaml", 0, "sales_to_customers")
    changes = {
        # Renamed and moved: the move still finds it by its saved name.
        "updates": [dict(rel, values={"name": "orders_to_customers"})],
        "moves": [
            dict(rel, to="metrics/metrics.yaml"),
            dict(
                at("datasets", "schema.yaml", 1, "customers"), to="metrics/metrics.yaml"
            ),
            dict(
                at("metrics", "metrics/metrics.yaml", 1, "SQL only"), to="schema.yaml"
            ),
        ],
    }

    count, _ = sx._save_semantic_view("ws", "lh", "sales", changes)

    assert count == 4

    def read(path):
        return yaml.safe_load(lake.files[f"Context/Semantic Views/sales/{path}"])

    schema, metrics = read("schema.yaml"), read("metrics/metrics.yaml")
    assert [d["name"] for d in schema["datasets"]] == ["sales"]
    # A list emptied by a move is removed ('relationships: []' is invalid).
    assert "relationships" not in schema
    assert schema["metrics"] == [
        {
            "name": "SQL only",
            "expression": {
                "dialects": [{"dialect": "SNOWFLAKE", "expression": "COUNT(*)"}]
            },
        }
    ]
    assert [m["name"] for m in metrics["metrics"]] == ["Total Net Sales"]
    assert metrics["datasets"] == [{"name": "customers", "source": "dbo.customers"}]
    assert metrics["relationships"][0]["name"] == "orders_to_customers"
    assert metrics["relationships"][0]["from_columns"] == ["customer_id"]

    with pytest.raises(ValueError, match="no longer in the semantic view"):
        sx._save_semantic_view(
            "ws", "lh", "sales", {"moves": [dict(rel, to="gone.yaml")]}
        )


def test_adding_a_yaml_file_makes_a_single_file_view_a_combined_view(monkeypatch):
    import yaml

    lake = _FakeOneLake(
        files={
            "Context/Semantic Views/sales/sales_semantic_view.yaml": _VIEW_SCHEMA,
            "Context/Semantic Views/other/other.yaml": _VIEW_METRICS,
        }
    )
    widget, send = _view_explorer_widget(monkeypatch)
    monkeypatch.setattr(sx, "_onelake_request", lake)

    result = send(action="add_view_file", name="finance", request_id=1)
    assert not result["ok"] and "name for the combined view" in result["error"]
    result = send(
        action="add_view_file", name="finance", combined_name="Other", request_id=2
    )
    assert not result["ok"] and "already exists" in result["error"]

    result = send(
        action="add_view_file", name="finance", combined_name="all", request_id=3
    )

    assert result["ok"] and result["data"] == {
        "view": "all",
        "file": "finance/finance_semantic_view.yaml",
    }
    folder = "Context/Semantic Views/all"
    assert sorted(p for p in lake.files if p.startswith("Context/Semantic Views/")) == [
        f"{folder}/finance/finance_semantic_view.yaml",
        f"{folder}/sales_semantic_view.yaml",
        "Context/Semantic Views/other/other.yaml",
    ]
    new = yaml.safe_load(lake.files[f"{folder}/finance/finance_semantic_view.yaml"])
    assert (new["name"], new["version"]) == ("finance", sx._VIEW_YAML_VERSION)
    # The renamed view is the one edited from now on.
    result = send(action="add_view_file", name="hr", request_id=4)
    assert result["ok"] and result["data"]["view"] == "all"
    assert f"{folder}/hr/hr_semantic_view.yaml" in lake.files


def test_adding_a_yaml_file_to_a_combined_view_keeps_its_name(monkeypatch):
    lake = _saved_view_lake()
    widget, send = _view_explorer_widget(monkeypatch)
    monkeypatch.setattr(sx, "_onelake_request", lake)

    # Both files already define a semantic view named 'sales'.
    result = send(action="add_view_file", name="Sales", request_id=1)
    assert not result["ok"] and "already defines" in result["error"]

    result = send(
        action="add_view_file", name="finance", combined_name="ignored", request_id=2
    )

    path = "Context/Semantic Views/sales/finance/finance_semantic_view.yaml"
    assert result["ok"] and result["data"]["view"] == "sales"
    assert path in lake.files
    assert not any("ignored" in p for p in lake.files)
    result = send(action="add_view_file", name="finance", request_id=3)
    assert not result["ok"] and "already has" in result["error"]
    # Once the file is deleted (its folder is left behind), it can be added again.
    del lake.files[path]
    result = send(action="add_view_file", name="finance", request_id=4)
    assert result["ok"] and path in lake.files


def test_view_object_names_must_be_unique_across_files():
    second = """
version: 0.2.0.dev0
name: finance
datasets:
  - name: Sales
    source: dbo.sales2
metrics:
  - name: total net sales
    expression:
      dialects:
        - dialect: DAX
          expression: "1"
"""
    import yaml

    merged = sx._merge_view_documents(
        [
            ("schema.yaml", yaml.safe_load(_VIEW_SCHEMA)),
            ("metrics.yaml", yaml.safe_load(_VIEW_METRICS)),
            ("finance/finance.yaml", yaml.safe_load(second)),
        ]
    )

    dupes = [i for i in merged["issues"] if "must be unique" in i["message"]]
    assert [(i["level"], i["message"], i["file"]) for i in dupes] == [
        (
            "error",
            "The dataset name 'sales' is used 2 times (finance/finance.yaml, "
            "schema.yaml); dataset names must be unique.",
            "finance/finance.yaml",
        ),
        (
            "error",
            "The metric name 'Total Net Sales' is used 2 times "
            "(finance/finance.yaml, metrics.yaml); metric names must be unique.",
            "finance/finance.yaml",
        ),
    ]


def test_save_semantic_view_refuses_to_overwrite_changed_files(monkeypatch):
    lake = _saved_view_lake()
    monkeypatch.setattr(sx, "_onelake_request", lake)
    update = {
        "kind": "datasets",
        "file": "schema.yaml",
        "index": 0,
        "name": "sales",
        "values": {"description": "x"},
    }

    with pytest.raises(ValueError, match="changed after the view was loaded"):
        sx._save_semantic_view(
            "ws", "lh", "sales", {"updates": [update]}, {"schema.yaml": '"stale"'}
        )
    with pytest.raises(ValueError, match="has changed"):
        sx._save_semantic_view(
            "ws", "lh", "sales", {"updates": [dict(update, name="other")]}
        )
    assert (
        b"description: x" not in lake.files["Context/Semantic Views/sales/schema.yaml"]
    )


def test_crlf_view_files_are_read_with_lf_line_endings(monkeypatch):
    lake = _FakeOneLake(
        files={
            "Context/Semantic Views/v/v.yaml": "version: 0.2.0.dev0\r\nname: v\r\n",
            "SystemContext/Semantic Views/v/v.tmdl": _VIEW_TMDL.replace("\n", "\r\n"),
        }
    )
    monkeypatch.setattr(sx, "_onelake_request", lake)

    payload = sx._semantic_view_payload("ws", "lh", "v")

    tmdl = next(f for f in payload["files"] if f["language"] == "tmdl")
    assert "\r" not in tmdl["content"] and tmdl["content"].startswith("model Model\n")
    assert {d["name"] for d in payload["datasets"]} == {"sales", "customers", "date"}


def test_single_file_tmdl_marks_published_objects(monkeypatch):
    # A single .tmdl file nests everything in the model (as Fabric writes it).
    body = _VIEW_TMDL.split("\n", 2)[2]  # drop 'model Model' and its culture
    nested = "createOrReplace\n\tdatabase sales\n\t\tmodel Model\n" + "\n".join(
        "\t\t\t" + line if line else "" for line in body.split("\n")
    )
    lake = _FakeOneLake(
        files={
            "Context/Semantic Views/sales/schema.yaml": _VIEW_SCHEMA
            + "  - name: renamed_rel\n    from: customers\n    to: sales\n"
            "    from_columns: [customer_id]\n    to_columns: [net_amount]\n",
            "Context/Semantic Views/sales/metrics.yaml": _VIEW_METRICS,
            "SystemContext/Semantic Views/sales/sales.tmdl": nested,
        }
    )
    monkeypatch.setattr(sx, "_onelake_request", lake)
    monkeypatch.setattr(sx, "_lakehouse_table_columns", lambda *a: [])

    payload = sx._semantic_view_payload("ws", "lh", "sales")

    datasets = {d["name"]: d for d in payload["datasets"]}
    assert (
        datasets["sales"]["published"]
        and datasets["sales"]["columns_from"] == "published"
    )
    rels = {r["name"]: r["published"] for r in payload["relationships"]}
    assert rels == {"sales_to_customers": True, "renamed_rel": False}
    metrics = {m["name"]: m["published"] for m in payload["metrics"]}
    assert metrics == {"Total Net Sales": True, "SQL only": False}


def test_publish_semantic_view_calls_the_publish_api(monkeypatch):
    from sempy_labs.semantic_model import _semantic_view as sv

    calls = []

    class _Resp:
        def json(self):
            return {
                "warnings": [
                    {
                        "code": "W1",
                        "message": "Omitted",
                        "sourcePath": "m.yaml",
                        "line": 3,
                    }
                ]
            }

    monkeypatch.setattr(sv, "resolve_workspace_name_and_id", lambda w: ("WS", "ws-id"))
    monkeypatch.setattr(
        sv,
        "resolve_lakehouse_name_and_id",
        lambda lakehouse, workspace: ("LH", "lh-id"),
    )
    monkeypatch.setattr(sv, "_base_api", lambda **kw: calls.append(kw) or _Resp())

    assert sx._publish_view("ws-id", "lh-id", "sales view") == ["Omitted (m.yaml:3)"]
    assert calls[0]["request"] == (
        "modeling/workspaces/ws-id/items/lh-id/semanticViews/sales%20view/publish"
    )
    assert (calls[0]["method"], calls[0]["client"]) == ("post", "internal")


def _explorer_without_a_model(monkeypatch, workspaces, error=""):
    import IPython.display
    import sempy_labs._helper_functions as hf
    import sempy_labs._ui_components as ui

    shown = []
    # What the browser receives as initial state, before any comm round trip.
    monkeypatch.setattr(
        IPython.display,
        "display",
        lambda w: shown.append((w, dict(w.get_state()))),
    )
    monkeypatch.setattr(
        hf, "resolve_workspace_name_and_id", lambda w: ("Current", "ws-cur")
    )
    monkeypatch.setattr(
        ui, "_list_picker_workspaces_checked", lambda *a: (workspaces, error)
    )
    monkeypatch.setattr(
        ui, "list_picker_datasets", lambda ws, *a: [{"id": "d1", "name": f"{ws} m"}]
    )
    sx.semantic_explorer()
    return shown[0]


def test_explorer_seeds_all_workspaces_before_display(monkeypatch):
    workspaces = [
        {"id": "ws-a", "name": "Alpha"},
        {"id": "ws-cur", "name": "Current"},
    ]
    _, state = _explorer_without_a_model(monkeypatch, workspaces)

    assert state["available_workspaces"] == workspaces
    assert state["workspaces_loaded"] is True
    assert state["available_datasets"] == [{"id": "d1", "name": "ws-cur m"}]
    assert state["status"] == {}


def test_explorer_reports_why_the_workspaces_could_not_be_listed(monkeypatch):
    current = [{"id": "ws-cur", "name": "Current"}]
    _, state = _explorer_without_a_model(monkeypatch, current, "Fabric REST API: 401")

    assert state["available_workspaces"] == current
    # Not loaded, so the picker asks again when it is opened.
    assert state["workspaces_loaded"] is False
    assert state["status"]["kind"] == "error"
    assert "Fabric REST API: 401" in state["status"]["message"]


def _view_explorer_widget(monkeypatch):
    import IPython.display
    import sempy_labs._helper_functions as hf
    import sempy_labs._ui_components as ui

    shown = []
    monkeypatch.setattr(IPython.display, "display", shown.append)
    monkeypatch.setattr(hf, "resolve_workspace_name_and_id", lambda w: ("WS", "ws-id"))
    monkeypatch.setattr(
        hf, "resolve_lakehouse_name_and_id", lambda *a, **k: ("LH", "lh-id")
    )
    monkeypatch.setattr(ui, "run_widget_task", lambda fn, args: fn(*args))
    monkeypatch.setattr(ui, "_list_picker_workspaces_checked", lambda *a: ([], ""))
    monkeypatch.setattr(ui, "list_picker_datasets", lambda *a: [])
    monkeypatch.setattr(sx, "_semantic_view_payload", lambda *a: {"view": {}})
    sx.semantic_explorer()
    widget = shown[0]

    def send(**data):
        widget.pending_action = data
        widget.run += 1
        return widget.result

    send(action="open_view", workspace_id="ws-id", lakehouse_id="lh-id", view="sales")
    return widget, send


def _step_states(status):
    return {s["id"]: s["state"] for s in status.get("steps") or []}


def test_format_dax_formats_all_expressions_in_one_call(monkeypatch):
    import sempy_labs._daxformatter as daxformatter

    calls = []

    def _format(expressions, metadata=None):
        calls.append((list(expressions), metadata))
        return [e.upper() for e in expressions][: 1 if len(calls) > 1 else None]

    monkeypatch.setattr(daxformatter, "_format_dax", _format)
    widget, send = _view_explorer_widget(monkeypatch)

    result = send(
        action="format_dax",
        expressions=["sum(a)", "max(b)"],
        names=["A", "B"],
        request_id=1,
    )

    assert result["ok"] and result["data"]["formatted"] == ["SUM(A)", "MAX(B)"]
    assert calls == [
        (
            ["sum(a)", "max(b)"],
            [{"name": "A", "type": "measures"}, {"name": "B", "type": "measures"}],
        )
    ]
    # A partial result is an error rather than misaligned expressions.
    result = send(action="format_dax", expressions=["x", "y"], names=[], request_id=2)
    assert not result["ok"] and "incomplete" in result["error"]


def test_publishing_a_view_refreshes_its_semantic_model(monkeypatch):
    from sempy_labs import _refresh_semantic_model as rsm

    calls, seen = [], []
    monkeypatch.setattr(
        sx, "_publish_view", lambda *a: calls.append(("publish", a)) or []
    )

    def _refresh(**kw):
        calls.append(("refresh", kw))
        seen.append(_step_states(widget.status))

    monkeypatch.setattr(rsm, "refresh_semantic_model", _refresh)
    widget, send = _view_explorer_widget(monkeypatch)

    result = send(action="save_view", changes={}, publish=True, request_id=1)

    assert result["ok"] and result["data"]["published"]
    assert calls == [
        ("publish", ("ws-id", "lh-id", "sales")),
        ("refresh", {"dataset": "sales", "workspace": "ws-id"}),
    ]
    # The status bar shows the progress from one step to the next.
    assert seen == [{"publish": "done", "refresh": "active"}]
    assert widget.status["kind"] == "success"
    assert _step_states(widget.status) == {"publish": "done", "refresh": "done"}


def test_view_model_refresh_failure_is_reported_and_skipped_if_not_published(
    monkeypatch,
):
    refreshes = []

    def _fail(*a):
        refreshes.append(a)
        raise RuntimeError("boom")

    monkeypatch.setattr(sx, "_refresh_view_model", _fail)
    monkeypatch.setattr(sx, "_publish_view", lambda *a: [])
    widget, send = _view_explorer_widget(monkeypatch)
    send(action="save_view", changes={}, publish=True, request_id=1)
    assert widget.status["kind"] == "error"
    assert (
        "Refreshing the 'sales' semantic model failed: boom" in widget.status["message"]
    )

    def _publish_fails(*a):
        raise RuntimeError("bad yaml")

    monkeypatch.setattr(sx, "_publish_view", _publish_fails)
    refreshes.clear()
    send(action="save_view", changes={}, publish=True, request_id=2)
    assert refreshes == []
    assert "Publishing failed: bad yaml" in widget.status["message"]
    assert _step_states(widget.status) == {"publish": "error", "refresh": "skipped"}

    def _save_fails(*a):
        raise RuntimeError("etag mismatch")

    monkeypatch.setattr(sx, "_save_semantic_view", _save_fails)
    result = send(
        action="save_view", changes={"description": "x"}, publish=True, request_id=3
    )
    assert not result["ok"]
    assert _step_states(widget.status) == {
        "save": "error",
        "publish": "skipped",
        "refresh": "skipped",
    }
    assert "Saving failed: etag mismatch" in widget.status["message"]


def test_saved_changes_which_are_not_published_are_reported(monkeypatch):
    metrics = _VIEW_METRICS.replace(
        "\"SUM('sales'[net_amount])\"", "\"SUM('sales'[net_amount]) * 2\""
    )
    schema = _VIEW_SCHEMA.replace("One row per sales line.", "One row per line.")
    # The relationship now goes the other way round.
    schema = schema.replace(
        "    from: sales\n    to: customers", "    from: customers\n    to: sales"
    )
    lake = _FakeOneLake(
        files={
            "Context/Semantic Views/sales/schema.yaml": schema,
            "Context/Semantic Views/sales/metrics.yaml": metrics,
            "SystemContext/Semantic Views/sales/sales.tmdl": _VIEW_TMDL.replace(
                "\t\tformatString: #,0.00\n",
                "\t\tformatString: #,0.00\n\n\tmeasure Removed = 1\n",
            ),
        }
    )
    monkeypatch.setattr(sx, "_onelake_request", lake)

    payload = sx._semantic_view_payload("ws", "lh", "sales")

    def status(items, name):
        o = next(i for i in items if i["name"] == name)
        return o["published"], o["publish_state"], o["publish_diff"]

    assert status(payload["metrics"], "Total Net Sales") == (
        True,
        "changed",
        ["expression"],
    )
    assert status(payload["metrics"], "SQL only") == (False, "missing", [])
    assert status(payload["datasets"], "sales") == (True, "changed", ["description"])
    assert status(payload["datasets"], "customers") == (True, "published", [])
    assert status(payload["relationships"], "sales_to_customers") == (
        True,
        "changed",
        ["from / to"],
    )
    # The view description isn't in the published model either.
    assert payload["view"]["publish_diff"] == ["description"]
    assert payload["view"]["unpublished_count"] == 5
    assert any("Metric 'Removed'" in i["message"] for i in payload["issues"])


def test_dax_layout_differences_are_not_unpublished_changes():
    assert sx._same_dax("SUM('sales'[net_amount])", "SUM(\n\t'sales'[net_amount]\n)")
    assert sx._same_dax("[A] + [B]", "[A]+[B]")
    assert not sx._same_dax("SUM('sales'[net_amount])", "SUM('sales'[amount])")
