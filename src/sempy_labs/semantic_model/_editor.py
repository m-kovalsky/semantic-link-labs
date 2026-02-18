"""Interactive Semantic Model Editor for Fabric notebooks.
"""

import ipywidgets as widgets
from IPython.display import display
import sempy
from uuid import UUID
from typing import Optional
from sempy._utils._log import log


# ── Tree icons per object type ─────────────────────────────────────────
# 📂
_ICONS = {
    "model": "📦",
    "tables_header": "▦",
    "table": "▦",
    "calc_group": "🧮",
    "column": "▮",
    "calc_column": "▮∑",
    "measure": "∑",
    "partition": "📄",
    "hierarchy": "≡",
    "level": "∷",
    "relationships_header": "→",
    "relationship": "→",
    "roles_header": "🛡️",
    "role": "🛡️",
    "expressions_header": "ƒ",
    "expression": "ƒ",
    "perspectives_header": "⌬",
    "perspective": "⌬",
    "cultures_header": "🌐",
    "culture": "🌐",
    "calculation_item": "🔢",
}


# ── Property configuration per object type ─────────────────────────────
# Each entry: {"name", "type", optional "readonly", optional "enum_type", optional "options"}

_PROPERTY_CONFIG = {
    "model": [
        {"name": "Name", "type": "text", "readonly": True},
        {"name": "Description", "type": "textarea"},
        {
            "category": "Database",
            "props": [
                {
                    "name": "CompatibilityLevel",
                    "type": "int",
                    "readonly": True,
                    "accessor": "Database.CompatibilityLevel",
                },
                {
                    "name": "CompatibilityMode",
                    "type": "text",
                    "readonly": True,
                    "accessor": "Database.CompatibilityMode",
                },
                {
                    "name": "ID",
                    "type": "text",
                    "readonly": True,
                    "accessor": "Database.ID",
                },
                {
                    "name": "Name",
                    "type": "text",
                    "readonly": True,
                    "accessor": "Database.Name",
                },
                {
                    "name": "Visible",
                    "type": "bool",
                    "readonly": True,
                    "accessor": "Database.Visible",
                },
            ],
        },
    ],
    "table": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
        {"name": "IsHidden", "type": "bool"},
        {
            "category": "Options",
            "props": [
                {"name": "AlternateSourcePrecedence", "type": "int"},
                {"name": "DataCategory", "type": "text"},
                {
                    "name": "DefaultDetailRowsExpression",
                    "type": "textarea",
                    "accessor": "DefaultDetailRowsDefinition.Expression",
                },
                {"name": "SourceLineageTag", "type": "text"},
                {"name": "SystemManaged", "type": "bool", "readonly": True},
                {"name": "LineageTag", "type": "text"},
                {"name": "ExcludeFromAutomaticAggregations", "type": "bool"},
                {"name": "ExcludeFromModelRefresh", "type": "bool"},
            ],
        },
        {
            "category": "Refresh Policy",
            "props": [
                {"name": "EnableRefreshPolicy", "type": "bool"},
                {
                    "name": "Mode",
                    "type": "text",
                    "readonly": True,
                    "accessor": "RefreshPolicy.Mode",
                },
                {
                    "name": "PolicyType",
                    "type": "text",
                    "readonly": True,
                    "accessor": "RefreshPolicy.PolicyType",
                },
            ],
        },
    ],
    "column": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
        {"name": "IsHidden", "type": "bool"},
        {
            "name": "DataType",
            "type": "enum",
            "enum_type": "DataType",
            "options": [
                "String",
                "Int64",
                "Double",
                "DateTime",
                "Decimal",
                "Boolean",
                "Binary",
                "Unknown",
                "Variant",
            ],
        },
        {"name": "FormatString", "type": "text"},
        {"name": "DisplayFolder", "type": "text"},
        {"name": "DataCategory", "type": "text"},
        {"name": "IsKey", "type": "bool"},
        {
            "name": "SummarizeBy",
            "type": "enum",
            "enum_type": "AggregateFunction",
            "options": [
                "Default",
                "None",
                "Sum",
                "Min",
                "Max",
                "Count",
                "Average",
                "DistinctCount",
            ],
        },
        {"name": "IsNullable", "type": "bool"},
        {"name": "IsAvailableInMDX", "type": "bool"},
        {
            "name": "EncodingHint",
            "type": "enum",
            "enum_type": "EncodingHintType",
            "options": ["Default", "Hash", "Value"],
        },
        {"name": "SourceColumn", "type": "text"},
    ],
    "measure": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
        {"name": "IsHidden", "type": "bool"},
        {"name": "FormatString", "type": "text"},
        {"name": "DisplayFolder", "type": "text"},
        {"name": "DataCategory", "type": "text"},
    ],
    "partition": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
        {
            "name": "Mode",
            "type": "enum",
            "enum_type": "ModeType",
            "options": ["Import", "DirectQuery", "Dual", "Default", "Push"],
        },
    ],
    "hierarchy": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
        {"name": "IsHidden", "type": "bool"},
    ],
    "level": [
        {"name": "Name", "type": "text"},
        {"name": "Ordinal", "type": "int"},
    ],
    "relationship": [
        {"name": "Name", "type": "text", "readonly": True},
        {"name": "IsActive", "type": "bool"},
        {
            "name": "CrossFilteringBehavior",
            "type": "enum",
            "enum_type": "CrossFilteringBehavior",
            "options": ["OneDirection", "BothDirections", "Automatic"],
        },
        {
            "name": "SecurityFilteringBehavior",
            "type": "enum",
            "enum_type": "SecurityFilteringBehavior",
            "options": ["OneDirection", "BothDirections"],
        },
        {"name": "RelyOnReferentialIntegrity", "type": "bool"},
    ],
    "role": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
        {
            "name": "ModelPermission",
            "type": "enum",
            "enum_type": "ModelPermission",
            "options": ["Read", "ReadRefresh", "Administrator", "None"],
        },
    ],
    "expression": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
    ],
    "calculation_item": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
        {"name": "Ordinal", "type": "int"},
    ],
    "perspective": [
        {"name": "Name", "type": "text"},
        {"name": "Description", "type": "textarea"},
    ],
    "culture": [
        {"name": "Name", "type": "text", "readonly": True},
    ],
}


class _SemanticModelEditor:
    """
    Interactive semantic model editor using ipywidgets.

    Provides a tree browser for model objects (tables, columns, measures,
    relationships, roles, etc.), an expression editor, and a property grid
    — similar to Tabular Editor 2.
    """

    def __init__(self, tom, readonly):
        self._tom = tom
        self._readonly = readonly
        self._object_map = {}
        self._type_map = {}
        self._expanded = {"tables"}
        self._current_id = None
        self._has_unsaved = False
        self._closed = False
        self._table_order = []

        self._build_object_map()
        self._create_widgets()
        self._refresh_tree()

    def __repr__(self):
        return ""

    def _repr_mimebundle_(self, **kwargs):
        return {"text/plain": ""}

    # ── Object Map ─────────────────────────────────────────────────────

    def _build_object_map(self):
        """Build flat maps of id -> TOM object and id -> type string."""

        model = self._tom.model
        self._object_map = {}
        self._type_map = {}
        self._table_order = []

        self._object_map["model"] = model
        self._type_map["model"] = "model"

        for table in sorted(model.Tables, key=lambda t: t.Name):
            t_id = f"table::{table.Name}"
            self._object_map[t_id] = table
            is_cg = table.CalculationGroup is not None
            self._type_map[t_id] = "calc_group" if is_cg else "table"
            self._table_order.append(t_id)

            for m in table.Measures:
                mid = f"measure::{table.Name}::{m.Name}"
                self._object_map[mid] = m
                self._type_map[mid] = "measure"

            for col in table.Columns:
                if str(col.Type) == "RowNumber":
                    continue
                cid = f"column::{table.Name}::{col.Name}"
                self._object_map[cid] = col
                self._type_map[cid] = (
                    "calc_column" if str(col.Type) == "Calculated" else "column"
                )

            for p in table.Partitions:
                pid = f"partition::{table.Name}::{p.Name}"
                self._object_map[pid] = p
                self._type_map[pid] = "partition"

            for h in table.Hierarchies:
                hid = f"hierarchy::{table.Name}::{h.Name}"
                self._object_map[hid] = h
                self._type_map[hid] = "hierarchy"
                for lv in h.Levels:
                    lvid = f"level::{table.Name}::{h.Name}::{lv.Name}"
                    self._object_map[lvid] = lv
                    self._type_map[lvid] = "level"

            if is_cg:
                for ci in table.CalculationGroup.CalculationItems:
                    ciid = f"calcitem::{table.Name}::{ci.Name}"
                    self._object_map[ciid] = ci
                    self._type_map[ciid] = "calculation_item"

        for rel in model.Relationships:
            rid = f"relationship::{rel.Name}"
            self._object_map[rid] = rel
            self._type_map[rid] = "relationship"

        for role in model.Roles:
            self._object_map[f"role::{role.Name}"] = role
            self._type_map[f"role::{role.Name}"] = "role"

        for expr in model.Expressions:
            self._object_map[f"expression::{expr.Name}"] = expr
            self._type_map[f"expression::{expr.Name}"] = "expression"

        for persp in model.Perspectives:
            self._object_map[f"perspective::{persp.Name}"] = persp
            self._type_map[f"perspective::{persp.Name}"] = "perspective"

        for culture in model.Cultures:
            self._object_map[f"culture::{culture.Name}"] = culture
            self._type_map[f"culture::{culture.Name}"] = "culture"

    # ── Tree Building ──────────────────────────────────────────────────

    def _get_visible_tree_items(self):
        """Build flat list of (label, value) tuples for the tree Select widget."""

        items = []
        model = self._tom.model
        search = (
            self._search_box.value.strip().lower()
            if hasattr(self, "_search_box")
            else ""
        )

        def matches(name):
            return not search or search in name.lower()

        def add(label, obj_id, indent=0):
            items.append(("  " * indent + label, obj_id))

        # ── Model ──
        add(f"{_ICONS['model']} {model.Name}", "model")

        # ── Tables ──
        t_exp = "tables" in self._expanded
        chev = "▼" if t_exp else "▶"
        add(
            f"{chev} {_ICONS['tables_header']} Tables ({model.Tables.Count})",
            "__toggle__tables",
            1,
        )

        if t_exp:
            for t_id in self._table_order:
                table = self._object_map[t_id]
                ttype = self._type_map[t_id]
                icon = _ICONS.get(ttype, _ICONS["table"])
                expanded = t_id in self._expanded
                children = self._table_children(table)

                if search:
                    matching = [c for c in children if matches(c[1])]
                    if not matches(table.Name) and not matching:
                        continue

                chev = "▼" if expanded else "▶"
                hidden = " (hidden)" if table.IsHidden else ""
                add(f"{chev} {icon} {table.Name}{hidden}", t_id, 2)

                if expanded:
                    for child_label, child_name, child_id, child_type in children:
                        if search and not matches(child_name):
                            continue

                        # Partitions folder: collapsible
                        if child_type == "partitions_folder":
                            p_exp = child_id in self._expanded
                            chev = "\u25bc" if p_exp else "\u25b6"
                            add(
                                f"{chev} {child_label}",
                                f"__toggle__{child_id}",
                                3,
                            )
                            if p_exp:
                                for p in sorted(
                                    table.Partitions, key=lambda x: x.Name
                                ):
                                    pid = (
                                        f"partition::{table.Name}"
                                        f"::{p.Name}"
                                    )
                                    if not search or matches(p.Name):
                                        add(
                                            f"{_ICONS['partition']} {p.Name}",
                                            pid,
                                            4,
                                        )
                            continue

                        add(child_label, child_id, 3)

                        # Levels under hierarchies
                        if child_type == "hierarchy":
                            h_obj = self._object_map.get(child_id)
                            if h_obj:
                                for lv in h_obj.Levels:
                                    lvid = (
                                        f"level::{table.Name}"
                                        f"::{h_obj.Name}::{lv.Name}"
                                    )
                                    if not search or matches(lv.Name):
                                        add(
                                            f"  {_ICONS['level']} {lv.Name}",
                                            lvid,
                                            4,
                                        )

        # ── Relationships ──
        rc = model.Relationships.Count
        if rc > 0:
            r_exp = "relationships" in self._expanded
            chev = "▼" if r_exp else "▶"
            add(
                f"{chev} {_ICONS['relationships_header']} Relationships ({rc})",
                "__toggle__relationships",
                1,
            )
            if r_exp:
                for rel in model.Relationships:
                    rid = f"relationship::{rel.Name}"
                    try:
                        label = (
                            f"{rel.FromTable.Name}[{rel.FromColumn.Name}]"
                            f" → {rel.ToTable.Name}[{rel.ToColumn.Name}]"
                        )
                    except Exception:
                        label = rel.Name
                    if search and not matches(label):
                        continue
                    add(f"{_ICONS['relationship']} {label}", rid, 2)

        # ── Roles ──
        rlc = model.Roles.Count
        if rlc > 0:
            rl_exp = "roles" in self._expanded
            chev = "▼" if rl_exp else "▶"
            add(
                f"{chev} {_ICONS['roles_header']} Roles ({rlc})",
                "__toggle__roles",
                1,
            )
            if rl_exp:
                for role in model.Roles:
                    if search and not matches(role.Name):
                        continue
                    add(
                        f"{_ICONS['role']} {role.Name}",
                        f"role::{role.Name}",
                        2,
                    )

        # ── Expressions ──
        ec = model.Expressions.Count
        if ec > 0:
            e_exp = "expressions" in self._expanded
            chev = "▼" if e_exp else "▶"
            add(
                f"{chev} {_ICONS['expressions_header']} Expressions ({ec})",
                "__toggle__expressions",
                1,
            )
            if e_exp:
                for expr in model.Expressions:
                    if search and not matches(expr.Name):
                        continue
                    add(
                        f"{_ICONS['expression']} {expr.Name}",
                        f"expression::{expr.Name}",
                        2,
                    )

        # ── Perspectives ──
        pc = model.Perspectives.Count
        if pc > 0:
            p_exp = "perspectives" in self._expanded
            chev = "▼" if p_exp else "▶"
            add(
                f"{chev} {_ICONS['perspectives_header']} Perspectives ({pc})",
                "__toggle__perspectives",
                1,
            )
            if p_exp:
                for persp in model.Perspectives:
                    if search and not matches(persp.Name):
                        continue
                    add(
                        f"{_ICONS['perspective']} {persp.Name}",
                        f"perspective::{persp.Name}",
                        2,
                    )

        # ── Cultures ──
        cc = model.Cultures.Count
        if cc > 0:
            c_exp = "cultures" in self._expanded
            chev = "▼" if c_exp else "▶"
            add(
                f"{chev} {_ICONS['cultures_header']} Cultures ({cc})",
                "__toggle__cultures",
                1,
            )
            if c_exp:
                for culture in model.Cultures:
                    if search and not matches(culture.Name):
                        continue
                    add(
                        f"{_ICONS['culture']} {culture.Name}",
                        f"culture::{culture.Name}",
                        2,
                    )

        return items

    def _table_children(self, table):
        """Return children of a table as (label, name, id, type) tuples."""

        children = []

        # Partitions folder (always first)
        pc = table.Partitions.Count
        if pc > 0:
            folder_id = f"__partitions__{table.Name}"
            children.append(
                (
                    f"{_ICONS['partition']} Partitions ({pc})",
                    f"Partitions ({pc})",
                    folder_id,
                    "partitions_folder",
                )
            )

        for m in sorted(table.Measures, key=lambda x: x.Name):
            mid = f"measure::{table.Name}::{m.Name}"
            hidden = " (hidden)" if m.IsHidden else ""
            children.append(
                (f"{_ICONS['measure']} {m.Name}{hidden}", m.Name, mid, "measure")
            )

        columns = sorted(
            [c for c in table.Columns if str(c.Type) != "RowNumber"],
            key=lambda x: x.Name,
        )
        for col in columns:
            cid = f"column::{table.Name}::{col.Name}"
            is_calc = str(col.Type) == "Calculated"
            icon = _ICONS["calc_column"] if is_calc else _ICONS["column"]
            ctype = "calc_column" if is_calc else "column"
            hidden = " (hidden)" if col.IsHidden else ""
            children.append((f"{icon} {col.Name}{hidden}", col.Name, cid, ctype))

        if table.CalculationGroup is not None:
            for ci in sorted(
                table.CalculationGroup.CalculationItems, key=lambda x: x.Name
            ):
                ciid = f"calcitem::{table.Name}::{ci.Name}"
                children.append(
                    (
                        f"{_ICONS['calculation_item']} {ci.Name}",
                        ci.Name,
                        ciid,
                        "calculation_item",
                    )
                )

        for h in sorted(table.Hierarchies, key=lambda x: x.Name):
            hid = f"hierarchy::{table.Name}::{h.Name}"
            hidden = " (hidden)" if h.IsHidden else ""
            children.append(
                (f"{_ICONS['hierarchy']} {h.Name}{hidden}", h.Name, hid, "hierarchy")
            )

        return children

    # ── Widget Creation ────────────────────────────────────────────────

    def _create_widgets(self):
        """Create all ipywidgets for the editor layout."""

        model_name = self._tom.model.Name
        mode = "READ-ONLY" if self._readonly else "READ / WRITE"
        mode_css = (
            "color:#92400e;background:#fef3c7;border:1px solid #f59e0b;"
            if self._readonly
            else "color:#065f46;background:#d1fae5;border:1px solid #10b981;"
        )

        self._header = widgets.HTML(
            value=(
                '<div style="display:flex;align-items:center;gap:12px;'
                "padding:10px 16px;"
                "background:linear-gradient(135deg,#1e3a5f,#2563eb);"
                'border-radius:8px 8px 0 0;">'
                '<span style="font-size:22px;">📦</span>'
                '<span style="color:#fff;font-size:16px;font-weight:700;">'
                "Semantic Model Editor</span>"
                '<span style="color:#93c5fd;font-size:14px;">'
                f"— {model_name}</span>"
                '<span style="font-size:11px;font-weight:600;'
                f'padding:2px 10px;border-radius:4px;{mode_css}">'
                f"{mode}</span>"
                "</div>"
            ),
        )

        # ── Toolbar ──
        self._save_btn = widgets.Button(
            description="💾 Save",
            button_style="primary",
            disabled=self._readonly,
            layout=widgets.Layout(width="100px", height="30px"),
        )
        self._save_btn.on_click(self._on_save)

        self._refresh_btn = widgets.Button(
            description="🔄 Refresh Tree",
            button_style="",
            layout=widgets.Layout(width="130px", height="30px"),
        )
        self._refresh_btn.on_click(self._on_refresh)

        self._close_btn = widgets.Button(
            description="✕ Close",
            button_style="danger",
            layout=widgets.Layout(width="90px", height="30px"),
        )
        self._close_btn.on_click(self._on_close)

        self._status = widgets.HTML(value=self._fmt_status("Ready"))

        self._toolbar = widgets.HBox(
            [self._save_btn, self._refresh_btn, self._close_btn, self._status],
            layout=widgets.Layout(
                padding="6px 12px",
                gap="8px",
                align_items="center",
                border_bottom="1px solid #e5e7eb",
            ),
        )

        # ── Left panel: search + tree ──
        self._search_box = widgets.Text(
            placeholder="🔍 Search objects...",
            layout=widgets.Layout(width="100%", height="30px"),
        )
        self._search_box.observe(self._on_search, names="value")

        self._tree_select = widgets.Select(
            options=[],
            rows=28,
            layout=widgets.Layout(width="100%", flex="1"),
        )
        self._tree_select.observe(self._on_tree_select, names="value")

        self._left_panel = widgets.VBox(
            [self._search_box, self._tree_select],
            layout=widgets.Layout(
                width="380px",
                min_width="300px",
                padding="8px",
                border_right="1px solid #e5e7eb",
            ),
        )

        # ── Right panel: object info + expression + properties ──
        self._obj_info = widgets.HTML(
            value=self._fmt_obj_info("Select an object from the tree")
        )

        self._expr_label = widgets.HTML(value=self._fmt_section("Expression"))
        self._expr_area = widgets.Textarea(
            placeholder="Select an object to view its expression...",
            disabled=True,
            layout=widgets.Layout(width="100%", height="180px"),
        )
        self._expr_panel = widgets.VBox(
            [self._expr_label, self._expr_area],
            layout=widgets.Layout(padding="4px 8px", display="none"),
        )

        self._props_label = widgets.HTML(value=self._fmt_section("Properties"))
        self._props_box = widgets.VBox(
            layout=widgets.Layout(padding="0 4px", overflow_y="auto"),
        )
        self._props_panel = widgets.VBox(
            [self._props_label, self._props_box],
            layout=widgets.Layout(padding="4px 8px", flex="1", overflow_y="auto"),
        )

        self._right_panel = widgets.VBox(
            [self._obj_info, self._expr_panel, self._props_panel],
            layout=widgets.Layout(flex="1", overflow_y="auto"),
        )

        # ── Main body ──
        self._body = widgets.HBox(
            [self._left_panel, self._right_panel],
            layout=widgets.Layout(height="650px"),
        )

        # ── Full UI ──
        self.ui = widgets.VBox(
            [self._header, self._toolbar, self._body],
            layout=widgets.Layout(
                border="1px solid #d1d5db",
                border_radius="8px",
                overflow="hidden",
                box_shadow="0 1px 3px rgba(0,0,0,0.1)",
            ),
        )

    # ── HTML formatting helpers ────────────────────────────────────────

    @staticmethod
    def _fmt_status(msg, color="#6b7280"):
        return (
            f'<span style="color:{color};font-size:12px;'
            f'font-family:Segoe UI,sans-serif;">{msg}</span>'
        )

    @staticmethod
    def _fmt_section(title):
        return (
            f'<div style="padding:4px 0;font-size:11px;font-weight:700;'
            f"color:#6b7280;text-transform:uppercase;"
            f'letter-spacing:0.5px;">{title}</div>'
        )

    @staticmethod
    def _fmt_obj_info(name, icon="", obj_type=""):
        badge = ""
        if obj_type:
            badge = (
                f'<span style="font-size:10px;color:#2563eb;'
                f"padding:1px 8px;border:1px solid #93c5fd;"
                f"border-radius:4px;margin-left:8px;"
                f'background:#eff6ff;">{obj_type}</span>'
            )
        return (
            f'<div style="padding:8px 12px;'
            f"border-bottom:1px solid #e5e7eb;"
            f'display:flex;align-items:center;">'
            f'<span style="font-size:18px;margin-right:8px;">'
            f"{icon}</span>"
            f'<span style="font-size:14px;font-weight:600;'
            f'color:#1f2937;">{name}</span>{badge}</div>'
        )

    # ── Tree Refresh ───────────────────────────────────────────────────

    def _refresh_tree(self):
        """Rebuild tree options, preserving the current selection."""

        items = self._get_visible_tree_items()
        self._tree_select.unobserve(self._on_tree_select, names="value")
        self._tree_select.options = items
        valid = {v for _, v in items}
        if self._current_id and self._current_id in valid:
            self._tree_select.value = self._current_id
        self._tree_select.observe(self._on_tree_select, names="value")

    # ── Event Handlers ─────────────────────────────────────────────────

    def _on_search(self, change):
        self._refresh_tree()

    def _on_tree_select(self, change):
        val = change.get("new")
        if not val:
            return

        # Toggle section headers (Tables, Relationships, Roles, etc.)
        if val.startswith("__toggle__"):
            section = val[len("__toggle__") :]
            self._expanded.symmetric_difference_update({section})
            self._refresh_tree()
            return

        # Tables / calc groups: toggle expand + show properties
        otype = self._type_map.get(val)
        if otype in ("table", "calc_group"):
            self._expanded.symmetric_difference_update({val})
            self._current_id = val
            self._load_properties(val)
            self._refresh_tree()
            return

        # Regular leaf objects: show properties
        if val in self._object_map:
            self._current_id = val
            self._load_properties(val)

    def _on_save(self, btn):
        try:
            self._tom.model.SaveChanges()
            self._has_unsaved = False
            self._status.value = self._fmt_status("✓ Saved successfully", "#059669")
        except Exception as e:
            self._status.value = self._fmt_status(f"✗ Save failed: {e}", "#dc2626")

    def _on_refresh(self, btn):
        self._build_object_map()
        self._refresh_tree()
        if self._current_id and self._current_id in self._object_map:
            self._load_properties(self._current_id)
        self._status.value = self._fmt_status("Tree refreshed")

    def _on_close(self, btn):
        if self._closed:
            return
        try:
            if self._has_unsaved and not self._readonly:
                self._status.value = self._fmt_status(
                    "⚠ Save first, or click Close again to discard",
                    "#d97706",
                )
                self._has_unsaved = False  # allow close on next click
                return
            self._tom._tom_server.Dispose()
            self._closed = True
            for w in [
                self._save_btn,
                self._close_btn,
                self._refresh_btn,
                self._tree_select,
                self._search_box,
                self._expr_area,
            ]:
                w.disabled = True
            self._status.value = self._fmt_status("Connection closed")
        except Exception as e:
            self._status.value = self._fmt_status(f"Close error: {e}", "#dc2626")

    # ── Property & Expression Loading ──────────────────────────────────

    def _load_properties(self, obj_id):
        """Load expression and property widgets for the selected object."""

        obj = self._object_map.get(obj_id)
        obj_type = self._type_map.get(obj_id, "")
        if obj is None:
            return

        # Map subtypes to their config key
        config_type = {
            "calc_column": "column",
            "calc_group": "table",
        }.get(obj_type, obj_type)

        # ── Object info header ──
        icon = _ICONS.get(obj_type, "")
        try:
            name = obj.Name
        except Exception:
            name = str(obj)

        type_label = config_type.replace("_", " ").title()
        if obj_type == "calc_column":
            type_label = "Calculated Column"
        elif obj_type == "calc_group":
            type_label = "Calculation Group"

        self._obj_info.value = self._fmt_obj_info(name, icon, type_label)

        # ── Expression ──
        expr = self._get_expression(obj, obj_type)
        self._expr_area.unobserve_all()

        if expr is not None:
            self._expr_area.value = expr
            self._expr_area.disabled = self._readonly
            self._expr_area.placeholder = "Enter DAX or M expression..."
            self._expr_panel.layout.display = ""

            if not self._readonly:
                self._expr_area.observe(
                    lambda c, oid=obj_id, ot=obj_type: self._on_expr_change(c, oid, ot),
                    names="value",
                )
        else:
            self._expr_area.value = ""
            self._expr_panel.layout.display = "none"

        # ── Properties grid ──
        config = _PROPERTY_CONFIG.get(config_type, [])
        rows = []

        for pdef in config:
            # ── Collapsible category group ──
            if "category" in pdef:
                cat_rows = self._build_category(obj, pdef["category"], pdef["props"])
                if cat_rows:
                    rows.extend(cat_rows)
                continue

            rows.append(self._build_prop_row(obj, pdef))

        # ── Dynamic Perspectives category ──
        if config_type in ("table", "column", "measure", "hierarchy"):
            persp_rows = self._build_perspectives_category(obj)
            if persp_rows:
                rows.extend(persp_rows)

        rows = [r for r in rows if r is not None]
        self._props_box.children = rows

    @staticmethod
    def _resolve_accessor(obj, accessor):
        """Walk a dotted accessor path like 'Database.Name' from obj."""
        current = obj
        for part in accessor.split("."):
            current = getattr(current, part)
        return current

    def _build_prop_row(self, obj, pdef):
        """Build a single HBox property row for a given property def."""

        pname = pdef["name"]
        ptype = pdef["type"]
        ro = pdef.get("readonly", False) or self._readonly
        accessor = pdef.get("accessor")

        try:
            cur = (
                self._resolve_accessor(obj, accessor)
                if accessor
                else getattr(obj, pname)
            )
        except Exception:
            return None

        w = self._make_prop_widget(pname, ptype, cur, pdef, ro)
        if w is None:
            return None

        if not ro and not accessor:
            self._bind_prop(w, pname, ptype, obj, pdef)

        label_html = widgets.HTML(
            value=(
                '<div style="font-size:12px;color:#374151;'
                "min-width:150px;padding:4px 0;"
                f'font-weight:500;">{pname}</div>'
            ),
            layout=widgets.Layout(min_width="150px"),
        )
        return widgets.HBox(
            [label_html, w],
            layout=widgets.Layout(
                align_items="center",
                gap="8px",
                padding="2px 0",
            ),
        )

    def _build_category(self, obj, cat_name, prop_defs):
        """Build a collapsible category with a toggle header."""

        child_rows = []
        for pdef in sorted(prop_defs, key=lambda p: p["name"]):
            row = self._build_prop_row(obj, pdef)
            if row is not None:
                child_rows.append(row)

        if not child_rows:
            return []

        content = widgets.VBox(
            child_rows,
            layout=widgets.Layout(
                padding="0 0 0 16px",
                display="none",
            ),
        )

        toggle_btn = widgets.Button(
            description=f"▶ {cat_name}",
            button_style="",
            layout=widgets.Layout(
                width="auto",
                height="26px",
                border="none",
            ),
        )
        toggle_btn.style.font_weight = "600"
        toggle_btn.style.font_size = "12px"

        def _toggle(btn, _c=content, _n=cat_name):
            if _c.layout.display == "none":
                _c.layout.display = ""
                btn.description = f"▼ {_n}"
            else:
                _c.layout.display = "none"
                btn.description = f"▶ {_n}"

        toggle_btn.on_click(_toggle)

        header = widgets.HBox(
            [toggle_btn],
            layout=widgets.Layout(
                padding="4px 0",
                border_bottom="1px solid #e5e7eb",
            ),
        )

        return [header, content]

    def _build_perspectives_category(self, obj):
        """Build a collapsible Perspectives category with membership checkboxes."""

        perspectives = self._tom.model.Perspectives
        if perspectives.Count == 0:
            return []

        total = perspectives.Count
        included = 0
        checkbox_rows = []

        for persp in sorted(perspectives, key=lambda p: p.Name):
            pname = persp.Name
            try:
                in_persp = bool(obj.InPerspective[pname])
            except Exception:
                continue
            if in_persp:
                included += 1

            cb = widgets.Checkbox(
                value=in_persp,
                indent=False,
                disabled=self._readonly,
            )

            if not self._readonly:
                def _on_toggle(change, _obj=obj, _pn=pname):
                    try:
                        _obj.InPerspective[_pn] = change["new"]
                        self._has_unsaved = True
                        self._status.value = self._fmt_status(
                            "\u25cf Unsaved changes", "#d97706"
                        )
                    except Exception as e:
                        self._status.value = self._fmt_status(
                            f"Error setting perspective: {e}", "#dc2626"
                        )

                cb.observe(_on_toggle, names="value")

            label_html = widgets.HTML(
                value=(
                    '<div style="font-size:12px;color:#374151;'
                    "min-width:150px;padding:4px 0;"
                    f'font-weight:500;">{pname}</div>'
                ),
                layout=widgets.Layout(min_width="150px"),
            )
            checkbox_rows.append(
                widgets.HBox(
                    [label_html, cb],
                    layout=widgets.Layout(
                        align_items="center",
                        gap="8px",
                        padding="2px 0",
                    ),
                )
            )

        if not checkbox_rows:
            return []

        # Summary row
        summary_label = widgets.HTML(
            value=(
                '<div style="font-size:12px;color:#374151;'
                "min-width:150px;padding:4px 0;"
                'font-weight:500;">Shown in Perspective</div>'
            ),
            layout=widgets.Layout(min_width="150px"),
        )
        summary_value = widgets.HTML(
            value=(
                '<div style="font-size:12px;color:#374151;'
                f'padding:4px 0;">Show in {included} out of {total}'
                " perspectives</div>"
            ),
        )
        summary_row = widgets.HBox(
            [summary_label, summary_value],
            layout=widgets.Layout(
                align_items="center",
                gap="8px",
                padding="2px 0",
            ),
        )

        content = widgets.VBox(
            [summary_row] + checkbox_rows,
            layout=widgets.Layout(
                padding="0 0 0 16px",
                display="none",
            ),
        )

        toggle_btn = widgets.Button(
            description="\u25b6 Perspectives",
            button_style="",
            layout=widgets.Layout(
                width="auto",
                height="26px",
                border="none",
            ),
        )
        toggle_btn.style.font_weight = "600"
        toggle_btn.style.font_size = "12px"

        def _toggle(btn, _c=content):
            if _c.layout.display == "none":
                _c.layout.display = ""
                btn.description = "\u25bc Perspectives"
            else:
                _c.layout.display = "none"
                btn.description = "\u25b6 Perspectives"

        toggle_btn.on_click(_toggle)

        header = widgets.HBox(
            [toggle_btn],
            layout=widgets.Layout(
                padding="4px 0",
                border_bottom="1px solid #e5e7eb",
            ),
        )

        return [header, content]

    def _make_prop_widget(self, name, ptype, cur, pdef, ro):
        """Create the appropriate ipywidget for a property value."""

        if ptype == "text":
            return widgets.Text(
                value=str(cur if cur is not None else ""),
                disabled=ro,
                layout=widgets.Layout(flex="1"),
            )
        elif ptype == "textarea":
            return widgets.Textarea(
                value=str(cur if cur is not None else ""),
                disabled=ro,
                layout=widgets.Layout(flex="1", height="50px"),
            )
        elif ptype == "bool":
            return widgets.Checkbox(value=bool(cur), indent=False, disabled=ro)
        elif ptype == "int":
            return widgets.IntText(
                value=int(cur if cur is not None else 0),
                disabled=ro,
                layout=widgets.Layout(width="100px"),
            )
        elif ptype == "enum":
            opts = list(pdef.get("options", []))
            cur_str = str(cur)
            if cur_str not in opts:
                opts.insert(0, cur_str)
            return widgets.Dropdown(
                options=opts,
                value=cur_str,
                disabled=ro,
                layout=widgets.Layout(width="200px"),
            )
        return None

    def _bind_prop(self, widget, pname, ptype, obj, pdef):
        """Attach a change observer that writes back to the TOM object."""

        def handler(change, _pn=pname, _pt=ptype, _o=obj, _pd=pdef):
            val = change.get("new")
            try:
                if _pt == "enum":
                    import Microsoft.AnalysisServices.Tabular as TOM
                    import System

                    enum_cls = getattr(TOM, _pd["enum_type"])
                    setattr(_o, _pn, System.Enum.Parse(enum_cls, val))
                elif _pt == "int":
                    setattr(_o, _pn, int(val))
                elif _pt == "bool":
                    setattr(_o, _pn, bool(val))
                else:
                    setattr(_o, _pn, str(val))
                self._has_unsaved = True
                self._status.value = self._fmt_status("● Unsaved changes", "#d97706")
            except Exception as e:
                self._status.value = self._fmt_status(
                    f"Error setting {_pn}: {e}", "#dc2626"
                )

        widget.observe(handler, names="value")

    # ── Expression Helpers ─────────────────────────────────────────────

    @staticmethod
    def _get_expression(obj, obj_type):
        """Get expression text for an object, or None if not applicable."""

        if obj_type in ("measure", "calculation_item"):
            return getattr(obj, "Expression", "") or ""
        elif obj_type == "calc_column":
            return getattr(obj, "Expression", "") or ""
        elif obj_type == "partition":
            src = getattr(obj, "Source", None)
            if src and hasattr(src, "Expression"):
                return src.Expression or ""
        elif obj_type == "expression":
            return getattr(obj, "Expression", "") or ""
        return None

    def _on_expr_change(self, change, obj_id, obj_type):
        """Write expression changes back to the TOM object."""

        val = change.get("new", "")
        obj = self._object_map.get(obj_id)
        if not obj:
            return
        try:
            if obj_type in (
                "measure",
                "calculation_item",
                "calc_column",
                "expression",
            ):
                obj.Expression = val
            elif obj_type == "partition" and hasattr(obj.Source, "Expression"):
                obj.Source.Expression = val
            self._has_unsaved = True
            self._status.value = self._fmt_status("● Unsaved changes", "#d97706")
        except Exception as e:
            self._status.value = self._fmt_status(f"Expression error: {e}", "#dc2626")


# ── Public API ─────────────────────────────────────────────────────────


@log
def editor(
    dataset: str | UUID,
    readonly: bool = False,
    workspace: Optional[str | UUID] = None,
) -> _SemanticModelEditor:
    """
    Opens an interactive semantic model editor in a Fabric notebook.

    Provides a Tabular Editor 2-like experience for browsing and modifying
    semantic model objects, expressions, and properties using an ipywidgets
    interface.

    The editor displays a collapsible tree of all model objects (tables,
    columns, measures, partitions, hierarchies, relationships, roles,
    expressions, perspectives, cultures) in the left panel. Selecting an
    object shows its editable properties and expression (if applicable) in
    the right panel.

    `XMLA read/write <https://learn.microsoft.com/power-bi/enterprise/service-premium-connect-tools#to-enable-read-write-for-a-premium-capacity>`_ must be enabled when ``readonly=False``.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    readonly : bool, default=False
        If True, opens in read-only mode (no edits allowed).
        Set to False to enable editing (requires XMLA read/write on the
        capacity).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached
        lakehouse or if no lakehouse attached, resolves to the workspace
        of the notebook.

    Returns
    -------
    _SemanticModelEditor
        The editor instance. The widget is automatically displayed.
        Access the underlying TOM connection via ``editor._tom``.
    """

    sempy.fabric._client._utils._init_analysis_services()

    from sempy_labs.tom._model import TOMWrapper

    tom = TOMWrapper(dataset=dataset, workspace=workspace, readonly=readonly)
    ed = _SemanticModelEditor(tom, readonly)
    display(ed.ui)

    return ed
