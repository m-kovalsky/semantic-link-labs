import re
from typing import Optional


def convert_column_data_type(str_type: str) -> str:

    TYPE_MAPPING = {
        "boolean": "Boolean",
        "tinyint": "Int64",
        "smallint": "Int64",
        "int": "Int64",
        "integer": "Int64",
        "bigint": "Int64",
        "long": "Int64",
        "short": "Int64",
        "float": "Double",
        "double": "Double",
        "decimal": "Decimal",
        "string": "String",
        "char": "String",
        "varchar": "String",
        "binary": "Binary",
        "date": "DateTime",
        "timestamp": "DateTime",
        "timestamp_ntz": "DateTime",
    }
    str_type = str_type.lower()
    if str_type in TYPE_MAPPING:
        return TYPE_MAPPING[str_type]
    if "decimal" in str_type:
        return "Decimal"
    if "char" in str_type or "string" in str_type:
        return "String"
    if "int" in str_type or "long" in str_type:
        return "Int64"
    if "float" in str_type or "double" in str_type:
        return "Double"
    else:
        print(f"Warning: Unrecognized data type '{str_type}'. Defaulting to 'String'.")
        return "String"


def convert_sql_to_dax(
    sql: str,
    column_map: dict[str, str],
    default_table: str = "summary",
    relationships: Optional[list[dict]] = None,
) -> str:
    dax = sql.strip()

    # Build a map of (from_table, to_table) -> True for both orientations
    # so iterator selection can prefer the "from" (many) side of the
    # relationship and wrap the "to" (one) side in ``RELATED(...)``.
    #
    # ``relationships`` is expected to be a list of dicts with at least
    # ``fromTable`` and ``toTable`` keys (as produced by
    # ``convert_from_snowflake``). Other keys are ignored.
    _rel_from_to: set = set()
    for _r in relationships or []:
        _ft = _r.get("fromTable") if isinstance(_r, dict) else None
        _tt = _r.get("toTable") if isinstance(_r, dict) else None
        if _ft and _tt:
            _rel_from_to.add((_ft, _tt))

    # =========================================================
    # 1. STRING PROTECTION (CRITICAL - MUST BE FIRST)
    # =========================================================
    def protect_strings(text):
        strings = {}

        def repl(m):
            key = f"__str{len(strings)}__"
            strings[key] = m.group(0)
            return key

        return re.sub(r"'[^']*'", repl, text), strings

    def restore_strings(text, strings):
        for k, v in strings.items():
            text = text.replace(k, '"' + v.strip("'") + '"')
        return text

    dax, strings = protect_strings(dax)

    # =========================================================
    # 1b. BALANCED-PAREN HELPER
    # =========================================================
    def _find_matching_paren(text: str, open_idx: int) -> int:
        """Given the index of an opening ``(`` in ``text``, return the index
        of the matching ``)``, or -1 if unbalanced."""
        depth = 0
        for k in range(open_idx, len(text)):
            ch = text[k]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return k
        return -1

    def _split_top_level(text: str, sep: str) -> list:
        """Split ``text`` on ``sep`` at depth 0 (ignoring parens)."""
        parts = []
        depth = 0
        start = 0
        for k, ch in enumerate(text):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif depth == 0 and ch == sep:
                parts.append(text[start:k])
                start = k + 1
        parts.append(text[start:])
        return parts

    # =========================================================
    # 2. MEASURE → COLUMN
    # =========================================================
    dax = re.sub(
        r"\bMEASURE\(`([^`]+)`\)",
        r"[\1]",
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 2b. DIV0(a, b) → DIVIDE(a, b)
    # =========================================================
    # Snowflake's ``DIV0`` is divide-by-zero-safe and is the same shape as
    # DAX ``DIVIDE``.
    dax = re.sub(r"\bDIV0\s*\(", "DIVIDE(", dax, flags=re.IGNORECASE)

    # =========================================================
    # 3. COUNT(*)
    # =========================================================
    dax = re.sub(
        r"\bCOUNT\s*\(\s*\*\s*\)",
        f"COUNTROWS('{default_table}')",
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 4. COUNT(DISTINCT ...) — including CASE WHEN
    # =========================================================
    # Translate ``COUNT(DISTINCT CASE WHEN <cond> THEN <col> END)`` to the
    # CALCULATE(DISTINCTCOUNT(<col>), <cond>) form. Plain
    # ``COUNT(DISTINCT <expr>)`` becomes ``DISTINCTCOUNT(<expr>)``.
    def _rewrite_count_distinct(text: str) -> str:
        out = []
        i = 0
        pat = re.compile(r"\bCOUNT\s*\(\s*DISTINCT\s+", flags=re.IGNORECASE)
        while i < len(text):
            m = pat.search(text, i)
            if not m:
                out.append(text[i:])
                break
            out.append(text[i : m.start()])
            # Find the matching close paren of the COUNT call.
            open_paren = text.rfind("(", m.start(), m.end())
            close = _find_matching_paren(text, open_paren)
            if close == -1:
                out.append(text[m.start() :])
                break
            arg = text[m.end() : close].strip()
            case_match = re.fullmatch(
                r"CASE\s+WHEN\s+(.+?)\s+THEN\s+(.+?)\s+END",
                arg,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if case_match:
                cond = case_match.group(1).strip()
                col = case_match.group(2).strip()
                out.append(f"CALCULATE(DISTINCTCOUNT({col}), {cond})")
            else:
                out.append(f"DISTINCTCOUNT({arg})")
            i = close + 1
        return "".join(out)

    dax = _rewrite_count_distinct(dax)

    # =========================================================
    # 5. AGGREGATIONS (rename AVG → AVERAGE; SUM/MAX/MIN unchanged)
    # =========================================================
    # Use balanced-paren matching so nested parens (e.g. ``AVG(SUM(x))``) are
    # handled correctly.
    def _rename_avg(text: str) -> str:
        out = []
        i = 0
        pat = re.compile(r"\bAVG\s*\(", flags=re.IGNORECASE)
        while i < len(text):
            m = pat.search(text, i)
            if not m:
                out.append(text[i:])
                break
            out.append(text[i : m.start()])
            open_paren = m.end() - 1
            close = _find_matching_paren(text, open_paren)
            if close == -1:
                out.append(text[m.start() :])
                break
            inner = text[open_paren + 1 : close]
            out.append(f"AVERAGE({inner})")
            i = close + 1
        return "".join(out)

    dax = _rename_avg(dax)

    # =========================================================
    # 6. FILTER (WHERE → CALCULATE)
    # =========================================================
    def handle_filter(match):
        expr = match.group(1)
        condition = match.group(2)
        return f"CALCULATE({expr}, {condition})"

    dax = re.sub(
        r"(.+?)\s+FILTER\s*\(\s*WHERE\s+(.+?)\)",
        handle_filter,
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 7. IN (...) → IN {...}
    # =========================================================
    dax = re.sub(
        r"\bIN\s*\(([^)]+)\)",
        lambda m: f"IN {{{m.group(1)}}}",
        dax,
        flags=re.IGNORECASE,
    )

    # =========================================================
    # 8. SAFE DIVISION (recursive, balanced-paren aware)
    # =========================================================
    # Translate ``a / b`` to ``DIVIDE(a, b)`` at every nesting level. Unwrap
    # ``NULLIF(b, 0)`` on the denominator since DIVIDE is already
    # divide-by-zero-safe.
    def _rewrite_division(text: str) -> str:
        # First recurse into each balanced ``(...)`` sub-expression so that
        # divisions nested inside parens are handled before processing the
        # current level.
        rebuilt = []
        i = 0
        while i < len(text):
            if text[i] == "(":
                close = _find_matching_paren(text, i)
                if close == -1:
                    rebuilt.append(text[i:])
                    break
                inner = text[i + 1 : close]
                rebuilt.append("(" + _rewrite_division(inner) + ")")
                i = close + 1
            else:
                rebuilt.append(text[i])
                i += 1
        text = "".join(rebuilt)

        # Split at the top-level ``/`` operator.
        parts = _split_top_level(text, "/")
        if len(parts) <= 1:
            return text
        result = parts[0].strip()
        for right in parts[1:]:
            right = right.strip()
            # Unwrap NULLIF(<x>, 0) on the denominator.
            nullif = re.fullmatch(
                r"NULLIF\s*\(\s*(.+?)\s*,\s*0\s*\)",
                right,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if nullif:
                right = nullif.group(1).strip()
            result = f"DIVIDE({result}, {right})"
        return result

    dax = _rewrite_division(dax)

    # =========================================================
    # 9. WINDOW FUNCTIONS
    # =========================================================
    # ``<agg>() OVER ()`` — unbounded window over the default table.
    dax = re.sub(
        r"(MAX\([^)]+\))\s+OVER\(\)",
        lambda m: f"CALCULATE({m.group(1)}, ALL('{default_table}'))",
        dax,
        flags=re.IGNORECASE,
    )

    # ``<outer_agg>(<inner>) OVER (ORDER BY <col> ROWS BETWEEN N PRECEDING
    # AND CURRENT ROW)`` — translate to a CALCULATE with a DATESINPERIOD
    # filter. When ``<inner>`` is itself an aggregate (a non-standard but
    # common Snowflake/BigQuery pattern), the redundant outer aggregate is
    # stripped and only the inner aggregate body is preserved.
    def _find_open_paren(text: str, close_idx: int) -> int:
        depth = 0
        for k in range(close_idx, -1, -1):
            ch = text[k]
            if ch == ")":
                depth += 1
            elif ch == "(":
                depth -= 1
                if depth == 0:
                    return k
        return -1

    def _rewrite_over_rolling(text: str) -> str:
        agg_token_re = re.compile(
            r"\b(SUM|AVERAGE|AVG|MIN|MAX|COUNT)\s*\(", flags=re.IGNORECASE
        )
        # Window ORDER BY column: bare identifier, ``table.column``, or
        # backtick-quoted forms.
        order_re = re.compile(
            r"ORDER\s+BY\s+"
            r"("
            r"`[^`]+`(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*))?"
            r"|[A-Za-z_][A-Za-z0-9_]*(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*))?"
            r")",
            flags=re.IGNORECASE,
        )
        rows_re = re.compile(
            r"ROWS\s+BETWEEN\s+(\d+)\s+PRECEDING\s+AND\s+CURRENT\s+ROW",
            flags=re.IGNORECASE,
        )

        over_re = re.compile(r"\bOVER\s*\(", flags=re.IGNORECASE)
        # Iterate until no more OVER patterns are found. Each successful
        # rewrite shortens the string at known positions, so we restart
        # the scan from the beginning each iteration to avoid index drift.
        guard = 0
        while True:
            guard += 1
            if guard > 50:
                break
            m = over_re.search(text)
            if not m:
                break
            over_open = m.end() - 1
            over_close = _find_matching_paren(text, over_open)
            if over_close == -1:
                break

            # Locate the aggregate call immediately preceding OVER.
            k = m.start() - 1
            while k >= 0 and text[k].isspace():
                k -= 1
            if k < 0 or text[k] != ")":
                # No aggregate call before OVER — give up on this OVER.
                break
            agg_close = k
            agg_open = _find_open_paren(text, agg_close)
            if agg_open == -1:
                break
            # Walk back to capture the function name.
            fn_end = agg_open
            fn_start = fn_end
            while fn_start > 0 and (
                text[fn_start - 1].isalnum() or text[fn_start - 1] == "_"
            ):
                fn_start -= 1
            if fn_start == fn_end:
                break
            outer_agg = text[fn_start:fn_end]
            if not agg_token_re.match(outer_agg + "("):
                # Not a recognized aggregate — bail to avoid corrupting text.
                break
            agg_inner = text[agg_open + 1 : agg_close]
            window_spec = text[over_open + 1 : over_close]

            order_match = order_re.search(window_spec)
            rows_match = rows_re.search(window_spec)
            if not order_match or not rows_match:
                # Unsupported window shape — leave as-is and stop scanning
                # to avoid infinite loops.
                break

            order_col = order_match.group(1)
            n_preceding = int(rows_match.group(1))

            # If the inner expression is itself an aggregate, strip the
            # redundant outer aggregate.
            inner_stripped = agg_inner.strip()
            inner_is_agg = bool(
                agg_token_re.match(inner_stripped)
            )
            body_sql = inner_stripped if inner_is_agg else f"{outer_agg}({agg_inner})"

            replacement = (
                f"CALCULATE({body_sql}, "
                f"DATESINPERIOD({order_col}, MAX({order_col}), "
                f"-{n_preceding}, DAY))"
            )
            text = text[:fn_start] + replacement + text[over_close + 1 :]
        return text

    dax = _rewrite_over_rolling(dax)

    # =========================================================
    # 10. RESTORE STRINGS (AFTER ALL LOGIC)
    # =========================================================
    dax = restore_strings(dax, strings)

    # =========================================================
    # 11. TABLE QUOTE FIXES
    # =========================================================
    dax = re.sub(r'COUNTROWS\("([^"]+)"\)', r"COUNTROWS('\1')", dax)
    dax = re.sub(r'ALL\("([^"]+)"\)', r"ALL('\1')", dax)

    # =========================================================
    # 12. COLUMN REPLACEMENT (case-insensitive, backtick-aware)
    # =========================================================
    # SQL identifiers are case-insensitive for unquoted names. Match
    # case-insensitively against ``column_map`` keys; the substituted DAX
    # form uses the canonical case from the map's value. Backtick-quoted
    # identifiers (``table.`column name``` and bare ```column name```) are
    # also handled here.
    def replace_columns(text):
        refs = {}

        def protect(m):
            key = f"__col{len(refs)}__"
            refs[key] = m.group(0)
            return key

        text = re.sub(r"'[^']+'\[[^\]]+\]", protect, text)

        # Process longer keys first so that ``table.column`` wins over
        # ``column``.
        for col in sorted(column_map.keys(), key=len, reverse=True):
            replacement = column_map[col]
            if "." in col:
                # Qualified key: table.column. Allow either side to be
                # backtick-quoted in the source.
                tbl_part, col_part = col.split(".", 1)
                tbl_pat = rf"`{re.escape(tbl_part)}`|{re.escape(tbl_part)}"
                col_pat = rf"`{re.escape(col_part)}`|{re.escape(col_part)}"
                pattern = (
                    rf"(?<![A-Za-z0-9_])(?:{tbl_pat})\.(?:{col_pat})"
                    rf"(?![A-Za-z0-9_])"
                )
            else:
                # Bare key. Must not be the trailing part of a qualified
                # ref (``x.col``) and must not already be inside ``[...]``.
                pattern = (
                    rf"(?<!\[)(?<![A-Za-z0-9_.`])"
                    rf"(?:`{re.escape(col)}`|{re.escape(col)})"
                    rf"(?![A-Za-z0-9_])"
                )
            text = re.sub(pattern, lambda _m: replacement, text, flags=re.IGNORECASE)

        for k, v in refs.items():
            text = text.replace(k, v)

        return text

    dax = replace_columns(dax)

    # =========================================================
    # 13. AGGREGATIONS WITH EXPRESSIONS → ITERATOR FORM
    # =========================================================
    # DAX SUM/AVERAGE/MIN/MAX accept only a single column reference. If the
    # argument contains an arithmetic expression (or multiple column refs),
    # rewrite as follows:
    #   * If the argument decomposes (recursively) into a top-level additive
    #     combination of single column references, distribute the
    #     aggregation: ``SUM(a - b) -> (SUM(a) - SUM(b))``.
    #   * Otherwise convert to the iterator form
    #     ``SUMX/AVERAGEX/MINX/MAXX`` over the table containing the
    #     referenced columns.
    def _rewrite_agg_iterators(text: str) -> str:
        agg_iter_map = {
            "SUM": "SUMX",
            "AVERAGE": "AVERAGEX",
            "MIN": "MINX",
            "MAX": "MAXX",
        }
        col_ref_re = re.compile(r"'([^']+)'\[[^\]]+\]")
        single_col_re = re.compile(r"^'[^']+'\[[^\]]+\]$")
        agg_re = re.compile(r"\b(SUM|AVERAGE|MIN|MAX)\s*\(", flags=re.IGNORECASE)

        def _strip_outer_parens(expr: str) -> str:
            expr = expr.strip()
            while expr.startswith("(") and expr.endswith(")"):
                inner = expr[1:-1]
                d = 0
                balanced = True
                for ch in inner:
                    if ch == "(":
                        d += 1
                    elif ch == ")":
                        d -= 1
                        if d < 0:
                            balanced = False
                            break
                if balanced and d == 0:
                    expr = inner.strip()
                else:
                    break
            return expr

        def _split_top_level_additive(arg: str):
            """Split ``arg`` on top-level ``+``/``-`` operators.

            Returns a list of ``(sign, term)`` tuples. Top-level ``*``/``/``
            do not split; if the whole expression is a single multiplicative
            term, returns a single-element list.
            """
            terms = []
            depth = 0
            start = 0
            sign = "+"
            i = 0
            while i < len(arg):
                ch = arg[i]
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                elif depth == 0 and ch in "+-":
                    prev = arg[:i].rstrip()
                    if not prev or prev[-1] in "+-*/(":
                        i += 1
                        continue
                    term = arg[start:i].strip()
                    if term:
                        terms.append((sign, term))
                    sign = ch
                    start = i + 1
                i += 1
            tail = arg[start:].strip()
            if tail:
                terms.append((sign, tail))
            return terms

        def _emit_iterator(func: str, term: str) -> str:
            """Emit the iterator form (SUMX/AVERAGEX/etc.) for ``term``.

            Iterator table selection (in order of preference):

            1. **Relationship-driven**: if ``relationships`` was supplied
               and one of the referenced tables is on the "from" (many)
               side of a relationship to another referenced table, use
               that "from" table. Columns belonging to the "to" (one)
               side are wrapped in ``RELATED(...)``.
            2. ``default_table`` if it is one of the referenced tables.
            3. The first referenced table.

            Wrapping rule: any column reference whose table is NOT the
            chosen iterator table is wrapped in ``RELATED(...)``.
            """
            referenced_tables = []
            for c in col_ref_re.finditer(term):
                t = c.group(1)
                if t not in referenced_tables:
                    referenced_tables.append(t)

            iter_table = None
            # 1. Relationship-driven choice — pick the table that is on the
            #    "from" (many) side of a relationship to another
            #    referenced table.
            if _rel_from_to and len(referenced_tables) > 1:
                for cand in referenced_tables:
                    for other in referenced_tables:
                        if cand == other:
                            continue
                        if (cand, other) in _rel_from_to:
                            iter_table = cand
                            break
                    if iter_table:
                        break

            # 2. Fall back to default_table if it's one of the refs.
            if iter_table is None:
                if default_table and default_table in referenced_tables:
                    iter_table = default_table
                elif referenced_tables:
                    iter_table = referenced_tables[0]
                else:
                    iter_table = default_table

            if iter_table and len(referenced_tables) > 1:
                def _wrap_related(match: "re.Match[str]") -> str:
                    ref = match.group(0)
                    tbl = match.group(1)
                    if tbl == iter_table:
                        return ref
                    return f"RELATED({ref})"
                term_rewritten = col_ref_re.sub(_wrap_related, term)
            else:
                term_rewritten = term
            return f"{agg_iter_map[func]}('{iter_table}', {term_rewritten})"

        def _emit_term(func: str, term: str) -> Optional[str]:
            """Convert a single additive term into an aggregation.

            * Single column reference → scalar ``func(col)``.
            * Recursively splittable additive expression → distributed form.
            * Multiplicative / arbitrary expression → iterator form.
            """
            term = _strip_outer_parens(term)
            if single_col_re.match(term):
                return f"{func}({term})"
            nested = _try_distribute(func, term)
            if nested is not None:
                return nested
            # Multiplicative or otherwise non-additive term — emit iterator.
            if not col_ref_re.search(term):
                return None
            return _emit_iterator(func, term)

        def _try_distribute(func: str, arg: str) -> Optional[str]:
            arg = _strip_outer_parens(arg)
            if single_col_re.match(arg):
                return f"{func}({arg})"
            terms = _split_top_level_additive(arg)
            if not terms or len(terms) <= 1:
                return None
            distributed_terms = []
            for sign, term in terms:
                emitted = _emit_term(func, term)
                if emitted is None:
                    return None
                distributed_terms.append((sign, emitted))
            pieces = []
            for idx, (s, t) in enumerate(distributed_terms):
                if idx == 0:
                    pieces.append("" if s == "+" else "-")
                    pieces.append(t)
                else:
                    pieces.append(f" {s} ")
                    pieces.append(t)
            return "(" + "".join(pieces) + ")"

        i = 0
        out_parts = []
        while i < len(text):
            m = agg_re.search(text, i)
            if not m:
                out_parts.append(text[i:])
                break
            out_parts.append(text[i : m.start()])
            func = m.group(1).upper()
            depth = 1
            j = m.end()
            while j < len(text) and depth > 0:
                ch = text[j]
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                j += 1
            if depth != 0:
                out_parts.append(text[m.start() :])
                break
            arg = _strip_outer_parens(text[m.end() : j - 1])

            if single_col_re.match(arg):
                out_parts.append(f"{func}({arg})")
                i = j
                continue

            # If the argument is a bare unqualified identifier (a column
            # name that wasn't resolved via column_map because it wasn't
            # declared in the YAML), qualify it against ``default_table``
            # and emit the scalar aggregation form. Without this, the
            # iterator branch below would emit e.g.
            # ``SUMX('fact_returns', ORIGINAL_SALES_AMOUNT)`` which is
            # invalid DAX. Backticks are stripped to match the rest of
            # the translator.
            bare_ident = re.fullmatch(
                r"`?([A-Za-z_][A-Za-z0-9_ ]*)`?", arg
            )
            if bare_ident and default_table:
                out_parts.append(
                    f"{func}('{default_table}'[{bare_ident.group(1)}])"
                )
                i = j
                continue

            distributed = _try_distribute(func, arg)
            if distributed is not None:
                out_parts.append(distributed)
                i = j
                continue

            out_parts.append(_emit_iterator(func, arg))
            i = j
        return "".join(out_parts)

    dax = _rewrite_agg_iterators(dax)

    return dax


def convert_format_from_databricks(fmt: dict = None) -> str | None:
    """
    Convert Databricks metric view format dictionary
    into a Power BI format string.

    Returns
    -------
    str | None
    """

    if not fmt:
        return None

    # =========================
    # Currency symbol resolver
    # =========================
    def get_currency_symbol(code: str) -> str:
        symbols = {
            "USD": "$",
            "AUD": "$",
            "CAD": "$",
            "EUR": "€",
            "GBP": "£",
            "ILS": "₪",
            "JPY": "¥",
            "CNY": "¥",
            "INR": "₹",
            "KRW": "₩",
            "RUB": "₽",
            "TRY": "₺",
            "BRL": "R$",
            "MXN": "$",
            "ZAR": "R",
            "CHF": "CHF ",
            "SEK": "kr",
            "NOK": "kr",
            "DKK": "kr",
            "PLN": "zł",
            "CZK": "Kč",
            "HUF": "Ft",
            "AED": "د.إ",
            "SAR": "﷼",
            "DZD": "DZD ",
        }
        return symbols.get((code or "").upper(), f"{code.upper()} " if code else "")

    # =========================
    # Helpers
    # =========================
    def build_decimal_part(decimal_info: dict, abbreviation: str) -> str:
        if not decimal_info:
            return ""

        dtype = decimal_info.get("type")
        places = decimal_info.get("places", 0)

        # COMPACT → cap decimals
        if abbreviation == "COMPACT":
            max_places = min(places if places else 2, 2)
            return "." + ("#" * max_places) if max_places > 0 else ""

        if dtype == "ALL":
            return ".########"

        if places == 0:
            return ""

        if dtype == "EXACT":
            return "." + ("0" * places)

        if dtype == "MAX":
            return "." + ("#" * places)

        return ""

    def build_scientific(decimal_info: dict) -> str:
        if not decimal_info:
            return "0E+00"

        dtype = decimal_info.get("type")
        places = decimal_info.get("places", 2)

        if dtype == "EXACT":
            return f"0.{ '0'*places }E+00" if places > 0 else "0E+00"

        if dtype == "MAX":
            return f"0.{ '#'*places }E+00" if places > 0 else "0E+00"

        if dtype == "ALL":
            return "0.00E+00"  # controlled default

        return "0.00E+00"

    def apply_grouping(base: str, hide_group_separator: bool) -> str:
        if hide_group_separator:
            return base.replace("#,0", "0")
        return base

    def apply_compact(base: str, abbreviation: str) -> str:
        if abbreviation == "COMPACT":
            return base + ",,"
        return base

    # =========================
    # Validation
    # =========================
    if not fmt or not isinstance(fmt, dict):
        return None

    key = next(iter(fmt), None)
    if not key:
        return None

    props = fmt.get(key, {})

    decimal_info = props.get("decimal_places")
    abbreviation = props.get("abbreviation", "NONE")
    hide_group = props.get("hide_group_separator", False)

    # =========================
    # NUMBER PLAIN
    # =========================
    if key == "number_plain":
        if abbreviation == "SCIENTIFIC":
            return build_scientific(decimal_info)

        decimal_part = build_decimal_part(decimal_info, abbreviation)
        base = f"#,0{decimal_part}"
        base = apply_grouping(base, hide_group)
        base = apply_compact(base, abbreviation)
        return base

    # =========================
    # NUMBER CURRENCY
    # =========================
    if key == "number_currency":
        symbol = get_currency_symbol(props.get("currency_code"))

        if abbreviation == "SCIENTIFIC":
            return f"{symbol}{build_scientific(decimal_info)}"

        decimal_part = build_decimal_part(decimal_info, abbreviation)
        base = f"{symbol}#,0{decimal_part}"
        base = apply_grouping(base, hide_group)
        base = apply_compact(base, abbreviation)
        return base

    # =========================
    # NUMBER PERCENT
    # =========================
    if key == "number_percent":
        if abbreviation == "SCIENTIFIC":
            return build_scientific(decimal_info) + "%"

        decimal_part = build_decimal_part(decimal_info, abbreviation)
        return f"0{decimal_part}%"

    # =========================
    # NUMBER BYTES
    # =========================
    if key == "number_bytes":
        decimal_part = build_decimal_part(decimal_info, abbreviation)
        base = f"#,0{decimal_part}"
        base = apply_grouping(base, hide_group)
        return base

    # =========================
    # DATE
    # =========================
    if key == "date":
        return {
            "YEAR_MONTH_DAY": "yyyy-MM-dd",
            "MONTH_DAY_YEAR": "MM/dd/yyyy",
        }.get(props.get("date_format"))

    # =========================
    # DATE TIME
    # =========================
    if key == "date_time":
        date_part = {"YEAR_MONTH_DAY": "yyyy-MM-dd"}.get(
            props.get("date_format"), "yyyy-MM-dd"
        )

        return f"{date_part} HH:mm:ss"

    # =========================
    # FALLBACK
    # =========================
    return None
