from __future__ import annotations

import dataclasses
import textwrap


@dataclasses.dataclass(frozen=True)
class ComposedQuery:
    """A rendered statement paired with its positional arguments."""

    sql: str
    args: tuple[object, ...]


@dataclasses.dataclass
class SqlComposer:
    """Assemble one WITH-chained statement from conditional fragments.

    ``bind`` couples every runtime value to an auto-numbered ``$N``
    placeholder, so a fragment that is left out never leaves a gap in the
    parameter list — the hand-numbering foot-gun of dynamic SQL. The rendered
    text is deterministic for a given fragment set, so driver-side prepared
    statement caches see one entry per query shape.

    The composer owns every bit of whitespace, so no SQL literal carries
    indentation. ``cte`` takes one clause per argument and strips each before
    indenting the block; an optional clause is an empty string rather than a
    newline spliced in by hand. ``where`` renders a condition list under one
    keyword, and ``nest`` indents a body under a header, so nesting depth is
    declared by the call structure instead of typed into a literal. Because
    each clause is normalised on its own, a multi-line SQL string literal
    cannot span two clauses.

    Usage example::

        composer = SqlComposer()
        limit = composer.bind(10)
        composer.cte("ready", f"SELECT id FROM jobs LIMIT {limit}")
        query = composer.render("SELECT * FROM ready")
        await driver.fetch(query.sql, *query.args)
    """

    values: list[object] = dataclasses.field(default_factory=list)
    fragments: list[str] = dataclasses.field(default_factory=list)

    def bind(self, value: object) -> str:
        self.values.append(value)
        return f"${len(self.values)}"

    @staticmethod
    def clauses(*parts: str) -> str:
        """Join clauses onto their own lines, dropping the ones left empty."""
        return "\n".join(part for part in parts if part)

    @staticmethod
    def where(*conditions: str, joiner: str = "AND") -> str:
        """Render a WHERE from conditions, dropping empty ones; no conditions renders nothing."""
        kept = [condition for condition in conditions if condition]
        if not kept:
            return ""
        # Right-align the joiner in the width of WHERE so every condition starts
        # in the same column: "WHERE a", "  AND b", "   OR c".
        continuation = f"\n{joiner:>{len('WHERE')}} "
        return "WHERE " + continuation.join(kept)

    @staticmethod
    def nest(header: str, *body: str, footer: str = "") -> str:
        """Indent body one level under header, so nesting is declared rather than typed."""
        kept = [part for part in body if part]
        indented = textwrap.indent("\n".join(kept), "    ")
        return "\n".join(part for part in (header, indented, footer) if part)

    def cte(self, name: str, *clauses: str, comment: str = "") -> None:
        """Append a named CTE built from one clause per argument.

        A comment must be a single line: it orients whoever reads the
        statement in a Postgres log, while the reasoning behind a clause
        belongs in a Python comment next to the code that decides it.
        """
        if "\n" in comment:
            raise ValueError("A CTE comment must be a single line")

        dedented = [textwrap.dedent(clause).strip() for clause in clauses]
        body = textwrap.indent("\n".join(part for part in dedented if part), "    ")
        header = f"-- {comment}\n" if comment else ""
        self.fragments.append(f"{header}{name} AS (\n{body}\n)")

    def render(self, final: str) -> ComposedQuery:
        with_clause = "WITH\n" + ",\n\n".join(self.fragments) + "\n" if self.fragments else ""
        return ComposedQuery(sql=f"{with_clause}{final}", args=tuple(self.values))
