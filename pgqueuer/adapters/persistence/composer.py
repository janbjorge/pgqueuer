from __future__ import annotations

import dataclasses
import textwrap


@dataclasses.dataclass(frozen=True)
class ComposedQuery:
    """A rendered statement paired with its positional arguments."""

    sql: str
    args: tuple[object, ...]


@dataclasses.dataclass(frozen=True)
class Cte:
    """A named CTE body that formats as its name.

    A later body references it as ``f"FROM {available}"``, so the CTEs a body
    reads are Python values passed to it rather than names retyped as text.
    A comment must be a single line: it orients whoever reads the statement
    in a Postgres log, while the reasoning behind a clause belongs in a Python
    comment next to the code that decides it.
    """

    name: str
    body: str
    comment: str = ""

    def __post_init__(self) -> None:
        if "\n" in self.comment:
            raise ValueError("A CTE comment must be a single line")

    def __str__(self) -> str:
        return self.name


@dataclasses.dataclass
class SqlComposer:
    """Assemble one WITH-chained statement from conditional fragments.

    ``bind`` couples every runtime value to an auto-numbered ``$N``
    placeholder, so a fragment that is left out never leaves a gap in the
    parameter list — the hand-numbering foot-gun of dynamic SQL. The rendered
    text is deterministic for a given fragment set, so driver-side prepared
    statement caches see one entry per query shape.

    A CTE body is one SQL block, written at whatever indentation the
    surrounding Python happens to sit at: the composer dedents it and applies
    the single level a body needs. A clause that only some query shapes carry
    is interpolated on a line of its own and renders as the empty string when
    it does not apply, so the line goes with it. A body therefore reads as the
    SQL it becomes, and carries no blank lines of its own.

    Usage example::

        composer = SqlComposer()
        limit = composer.bind(10)
        composer.cte("ready", f"SELECT id FROM jobs LIMIT {limit}")
        query = composer.render("SELECT * FROM ready")
        await driver.fetch(query.sql, *query.args)
    """

    values: list[object] = dataclasses.field(default_factory=list)
    fragments: list[str] = dataclasses.field(default_factory=list)
    names: set[str] = dataclasses.field(default_factory=set)

    def bind(self, value: object) -> str:
        self.values.append(value)
        return f"${len(self.values)}"

    @staticmethod
    def block(sql: str) -> str:
        """Dedent a SQL block and drop the lines an inapplicable clause left empty."""
        # dedent ignores whitespace-only lines when measuring the common prefix, so
        # the line an elided clause leaves behind cannot drag the whole block left.
        lines = textwrap.dedent(sql).splitlines()
        return "\n".join(line for line in lines if line.strip())

    def cte(self, name: str, body: str, *, comment: str = "") -> Cte:
        """Append a named CTE built from one SQL block."""
        return self.add(Cte(name, body, comment))

    def add(self, cte: Cte) -> Cte:
        if cte.name in self.names:
            raise ValueError(f"CTE {cte.name!r} is already defined")
        self.names.add(cte.name)
        header = f"-- {cte.comment}\n" if cte.comment else ""
        indented = textwrap.indent(self.block(cte.body), "    ")
        self.fragments.append(f"{header}{cte.name} AS (\n{indented}\n)")
        return cte

    def render(self, final: str) -> ComposedQuery:
        with_clause = "WITH\n" + ",\n\n".join(self.fragments) + "\n" if self.fragments else ""
        return ComposedQuery(sql=f"{with_clause}{self.block(final)}", args=tuple(self.values))
