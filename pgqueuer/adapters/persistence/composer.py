from __future__ import annotations

import dataclasses
import textwrap
from collections.abc import Callable, Sequence
from typing import Generic, TypeVar

ArgsT = TypeVar("ArgsT")


@dataclasses.dataclass(frozen=True)
class ComposedQuery:
    """A rendered statement paired with its positional arguments."""

    sql: str
    args: tuple[object, ...]


@dataclasses.dataclass(frozen=True)
class Cte:
    """A named CTE body, written as one SQL block.

    A comment must be a single line: it orients whoever reads the statement
    in a Postgres log, while the reasoning behind a clause belongs in a Python
    comment next to the code that decides it.
    """

    name: str
    body: str
    _: dataclasses.KW_ONLY
    comment: str = ""

    def __post_init__(self) -> None:
        if "\n" in self.comment:
            raise ValueError("A CTE comment must be a single line")


def block(sql: str) -> str:
    """Dedent a SQL block and drop the lines an inapplicable clause left empty."""
    # dedent ignores whitespace-only lines when measuring the common prefix, so
    # the line an elided clause leaves behind cannot drag the whole block left.
    lines = textwrap.dedent(sql).splitlines()
    return "\n".join(line for line in lines if line.strip())


def render_cte(cte: Cte) -> str:
    definition = f"{cte.name} AS (\n{textwrap.indent(block(cte.body), '    ')}\n)"
    if cte.comment:
        return f"-- {cte.comment}\n{definition}"
    return definition


def render_statement(ctes: Sequence[Cte], final: str) -> str:
    """Chain ``ctes`` into one WITH clause ahead of ``final``."""
    defined: set[str] = set()
    for cte in ctes:
        if cte.name in defined:
            raise ValueError(f"CTE {cte.name!r} is defined twice")
        defined.add(cte.name)
    if not ctes:
        return block(final)
    with_clause = ",\n\n".join(render_cte(cte) for cte in ctes)
    return f"WITH\n{with_clause}\n{block(final)}"


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
        ready = Cte("ready", f"SELECT id FROM jobs LIMIT {limit}")
        query = composer.render([ready], "SELECT * FROM ready")
        await driver.fetch(query.sql, *query.args)
    """

    values: list[object] = dataclasses.field(init=False, default_factory=list)

    def bind(self, value: object) -> str:
        self.values.append(value)
        return f"${len(self.values)}"

    def render(self, ctes: Sequence[Cte], final: str) -> ComposedQuery:
        return ComposedQuery(sql=render_statement(ctes, final), args=tuple(self.values))


@dataclasses.dataclass(frozen=True)
class QueryTemplate(Generic[ArgsT]):
    """Rendered text, and for each ``$N`` in order the reader of its value."""

    sql: str
    readers: tuple[Callable[[ArgsT], object], ...] = dataclasses.field(repr=False)

    def bind(self, args: ArgsT) -> ComposedQuery:
        return ComposedQuery(sql=self.sql, args=tuple(read(args) for read in self.readers))


@dataclasses.dataclass
class TemplateComposer(Generic[ArgsT]):
    """Compose a statement once, to be bound to fresh arguments on every call.

    ``param`` takes how to read the value from a call's arguments instead of
    the value itself, so a cached template stays correct however its
    placeholders were numbered.

    Usage example::

        composer = TemplateComposer[Request]()
        limit = composer.param(lambda request: request.limit)
        template = composer.render([], f"SELECT id FROM jobs LIMIT {limit}")
        query = template.bind(Request(limit=10))
    """

    readers: list[Callable[[ArgsT], object]] = dataclasses.field(init=False, default_factory=list)

    def param(self, read: Callable[[ArgsT], object]) -> str:
        self.readers.append(read)
        return f"${len(self.readers)}"

    def render(self, ctes: Sequence[Cte], final: str) -> QueryTemplate[ArgsT]:
        return QueryTemplate(sql=render_statement(ctes, final), readers=tuple(self.readers))
