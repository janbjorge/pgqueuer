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

    CTE bodies are embedded verbatim — never rewritten — so SQL string
    literals pass through untouched. Optional lines are the caller's job:
    interpolate them (including their leading newline) only when present.

    Usage example::

        c = SqlComposer()
        limit = c.bind(10)
        c.cte("ready", f"SELECT id FROM jobs LIMIT {limit}")
        query = c.render("SELECT * FROM ready")
        await driver.fetch(query.sql, *query.args)
    """

    values: list[object] = dataclasses.field(default_factory=list)
    fragments: list[str] = dataclasses.field(default_factory=list)

    def bind(self, value: object) -> str:
        self.values.append(value)
        return f"${len(self.values)}"

    def cte(self, name: str, body: str, comment: str = "") -> None:
        # Dedent + strip lets callers pass indented triple-quoted comment blocks.
        comment_text = textwrap.dedent(comment).strip()
        header = "".join(f"-- {line}\n" for line in comment_text.splitlines())
        self.fragments.append(f"{header}{name} AS (\n{body}\n)")

    def render(self, final: str) -> ComposedQuery:
        with_clause = "WITH\n" + ",\n\n".join(self.fragments) + "\n" if self.fragments else ""
        return ComposedQuery(sql=f"{with_clause}{final}", args=tuple(self.values))
