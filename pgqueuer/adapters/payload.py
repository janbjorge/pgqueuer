from __future__ import annotations

import dataclasses
import json
from typing import Literal

PREVIEW_BYTES = 512


@dataclasses.dataclass(frozen=True)
class RenderedPayload:
    kind: Literal["empty", "json", "text", "binary"]
    text: str


def render_payload(payload: bytes | None) -> RenderedPayload:
    """Best-effort human-readable rendering: JSON, then text, then a hex preview."""
    if payload is None or payload == b"":
        return RenderedPayload(kind="empty", text="")
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError:
        preview = payload[:PREVIEW_BYTES].hex(" ")
        suffix = f" … ({len(payload)} bytes total)" if len(payload) > PREVIEW_BYTES else ""
        return RenderedPayload(kind="binary", text=preview + suffix)
    try:
        return RenderedPayload(kind="json", text=json.dumps(json.loads(decoded), indent=2))
    except (json.JSONDecodeError, TypeError):
        return RenderedPayload(kind="text", text=decoded)
