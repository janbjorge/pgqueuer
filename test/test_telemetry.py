import contextlib
import importlib
import sys
from types import ModuleType
from typing import Iterator

import pytest

import pgqueuer.telemetry as telemetry


def reload() -> None:
    importlib.reload(telemetry)


class DummyPropagate(ModuleType):
    def __init__(self) -> None:
        super().__init__("opentelemetry.propagate")
        self.inject_called = False
        self.extract_called_with: dict[str, str] | None = None

    def inject(self, carrier: dict[str, str]) -> None:
        self.inject_called = True
        carrier["traceparent"] = "abc"

    def extract(self, carrier: dict[str, str]) -> str:
        self.extract_called_with = carrier
        return "ctx"


class DummyContext(ModuleType):
    def __init__(self) -> None:
        super().__init__("opentelemetry.context")
        self.attached: str | None = None
        self.detached: str | None = None

    def attach(self, ctx: str) -> str:
        self.attached = ctx
        return "token"

    def detach(self, token: str) -> None:
        self.detached = token


class DummyTracer:
    def start_as_current_span(self, name: str) -> contextlib.AbstractContextManager[None]:
        @contextlib.contextmanager
        def cm() -> Iterator[None]:
            yield
        return cm()


class DummyTrace(ModuleType):
    def __init__(self) -> None:
        super().__init__("opentelemetry.trace")
        self.tracer = DummyTracer()

    def get_tracer(self, name: str) -> DummyTracer:
        return self.tracer


def test_no_op_without_opentelemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "opentelemetry", None)
    if "opentelemetry.propagate" in sys.modules:
        monkeypatch.setitem(sys.modules, "opentelemetry.propagate", None)
    reload()
    assert telemetry.capture_headers() is None
    with telemetry.span_from_headers("test", None):
        pass


def test_with_dummy_opentelemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    prop = DummyPropagate()
    ctx = DummyContext()
    trace = DummyTrace()
    monkeypatch.setitem(sys.modules, "opentelemetry.propagate", prop)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace)
    monkeypatch.setitem(sys.modules, "opentelemetry.context", ctx)
    monkeypatch.setitem(sys.modules, "opentelemetry", ModuleType("opentelemetry"))
    reload()
    hdr = telemetry.capture_headers()
    assert hdr == {"traceparent": "abc"}
    with telemetry.span_from_headers("task", hdr):
        pass
    assert prop.inject_called
    assert prop.extract_called_with == {"traceparent": "abc"}
    assert ctx.attached == "ctx"
    assert ctx.detached == "token"
