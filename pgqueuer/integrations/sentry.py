from __future__ import annotations

import asyncio
import dataclasses
from typing import Any, Awaitable, Callable

import sentry_sdk
from sentry_sdk.consts import OP
from sentry_sdk.integrations import Integration
from sentry_sdk.tracing import TransactionSource


def _inject_headers_single(headers: dict[str, str] | None) -> dict[str, str] | None:
    # Try to get the current active span
    span = sentry_sdk.get_current_span()

    if span is not None:
        # There is an active span, use it to get trace headers
        trace_headers = dict(span.iter_headers())
        return {**trace_headers, **(headers or {})}

    # No active span, so start a new transaction
    with sentry_sdk.start_transaction(
        op="queue.publish",
        name="PGQueuer Enqueue",
        source=TransactionSource.TASK,
    ) as transaction:
        trace_headers = dict(transaction.iter_headers())
        return {**trace_headers, **(headers or {})}


def _inject_headers_batch(headers: list[dict[str, str] | None]) -> list[dict[str, str] | None]:
    return [_inject_headers_single(h) for h in headers]


def _patch_enqueue(func: Callable[..., Any]) -> Callable[..., Any]:
    async def async_single(
        self: Any,
        entrypoint: str,
        payload: Any,
        priority: Any = 0,
        execute_after: Any | None = None,
        dedupe_key: Any | None = None,
        headers: Any | None = None,
    ) -> list[Any]:
        integration = sentry_sdk.get_client().get_integration(PgQueuerIntegration)
        if integration is None:
            return await func(
                self,
                entrypoint,
                payload,
                priority=priority,
                execute_after=execute_after,
                dedupe_key=dedupe_key,
                headers=headers,
            )

        new_headers = _inject_headers_single(headers)
        with sentry_sdk.start_span(
            op=OP.QUEUE_PUBLISH,
            name=entrypoint,
            origin=PgQueuerIntegration.origin,
        ):
            return await func(
                self,
                entrypoint,
                payload,
                priority=priority,
                execute_after=execute_after,
                dedupe_key=dedupe_key,
                headers=new_headers,
            )

    async def async_batch(
        self: Any,
        entrypoint: list[str],
        payload: Any,
        priority: Any = 0,
        execute_after: Any | None = None,
        dedupe_key: Any | None = None,
        headers: Any | None = None,
    ) -> list[Any]:
        integration = sentry_sdk.get_client().get_integration(PgQueuerIntegration)
        import icecream

        icecream.ic("heloo....")
        icecream.ic("Integration:", PgQueuerIntegration)
        if integration is None:
            return await func(
                self,
                entrypoint,
                payload,
                priority=priority,
                execute_after=execute_after,
                dedupe_key=dedupe_key,
                headers=headers,
            )

        header_list = headers if isinstance(headers, list) else [headers] * len(entrypoint)
        icecream.ic(header_list)
        new_headers = _inject_headers_batch(header_list)
        icecream.ic(new_headers)
        with sentry_sdk.start_span(
            op=OP.QUEUE_PUBLISH,
            name=str(entrypoint),
            origin=PgQueuerIntegration.origin,
        ):
            return await func(
                self,
                entrypoint,
                payload,
                priority=priority,
                execute_after=execute_after,
                dedupe_key=dedupe_key,
                headers=new_headers,
            )

    def sync_single(
        self: Any,
        entrypoint: str,
        payload: Any,
        priority: Any = 0,
        execute_after: Any | None = None,
        dedupe_key: Any | None = None,
        headers: Any | None = None,
    ) -> list[Any]:
        integration = sentry_sdk.get_client().get_integration(PgQueuerIntegration)
        if integration is None:
            return func(
                self,
                entrypoint,
                payload,
                priority=priority,
                execute_after=execute_after,
                dedupe_key=dedupe_key,
                headers=headers,
            )

        new_headers = _inject_headers_single(headers)
        with sentry_sdk.start_span(
            op=OP.QUEUE_PUBLISH,
            name=entrypoint,
            origin=PgQueuerIntegration.origin,
        ):
            return func(
                self,
                entrypoint,
                payload,
                priority=priority,
                execute_after=execute_after,
                dedupe_key=dedupe_key,
                headers=new_headers,
            )

    def sync_batch(
        self: Any,
        entrypoint: list[str],
        payload: Any,
        priority: Any = 0,
        execute_after: Any | None = None,
        dedupe_key: Any | None = None,
        headers: Any | None = None,
    ) -> list[Any]:
        integration = sentry_sdk.get_client().get_integration(PgQueuerIntegration)
        if integration is None:
            return func(
                self,
                entrypoint,
                payload,
                priority=priority,
                execute_after=execute_after,
                dedupe_key=dedupe_key,
                headers=headers,
            )

        header_list = headers if isinstance(headers, list) else [headers] * len(entrypoint)
        new_headers = _inject_headers_batch(header_list)
        with sentry_sdk.start_span(
            op=OP.QUEUE_PUBLISH,
            name=str(entrypoint),
            origin=PgQueuerIntegration.origin,
        ):
            return func(
                self,
                entrypoint,
                payload,
                priority=priority,
                execute_after=execute_after,
                dedupe_key=dedupe_key,
                headers=new_headers,
            )

    if asyncio.iscoroutinefunction(func):
        async_single_f = async_single
        async_batch_f = async_batch

        async def async_dispatcher(
            self: Any,
            entrypoint: str | list[str],
            payload: Any,
            priority: Any = 0,
            execute_after: Any | None = None,
            dedupe_key: Any | None = None,
            headers: Any | None = None,
        ) -> list[Any]:
            if isinstance(entrypoint, list):
                return await async_batch_f(
                    self,
                    entrypoint,
                    payload,
                    priority,
                    execute_after,
                    dedupe_key,
                    headers,
                )
            return await async_single_f(
                self,
                entrypoint,
                payload,
                priority,
                execute_after,
                dedupe_key,
                headers,
            )

        return async_dispatcher

    sync_single_f = sync_single
    sync_batch_f = sync_batch

    def sync_dispatcher(
        self: Any,
        entrypoint: str | list[str],
        payload: Any,
        priority: Any = 0,
        execute_after: Any | None = None,
        dedupe_key: Any | None = None,
        headers: Any | None = None,
    ) -> list[Any]:
        if isinstance(entrypoint, list):
            return sync_batch_f(
                self,
                entrypoint,
                payload,
                priority,
                execute_after,
                dedupe_key,
                headers,
            )
        return sync_single_f(
            self,
            entrypoint,
            payload,
            priority,
            execute_after,
            dedupe_key,
            headers,
        )

    return sync_dispatcher


class PgQueuerIntegration(Integration):
    """Sentry integration for PGQueuer."""

    identifier = "pgqueuer"
    origin = f"auto.queue.{identifier}"

    @staticmethod
    def setup_once() -> None:
        from .. import queries

        queries.Queries.enqueue = _patch_enqueue(queries.Queries.enqueue)  # type: ignore[method-assign]
        queries.SyncQueries.enqueue = _patch_enqueue(queries.SyncQueries.enqueue)  # type: ignore[method-assign]
