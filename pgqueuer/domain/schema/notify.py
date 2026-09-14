from __future__ import annotations

import textwrap

from pgqueuer.domain.schema.model import Function, Trigger
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import FunctionName, TableName, TriggerName


def notify_function_body(channel: str) -> str:
    """The plpgsql body of the change-notification trigger function."""
    return textwrap.dedent(
        f"""
        DECLARE
            to_emit BOOLEAN := false;  -- Flag to decide whether to emit a notification
        BEGIN
            -- Check operation type and set the emit flag accordingly
            IF TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW THEN
                to_emit := true;
            ELSIF TG_OP = 'DELETE' THEN
                to_emit := true;
            ELSIF TG_OP = 'INSERT' THEN
                to_emit := true;
            ELSIF TG_OP = 'TRUNCATE' THEN
                to_emit := true;
            END IF;

            -- Perform notification if the emit flag is set
            IF to_emit THEN
                PERFORM pg_notify(
                    '{channel}',
                    json_build_object(
                        'channel', '{channel}',
                        'operation', lower(TG_OP),
                        'sent_at', NOW(),
                        'table', TG_TABLE_NAME,
                        'type', 'table_changed_event'
                    )::text
                );
            END IF;

            -- Return appropriate value based on the operation
            IF TG_OP IN ('INSERT', 'UPDATE') THEN
                RETURN NEW;
            ELSIF TG_OP = 'DELETE' THEN
                RETURN OLD;
            ELSE
                RETURN NULL; -- For TRUNCATE and other non-row-specific contexts
            END IF;

        END;
        """
    )


def notify_function(settings: DBSettings) -> Function:
    return Function(
        name=FunctionName(settings.function),
        body=notify_function_body(settings.channel),
    )


def notify_trigger(settings: DBSettings) -> Trigger:
    return Trigger(
        name=TriggerName(settings.trigger),
        table=TableName(settings.queue_table),
        function=FunctionName(settings.function),
    )
