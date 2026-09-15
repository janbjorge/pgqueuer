CREATE TYPE pgqueuer_status AS ENUM ('queued', 'picked');
CREATE TABLE pgqueuer (
    id SERIAL PRIMARY KEY,
    priority INT NOT NULL,
    queue_manager_id UUID,
    created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    execute_after TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    status pgqueuer_status NOT NULL,
    entrypoint TEXT NOT NULL,
    payload BYTEA
);
CREATE INDEX pgqueuer_priority_id_id1_idx ON pgqueuer (priority ASC, id DESC)
    INCLUDE (id) WHERE status = 'queued';
CREATE INDEX pgqueuer_updated_id_id1_idx ON pgqueuer (updated ASC, id DESC)
    INCLUDE (id) WHERE status = 'picked';
CREATE INDEX pgqueuer_queue_manager_id_idx ON pgqueuer (queue_manager_id)
    WHERE queue_manager_id IS NOT NULL;

CREATE TYPE pgqueuer_statistics_status AS ENUM ('exception', 'successful', 'canceled');
CREATE TABLE pgqueuer_statistics (
    id SERIAL PRIMARY KEY,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT DATE_TRUNC('sec', NOW() at time zone 'UTC'),
    count BIGINT NOT NULL,
    priority INT NOT NULL,
    time_in_queue INTERVAL NOT NULL,
    status pgqueuer_statistics_status NOT NULL,
    entrypoint TEXT NOT NULL
);
CREATE UNIQUE INDEX pgqueuer_statistics_unique_count ON pgqueuer_statistics (
    priority,
    DATE_TRUNC('sec', created at time zone 'UTC'),
    DATE_TRUNC('sec', time_in_queue),
    status,
    entrypoint
);

CREATE TABLE pgqueuer_schedules (
    id SERIAL PRIMARY KEY,
    expression TEXT NOT NULL, -- Crontab-like schedule definition (e.g., '* * * * *')
    entrypoint TEXT NOT NULL,
    heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    next_run TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    last_run TIMESTAMP WITH TIME ZONE,
    status pgqueuer_status DEFAULT 'queued',
    UNIQUE (expression, entrypoint)
);

CREATE FUNCTION fn_pgqueuer_changed() RETURNS TRIGGER AS $$
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
            'ch_pgqueuer',
            json_build_object(
                'channel', 'ch_pgqueuer',
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
$$ LANGUAGE plpgsql;

CREATE TRIGGER tg_pgqueuer_changed
AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON pgqueuer
EXECUTE FUNCTION fn_pgqueuer_changed();
