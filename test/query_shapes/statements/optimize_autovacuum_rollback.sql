ALTER TABLE pgqueuer RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold,
    autovacuum_vacuum_cost_limit,
    autovacuum_vacuum_cost_delay,
    fillfactor
);

ALTER TABLE pgqueuer_schedules RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold,
    autovacuum_vacuum_cost_limit,
    autovacuum_vacuum_cost_delay,
    fillfactor
);

ALTER TABLE pgqueuer_log RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold
);

ALTER TABLE pgqueuer_statistics RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold
);
