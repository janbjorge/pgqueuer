ALTER TABLE pgqueuer SET (
    autovacuum_vacuum_scale_factor   = 0.01,
    autovacuum_vacuum_threshold      = 1000,
    autovacuum_analyze_scale_factor  = 0.02,
    autovacuum_analyze_threshold     = 500,
    autovacuum_vacuum_cost_limit     = 10000,
    autovacuum_vacuum_cost_delay     = 0,
    fillfactor                       = 70
);

ALTER TABLE pgqueuer_schedules SET (
    autovacuum_vacuum_scale_factor   = 0.01,
    autovacuum_vacuum_threshold      = 1000,
    autovacuum_analyze_scale_factor  = 0.02,
    autovacuum_analyze_threshold     = 500,
    autovacuum_vacuum_cost_limit     = 10000,
    autovacuum_vacuum_cost_delay     = 0,
    fillfactor                       = 70
);

ALTER TABLE pgqueuer_log SET (
    autovacuum_vacuum_scale_factor   = 0.95,
    autovacuum_vacuum_threshold      = 1000000,
    autovacuum_analyze_scale_factor  = 0.05,
    autovacuum_analyze_threshold     = 10000
);

ALTER TABLE pgqueuer_statistics SET (
    autovacuum_vacuum_scale_factor   = 0.95,
    autovacuum_vacuum_threshold      = 1000000,
    autovacuum_analyze_scale_factor  = 0.05,
    autovacuum_analyze_threshold     = 10000
);
