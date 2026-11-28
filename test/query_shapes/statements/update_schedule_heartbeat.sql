UPDATE pgqueuer_schedules SET heartbeat = NOW(), updated = NOW() WHERE id = ANY($1);
