UPDATE pgqueuer_schedules SET status = 'queued', last_run = NOW(), updated = NOW() WHERE id = ANY($1);
