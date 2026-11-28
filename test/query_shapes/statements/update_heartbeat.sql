UPDATE pgqueuer SET heartbeat = NOW() WHERE id = ANY($1::bigint[])
