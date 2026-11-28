DELETE FROM pgqueuer_log WHERE entrypoint = ANY($1)
