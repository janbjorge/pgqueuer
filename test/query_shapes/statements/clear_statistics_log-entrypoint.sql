DELETE FROM pgqueuer_statistics WHERE entrypoint = ANY($1)
