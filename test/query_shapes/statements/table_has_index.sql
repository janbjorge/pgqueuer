SELECT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE
            tablename  = $1
        AND indexname  = $2
        AND schemaname = current_schema()
);
