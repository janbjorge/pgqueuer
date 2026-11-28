SELECT EXISTS (
SELECT FROM information_schema.columns
WHERE
        table_schema = current_schema()
    AND table_name = $1
    AND column_name = $2
);
