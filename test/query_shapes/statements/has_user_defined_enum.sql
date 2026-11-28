SELECT enumlabel, typname
FROM pg_enum
JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
