import psycopg

with psycopg.connect() as conn:
    print(conn.pgconn.libpq_version())  # e.g., 160003 for libpq 16.3
