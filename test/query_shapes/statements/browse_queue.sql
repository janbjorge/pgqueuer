SELECT * FROM pgqueuer
WHERE ($3::text[] IS NULL OR status::text = ANY($3::text[]))
  AND ($4::text[] IS NULL OR entrypoint = ANY($4::text[]))
ORDER BY priority DESC, id ASC
LIMIT $1 OFFSET $2
