# PGQueuer Django Example

A minimal Django project demonstrating [PGQueuer](https://github.com/janbjorge/pgqueuer) integration with:

- **Background email task** enqueued from a Django view
- **PGQueuer worker** process that consumes jobs
- **Django Admin** for monitoring job status
- **Docker Compose** for one-command setup

## Quick Start

```bash
docker-compose up --build
```

Then open:
- **App**: http://localhost:8000/
- **Admin**: http://localhost:8000/admin/ (create superuser with `docker-compose exec web python manage.py createsuperuser`)

## Enqueue a Job

```bash
curl -X POST http://localhost:8000/enqueue/ \
  -d "recipient=user@example.com" \
  -d "subject=Hello" \
  -d "body=Test from PGQueuer"
```

## Architecture

```
Django View → pgqueuer.enqueue("send_email", payload)
                    ↓
            PostgreSQL (LISTEN/NOTIFY)
                    ↓
            PGQueuer Worker → send_email(payload)
                    ↓
            Django EmailJob model updated to "completed"
```

## Key Files

| File | Purpose |
|------|---------|
| `myapp/views.py` | Django views — enqueue jobs, show status |
| `myapp/worker.py` | PGQueuer worker — processes `send_email` jobs |
| `myapp/models.py` | `EmailJob` model tracked in Django admin |
| `myapp/admin.py` | Admin registration with filters and search |
| `docker-compose.yml` | PostgreSQL + Django web + PGQueuer worker |

## Development (without Docker)

```bash
# 1. Start PostgreSQL (or use existing instance)
# 2. Install dependencies
pip install -r requirements.txt

# 3. Run migrations
python manage.py migrate

# 4. In terminal 1 — start Django
python manage.py runserver

# 5. In terminal 2 — start worker
python -m myapp.worker
```

## CI / E2E Testing

The included GitHub Actions workflow (`.github/workflows/django-example.yml`) spins up PostgreSQL, runs migrations, enqueues a job, starts a worker, and verifies the job completes.
