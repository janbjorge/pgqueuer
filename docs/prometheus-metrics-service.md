Prometheus Metrics Integration
==============================
PGQueuer now includes an integration with Prometheus, enabling robust monitoring and metrics collection. This feature allows users to gain insights into the performance and behavior of their job queues in real-time.

Example of Running the Service
------------------------------

Here's a quick example to demonstrate how to run the Prometheus metrics service using the prebuilt image:

```bash
# Pull the latest image
docker pull ghcr.io/janbjorge/pgq-prometheus-service:latest

# Run the service
docker run -p 8000:8000 -e PGHOST=your-postgres-host -e PGDATABASE=your-database -e PGPASSWORD=your-password -e PGUSER=your-username -e PGPORT=5432 ghcr.io/janbjorge/pgqueuer/pgq-prometheus-service
```

This command pulls the prebuilt image from GitHub Container Registry and starts the service, making the metrics endpoint available at `http://localhost:8000/metrics` for Prometheus to scrape. You can access this endpoint directly using any HTTP client to verify the metrics being exposed.
