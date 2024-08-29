Prometheus Metrics Integration
==============================

pgqueuer now includes an integration with Prometheus, enabling robust monitoring and metrics collection. This feature allows users to gain insights into the performance and behavior of their job queues in real-time.

Running the Metrics Service
---------------------------

To run the Prometheus metrics service, use the provided docker-compose file located in the `tools/prometheus` directory. This method simplifies the setup process by using Docker to encapsulate all dependencies and configurations needed.

To start the service, navigate to the appropriate directory and run:

```bash
docker compose -f tools/prometheus/docker-compose.yml up
```

This command launches the metrics service, making it available at `http://localhost:8000/metrics` for Prometheus to scrape. You can access the metrics endpoint directly using any HTTP client to view the metrics being exposed.

No Prebuilt Docker Image
------------------------

Currently, there is no prebuilt Docker image available in a registry. The system relies on building the Docker image locally using the Dockerfile provided in the `tools/prometheus` directory. 

Docker Compose Configuration
----------------------------

Below is the docker-compose configuration needed to deploy the Prometheus metrics service. Adjust the environment variables according to your PostgreSQL setup.

```yaml
services:
    pgq-prometheus:
    working_dir: /src
    command: uvicorn --factory tools.prometheus.prometheus:create_app --host 0.0.0.0 --port 8000
    build:
        context: ../../
        dockerfile: tools/prometheus/Dockerfile
    ports:
        - "8000:8000"
    environment:
        PGHOST: ${PGHOST}
        PGDATABASE: ${PGDATABASE}
        PGPASSWORD: ${PGPASSWORD}
        PGUSER: ${PGUSER}
        PGPORT: ${PGPORT}
```

Endpoint Usage
--------------

Once deployed, the Prometheus metrics service exposes an endpoint at `/metrics`, which Prometheus can scrape to collect metrics such as queue lengths, job statuses, and processing times. This endpoint serves as a critical tool for monitoring the efficiency and reliability of the pgqueuer system.

To access the metrics, point your Prometheus instance to `http://<your-host>:8000/metrics`.
