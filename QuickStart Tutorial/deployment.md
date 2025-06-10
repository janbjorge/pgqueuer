# PgQueuer Deployment Guide

This document provides instructions for deploying PgQueuer in production environments, including Docker setup and configuration.

## Prerequisites

Before deploying PgQueuer, ensure you have the following:

- Docker installed on your machine.
- Docker Compose installed for managing multi-container applications.
- Access to a PostgreSQL database.

## Docker Setup

To deploy PgQueuer using Docker, you will need to create a `docker-compose.yml` file that defines the services required for your application. Below is an example configuration.

### Example `docker-compose.yml`

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: pgqueuer
      POSTGRES_USER: pguser
      POSTGRES_PASSWORD: pgpass
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pguser -d pgqueuer"]

  pgqueuer:
    build: .
    command: ["pgq", "run", "consumer:main"]
    depends_on:
      - postgres
    environment:
      DATABASE_URL: "postgresql://pguser:pgpass@postgres:5432/pgqueuer"
    ports:
      - "8000:8000"

volumes:
  postgres_data:
```

## Building and Running the Application

1. **Build the Docker images**: Navigate to the directory containing your `docker-compose.yml` file and run the following command:

   ```bash
   docker-compose build
   ```

2. **Start the services**: Once the build is complete, start the services using:

   ```bash
   docker-compose up
   ```

3. **Accessing the application**: After the services are up and running, you can access PgQueuer through the defined ports.

## Health Checks

The Docker configuration includes a health check for the PostgreSQL service to ensure that it is ready to accept connections before PgQueuer starts. You can monitor the health status using Docker commands.

## Conclusion

This guide provides a basic setup for deploying PgQueuer using Docker. For more advanced configurations, consider customizing the Dockerfile and docker-compose settings to suit your production needs.