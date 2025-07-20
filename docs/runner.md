# Runner Docker Image

PGQueuer ships a lightweight Dockerfile for running workers. Build the image and mount your application code when starting a container:

```bash
# Build the image
docker build -f docker/runner/Dockerfile -t pgqueuer-runner .

# Run the worker
docker run --rm -v $(pwd):/app pgqueuer-runner myapp.create_pgqueuer
```

You can provide the factory path via the `PGQUEUER_FACTORY` environment variable instead of a command-line argument:

```bash
docker run --rm -v $(pwd):/app -e PGQUEUER_FACTORY='myapp.create_pgqueuer' pgqueuer-runner
```
