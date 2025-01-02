# Development

This project provides a Docker Compose configuration for local development, along with a Makefile that wraps common operations. You can use the Makefile for a quick start, or directly call Docker Compose if you need more flexibility.

## Working with the Makefile

### `make build`
Invokes Docker Compose to build all images defined in the `docker-compose.yml` file. This ensures that the application and its dependencies are ready to be spun up.

### `make up`
Starts the database container in the background. This makes your database service available for any subsequent commands.

### `make populate`
Runs a one-off container that installs PGQueuer into the database. After the database is up, you can run this command to provision any needed schemas or tables.

### `make test`
Brings up the necessary containers, executes the test suite, and then exits. This is handy if you want a quick validation of your setup without leaving containers running afterward.

### `make down`
Stops and removes the containers that were previously started. If you need to free up system resources while keeping the images built, this is the command to use.

### `make clean`
Performs a more thorough cleanup by removing containers, networks, volumes, and images created by the project. This is useful when you want to reset your environment completely.

## Using Docker Compose Directly

All of these Makefile targets delegate work to Docker Compose. If you prefer controlling the process yourself or passing extra flags, you can run commands like `docker compose -f docker-compose.yml build` or `docker compose -f docker-compose.yml up -d db`. Running Docker Compose commands directly can be particularly useful for advanced scenarios that extend beyond the defaults offered by the Makefile.