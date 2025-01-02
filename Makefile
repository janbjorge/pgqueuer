COMPOSE_FILE = docker-compose.yml

.PHONY: help build up populate test down clean

help:
	@echo "============================="
	@echo " PGQueuer Makefile Commands"
	@echo "============================="
	@echo " build     Build Docker images"
	@echo " up        Start pgq container in background"
	@echo " populate  Run the populate service"
	@echo " test      Bring everything up, run tests, then exit"
	@echo " down      Stop and remove containers"
	@echo " clean     Remove containers, networks, volumes, and images"


build:
	docker compose -f $(COMPOSE_FILE) build

up:
	docker compose -f $(COMPOSE_FILE) up -d db

populate:
	docker compose -f $(COMPOSE_FILE) run --rm populate

test:
	docker compose -f $(COMPOSE_FILE) run --rm test

down:
	docker compose -f $(COMPOSE_FILE) down

clean:
	docker compose -f $(COMPOSE_FILE) down --rmi all --volumes --remove-orphans
