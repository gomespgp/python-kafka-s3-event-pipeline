.PHONY: up down logs restart clean build test help

# Default target when just typing 'make'
.DEFAULT_GOAL := help

help: ## Show help for each target
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## Start all Docker containers in background
	docker compose -f .docker/docker-compose.yaml up -d

down: ## Stop and remove containers and networks
	docker compose -f .docker/docker-compose.yaml down

logs: ## Follow logs for all services in real-time
	docker compose -f .docker/docker-compose.yaml logs -f

restart: ## Restart a specific container (or all if empty). Usage: make restart service=kafka
	docker compose -f .docker/docker-compose.yaml restart $(service)

clean: ## Stop services and delete all persistent volumes (Fresh Start)
	docker compose -f .docker/docker-compose.yaml down -v

build: ## Build the docker-compose file
	docker compose -f .docker/docker-compose.yaml up -d --build

test: ## Run producer test suite with pytest
	PYTHONDONTWRITEBYTECODE=1 pytest producer/tests -v
