# Makefile for Caddy DuckDB Extension
#
# Usage: make [target]
# Run 'make help' to see all available targets

# Configuration
BINARY_NAME := caddy
TOOLS_DIR := tools
DATA_DIR := /tmp/data
AUTH_DB := $(DATA_DIR)/auth.db

# Enable CGO for DuckDB bindings
export CGO_ENABLED := 1

# Colors for terminal output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

.PHONY: all build build-tools test test-verbose run run-json setup clean deps tidy fmt vet lint install-hooks help \
	auth-init auth-add-key auth-remove-key auth-list-keys auth-list-roles auth-list-perms auth-info auth-add-role auth-remove-role auth-add-perm auth-remove-perm

# Default target
all: help

## Build targets

build: ## Build the Caddy binary with DuckDB extension
	@echo "$(GREEN)Building $(BINARY_NAME)...$(NC)"
	go build -o $(BINARY_NAME) ./cmd/caddy
	@echo "$(GREEN)✓ Built $(BINARY_NAME)$(NC)"

build-tools: ## Build the auth-db CLI tool
	@echo "$(GREEN)Building tools...$(NC)"
	go build -o $(TOOLS_DIR)/auth-db ./$(TOOLS_DIR)/auth-db-src/
	@echo "$(GREEN)✓ Built $(TOOLS_DIR)/auth-db$(NC)"

build-all: build build-tools ## Build all binaries

## Test targets

test: ## Run all tests
	@echo "$(GREEN)Running tests...$(NC)"
	go test ./...

test-verbose: ## Run all tests with verbose output
	@echo "$(GREEN)Running tests (verbose)...$(NC)"
	go test -v ./...

test-coverage: ## Run tests with coverage report
	@echo "$(GREEN)Running tests with coverage...$(NC)"
	go test -cover ./...

## Run targets

run: build ## Build and run with example Caddyfile
	@echo "$(GREEN)Starting Caddy server...$(NC)"
	./$(BINARY_NAME) run --config examples/Caddyfile --adapter caddyfile

run-json: build ## Build and run with JSON config
	@echo "$(GREEN)Starting Caddy server (JSON config)...$(NC)"
	./$(BINARY_NAME) run --config test-config.json

## Setup targets

setup: ## Full development setup (create dirs, build, generate API key)
	@echo "$(GREEN)Running development setup...$(NC)"
	@./scripts/setup.sh

init-dirs: ## Create required data directories
	@echo "$(GREEN)Creating data directories...$(NC)"
	@mkdir -p $(DATA_DIR)
	@echo "$(GREEN)✓ Created $(DATA_DIR)$(NC)"

## Auth database management targets

auth-init: build-tools init-dirs ## Initialize auth database with default roles
	@if [ -f "$(AUTH_DB)" ]; then \
		echo "$(YELLOW)Auth database already exists at $(AUTH_DB)$(NC)"; \
		echo "$(YELLOW)Use 'make auth-info' to see current state$(NC)"; \
	else \
		./$(TOOLS_DIR)/auth-db init -d $(AUTH_DB); \
	fi

auth-add-key: build-tools ## Add a new API key (usage: make auth-add-key ROLE=admin)
	@if [ -z "$(ROLE)" ]; then \
		echo "$(RED)Error: ROLE is required. Usage: make auth-add-key ROLE=admin$(NC)"; \
		exit 1; \
	fi
	@./$(TOOLS_DIR)/auth-db key add -d $(AUTH_DB) -r $(ROLE)

auth-remove-key: build-tools ## Remove an API key (usage: make auth-remove-key KEY=<api-key>)
	@if [ -z "$(KEY)" ]; then \
		echo "$(RED)Error: KEY is required. Usage: make auth-remove-key KEY=<api-key>$(NC)"; \
		exit 1; \
	fi
	@./$(TOOLS_DIR)/auth-db key remove -d $(AUTH_DB) -k "$(KEY)"

auth-list-keys: build-tools ## List all API keys
	@./$(TOOLS_DIR)/auth-db key list -d $(AUTH_DB)

auth-list-roles: build-tools ## List all roles
	@./$(TOOLS_DIR)/auth-db role list -d $(AUTH_DB)

auth-list-perms: build-tools ## List all permissions
	@./$(TOOLS_DIR)/auth-db permission list -d $(AUTH_DB)

auth-info: build-tools ## Show auth database statistics
	@./$(TOOLS_DIR)/auth-db info -d $(AUTH_DB)

auth-add-role: build-tools ## Add a new role (usage: make auth-add-role NAME=analyst DESC="Data analyst role")
	@if [ -z "$(NAME)" ]; then \
		echo "$(RED)Error: NAME is required. Usage: make auth-add-role NAME=analyst$(NC)"; \
		exit 1; \
	fi
	@./$(TOOLS_DIR)/auth-db role add -d $(AUTH_DB) -n "$(NAME)" --desc "$(DESC)"

auth-remove-role: build-tools ## Remove a role (usage: make auth-remove-role NAME=analyst [FORCE=1])
	@if [ -z "$(NAME)" ]; then \
		echo "$(RED)Error: NAME is required. Usage: make auth-remove-role NAME=analyst$(NC)"; \
		exit 1; \
	fi
	@if [ "$(FORCE)" = "1" ]; then \
		./$(TOOLS_DIR)/auth-db role remove -d $(AUTH_DB) -n "$(NAME)" --force; \
	else \
		./$(TOOLS_DIR)/auth-db role remove -d $(AUTH_DB) -n "$(NAME)"; \
	fi

auth-add-perm: build-tools ## Add permission (usage: make auth-add-perm ROLE=analyst TABLE=users OPS=r)
	@if [ -z "$(ROLE)" ] || [ -z "$(TABLE)" ] || [ -z "$(OPS)" ]; then \
		echo "$(RED)Error: ROLE, TABLE, and OPS are required$(NC)"; \
		echo "$(RED)Usage: make auth-add-perm ROLE=analyst TABLE=users OPS=c,r,u,d$(NC)"; \
		exit 1; \
	fi
	@./$(TOOLS_DIR)/auth-db permission add -d $(AUTH_DB) -r "$(ROLE)" -t "$(TABLE)" -o "$(OPS)"

auth-remove-perm: build-tools ## Remove a permission (usage: make auth-remove-perm ROLE=analyst TABLE=users)
	@if [ -z "$(ROLE)" ] || [ -z "$(TABLE)" ]; then \
		echo "$(RED)Error: ROLE and TABLE are required$(NC)"; \
		echo "$(RED)Usage: make auth-remove-perm ROLE=analyst TABLE=users$(NC)"; \
		exit 1; \
	fi
	@./$(TOOLS_DIR)/auth-db permission remove -d $(AUTH_DB) -r "$(ROLE)" -t "$(TABLE)"

## Dependency targets

deps: ## Download Go module dependencies
	@echo "$(GREEN)Downloading dependencies...$(NC)"
	go mod download
	@echo "$(GREEN)✓ Dependencies downloaded$(NC)"

tidy: ## Tidy Go modules
	@echo "$(GREEN)Tidying modules...$(NC)"
	go mod tidy
	@echo "$(GREEN)✓ Modules tidied$(NC)"

## Code quality targets

fmt: ## Format Go code
	@echo "$(GREEN)Formatting code...$(NC)"
	go fmt ./...
	@echo "$(GREEN)✓ Code formatted$(NC)"

vet: ## Run go vet
	@echo "$(GREEN)Running go vet...$(NC)"
	go vet ./...
	@echo "$(GREEN)✓ Vet passed$(NC)"

lint: fmt vet ## Run all linters (fmt + vet)
	@echo "$(GREEN)✓ All linting passed$(NC)"

install-hooks: ## Install git pre-commit hooks
	@echo "$(GREEN)Installing pre-commit hook...$(NC)"
	@cp scripts/pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "$(GREEN)✓ Pre-commit hook installed$(NC)"

## Cleanup targets

clean: ## Remove build artifacts and test data
	@echo "$(YELLOW)Cleaning up...$(NC)"
	@rm -f $(BINARY_NAME)
	@rm -f $(TOOLS_DIR)/auth-db
	@rm -rf data/ test_data/
	@echo "$(GREEN)✓ Cleaned$(NC)"

clean-all: clean ## Remove all artifacts including /tmp/data
	@echo "$(YELLOW)Removing $(DATA_DIR)...$(NC)"
	@rm -rf $(DATA_DIR)
	@echo "$(GREEN)✓ All cleaned$(NC)"

## Docker targets (placeholders - will be implemented in Phase 2)

docker-build: ## Build Docker image
	@echo "$(GREEN)Building Docker image...$(NC)"
	docker build -t caddy-duckdb .

docker-run: ## Run with docker-compose
	@echo "$(GREEN)Starting with docker-compose...$(NC)"
	docker-compose up -d

docker-stop: ## Stop docker-compose services
	@echo "$(YELLOW)Stopping docker-compose...$(NC)"
	docker-compose down

docker-logs: ## Show docker-compose logs
	docker-compose logs -f

docker-shell: ## Open shell in running container
	docker-compose exec caddy-duckdb /bin/sh

docker-clean: docker-stop ## Remove Docker containers and images
	@echo "$(YELLOW)Removing Docker artifacts...$(NC)"
	docker-compose rm -f
	docker rmi caddy-duckdb || true
	@echo "$(GREEN)✓ Docker cleaned$(NC)"

## Help

help: ## Show this help message
	@echo "Caddy DuckDB Extension - Development Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "Quick start:"
	@echo "  make setup    # First-time setup"
	@echo "  make run      # Build and run server"
	@echo ""
