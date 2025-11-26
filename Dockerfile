# Dockerfile for Caddy DuckDB Extension
#
# Multi-stage build:
# - Stage 1: Build the Go binary with CGO
# - Stage 2: Create minimal runtime image
#
# The /data volume stores both main.db and auth.db databases.
# Mount this volume for data persistence.
#
# Usage:
#   docker build -t caddy-duckdb .
#   docker run -p 8080:8080 -v $(pwd)/data:/data caddy-duckdb
#
# Generate API key (run locally, not in container):
#   go build -o tools/create-api-key ./tools/create-api-key.go
#   ./tools/create-api-key -db ./data/auth.db

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM golang:1.24-bookworm AS builder

# Install build dependencies for CGO and DuckDB
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy go.mod and go.sum first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the main binary with optimizations
# -ldflags="-s -w" strips debug info and symbol table (~20% smaller)
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o caddy ./cmd/caddy

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM debian:bookworm-slim

# Install minimal runtime dependencies
# - ca-certificates: Required for HTTPS/TLS connections
# - curl: Required for health checks (smaller than wget)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r caddy && useradd -r -g caddy caddy

# Create directories with correct ownership upfront
RUN mkdir -p /data /app /etc/caddy && \
    chown -R caddy:caddy /data /app /etc/caddy

WORKDIR /app

# Copy binary from builder with correct ownership (avoids layer duplication)
COPY --from=builder --chown=caddy:caddy /build/caddy .

# Copy Docker-specific configuration (uses /data volume paths)
COPY --chown=caddy:caddy examples/Caddyfile.docker /etc/caddy/Caddyfile

# Labels
LABEL org.opencontainers.image.title="Caddy DuckDB Module" \
      org.opencontainers.image.description="Caddy server with DuckDB REST API" \
      org.opencontainers.image.source="https://github.com/tobilg/caddyserver-duckdb-module" \
      org.opencontainers.image.licenses="MIT"

# Expose default port
EXPOSE 8080

# Volume for persistent data
VOLUME ["/data"]

# Health check using the dedicated health endpoint
# Note: Uses default values; docker-compose.yml overrides this with env var support
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -fsS http://localhost:${DUCKDB_PORT:-8080}${DUCKDB_ROUTE_PREFIX:-/duckdb}/health || exit 1

# Run as non-root user
USER caddy

# Default command
ENTRYPOINT ["/app/caddy"]
CMD ["run", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"]
