# Caddy DuckDB Module

A Caddy server module that provides a REST API for DuckDB database operations with built-in authentication and authorization.

## Features

- **Dual Database Architecture**: Separate main database (file or in-memory) and internal auth database
- **CRUD Operations**: RESTful API for Create, Read, Update, Delete operations on tables
- **Raw SQL Queries**: Execute custom SQL queries with proper authorization
- **Multi-Format Responses**: JSON, CSV, Parquet, Apache Arrow IPC
- **Advanced Querying**: Pagination, sorting, and filtering
- **Authentication**: API key-based authentication
- **Authorization**: Role-based permissions at table level
- **Transactional Writes**: All write operations are atomic
- **SQL Injection Protection**: Query parameters and input validation
- **Configurable Timeouts**: Query timeout protection

## Quick Start

Get up and running in under 2 minutes:

```bash
# Clone the repository
git clone https://github.com/tobilg/caddyserver-duckdb-module.git
cd caddyserver-duckdb-module

# Build the server and tools
make build-all

# Initialize auth database and create an admin API key
make auth-init
make auth-add-key ROLE=admin

# Start the server
make run
```

The `auth-add-key` command will display your generated API key. Test the API:

```bash
# Replace YOUR_API_KEY with the key from setup output
curl -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT 1 AS test"}' \
     http://localhost:8080/duckdb/query
```

For more end-to-end examples including filtering, pagination, updates, and deletes, see [EXAMPLE_QUERIES.md](EXAMPLE_QUERIES.md).

For Docker deployment, see [Docker](#docker) section below.

## Building

### Prerequisites

- Go 1.24 or later
- CGO enabled (required for DuckDB bindings)
- C compiler (gcc, clang, or MSVC depending on platform)
- xcaddy (for building Caddy with custom modules)

### Important Notes

This module uses the official DuckDB Go driver (`github.com/duckdb/duckdb-go/v2`) which:
- Requires CGO to be enabled
- Downloads platform-specific binaries automatically during build
- Supports Linux (amd64, arm64), macOS (amd64, arm64), and Windows (amd64)

### Build with Go

The recommended way to build Caddy with the DuckDB module is using the provided build configuration:

```bash
# Clone the repository
git clone https://github.com/tobilg/caddyserver-duckdb-module.git
cd caddyserver-duckdb-module

# Download dependencies
go mod download

# Build Caddy with DuckDB module
go build -o caddy ./cmd/caddy
```

This will produce a `caddy` binary (~107MB) in the current directory with the DuckDB module included.

### Alternative: Build with xcaddy

You can also use xcaddy for published versions:

```bash
# Install xcaddy
go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest

# Build from GitHub (CGO_ENABLED=1 is required!)
CGO_ENABLED=1 xcaddy build --with github.com/tobilg/caddyserver-duckdb-module

# Build from local source
CGO_ENABLED=1 xcaddy build --with github.com/tobilg/caddyserver-duckdb-module=.
```

**Important:** You must set `CGO_ENABLED=1` when using xcaddy. Without it, the DuckDB C bindings won't compile and the build will fail with "undefined: bindings.Type" errors.

### Module-Only Build (For Testing)

To verify the module compiles correctly without building the full Caddy binary:

```bash
CGO_ENABLED=1 go build
```

This creates a package archive (not an executable) for testing purposes only.

### Build Troubleshooting

**Network Issues:**
If you encounter network errors downloading dependencies, ensure you have internet access. The DuckDB bindings will be downloaded automatically from GitHub on first build.

**CGO Errors:**
If you get CGO-related errors, ensure:
- CGO is enabled: `export CGO_ENABLED=1`
- A C compiler is installed (gcc on Linux, clang on macOS, MSVC on Windows)

**Platform-Specific Issues:**
- **Linux**: Install build-essential: `apt-get install build-essential` (Debian/Ubuntu)
- **macOS**: Install Xcode Command Line Tools: `xcode-select --install`
- **Windows**: Install MinGW-w64 or use Visual Studio Build Tools

## Configuration

### Caddyfile

```caddyfile
:8080 {
    route /duckdb/* {
        duckdb {
            # Main database path (optional, defaults to in-memory)
            database_path /data/main.db

            # Auth database path (required)
            auth_database_path /data/auth.db

            # Query timeout (default: 10s)
            query_timeout 10s

            # Max rows per page (default: 100)
            max_rows_per_page 100

            # Safety limit - max rows without pagination (default: 10000, 0 to disable)
            absolute_max_rows 10000

            # Number of threads (default: 4)
            threads 4

            # Access mode: read_only or read_write (default: read_write)
            access_mode read_write

            # Memory limit (optional, e.g., "4GB", "512MB". Default: 80% of RAM)
            # memory_limit 4GB

            # Enable object cache for faster repeated queries (optional, default: false)
            # enable_object_cache true

            # Temporary directory for spilling to disk (optional, uses system default if not set)
            # temp_directory /tmp/duckdb-temp
        }
    }
}
```

### JSON Configuration

```json
{
  "apps": {
    "http": {
      "servers": {
        "srv0": {
          "listen": [":8080"],
          "routes": [
            {
              "match": [{"path": ["/duckdb/*"]}],
              "handle": [
                {
                  "handler": "duckdb",
                  "database_path": "/data/main.db",
                  "auth_database_path": "/data/auth.db",
                  "query_timeout": "10s",
                  "max_rows_per_page": 100,
                  "absolute_max_rows": 10000,
                  "threads": 4,
                  "access_mode": "read_write",
                  "memory_limit": "4GB",
                  "enable_object_cache": true,
                  "temp_directory": "/tmp/duckdb-temp"
                }
              ]
            }
          ]
        }
      }
    }
  }
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database_path` | string | `:memory:` | Path to main database file. Omit for in-memory database. |
| `auth_database_path` | string | *required* | Path to authentication database (must be file-based). |
| `query_timeout` | duration | `10s` | Maximum query execution time. |
| `max_rows_per_page` | int | `100` | Default page size when pagination is used. |
| `absolute_max_rows` | int | `10000` | Safety limit - max rows without pagination. Set to `0` to disable. |
| `threads` | int | `4` | Number of threads for DuckDB query execution. |
| `access_mode` | string | `read_write` | Database access mode: `read_only` or `read_write`. |
| `memory_limit` | string | *80% of RAM* | Max memory DuckDB can use (e.g., `"4GB"`, `"512MB"`). Optional. |
| `enable_object_cache` | bool | `false` | Enable DuckDB's object cache for faster repeated queries. Optional. |
| `temp_directory` | string | *system default* | Directory for temporary files when spilling to disk. Optional. |

**Performance Tuning:**
- **`threads`**: Set to number of CPU cores for best performance
- **`memory_limit`**: Prevent DuckDB from consuming too much memory
- **`enable_object_cache`**: Useful for analytical workloads with repeated queries
- **`temp_directory`**: Important for queries that exceed memory limits

**Safety Features:**
- **`absolute_max_rows`**: Prevents accidentally large responses when pagination is not specified
- **`query_timeout`**: Protects against long-running queries

## Docker

### Pre-built Images

Official Docker images are available on Docker Hub:

```bash
docker pull tobilg/caddy-duckdb:latest
```

Available tags:
- `latest` - Latest stable release from main branch
- `x.y.z` - Specific version (e.g., `1.0.0`)
- `x.y` - Minor version (e.g., `1.0`)

Supported platforms: `linux/amd64`, `linux/arm64`

### Quick Start with Docker

```bash
# Using pre-built image
docker run -d \
  --name caddy-duckdb \
  -p 8080:8080 \
  -v $(pwd)/data:/data \
  tobilg/caddy-duckdb:latest

# Or build locally (~219MB)
docker build -t caddy-duckdb .

# Create local data directory
mkdir -p data

# Initialize auth database and create API key (run locally)
make auth-init
make auth-add-key ROLE=admin
# Save the displayed API key!

# Start the container with local data directory mounted
docker run -d \
  --name caddy-duckdb \
  -p 8080:8080 \
  -v $(pwd)/data:/data \
  caddy-duckdb

# Verify it's running
curl http://localhost:8080/duckdb/health
# {"status":"ok"}

# Test with your API key
curl -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT 1 AS test"}' \
     http://localhost:8080/duckdb/query
```

### Using Docker Compose

```bash
# Create local data directory, init auth DB, and create API key
mkdir -p data
make auth-init
make auth-add-key ROLE=admin

# Start with docker-compose
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### Health Check

The container includes a health check endpoint at `/{DUCKDB_ROUTE_PREFIX}/health` (default: `/duckdb/health`):

```bash
curl http://localhost:8080/duckdb/health
# {"status":"ok"}
```

This endpoint requires no authentication and is used by Docker's HEALTHCHECK.

### Environment Variables

All settings can be configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKDB_PORT` | `8080` | Server port |
| `DUCKDB_ROUTE_PREFIX` | `/duckdb` | API route prefix |
| `DUCKDB_DATABASE_PATH` | `/data/main.db` | Main database path |
| `DUCKDB_AUTH_DATABASE_PATH` | `/data/auth.db` | Auth database path |
| `DUCKDB_THREADS` | `4` | Number of query threads |
| `DUCKDB_MEMORY_LIMIT` | *(80% RAM)* | Memory limit (e.g., `4GB`) |
| `DUCKDB_QUERY_TIMEOUT` | `10s` | Query timeout |
| `DUCKDB_ACCESS_MODE` | `read_write` | `read_only` or `read_write` |
| `DUCKDB_MAX_ROWS_PER_PAGE` | `100` | Default pagination size |
| `DUCKDB_ABSOLUTE_MAX_ROWS` | `10000` | Max rows without pagination |

Example with custom settings:

```bash
docker run -d \
  --name caddy-duckdb \
  -p 8080:8080 \
  -e DUCKDB_THREADS=8 \
  -e DUCKDB_MEMORY_LIMIT=4GB \
  -e DUCKDB_QUERY_TIMEOUT=30s \
  -e DUCKDB_ROUTE_PREFIX=/api/v1 \
  -v $(pwd)/data:/data \
  caddy-duckdb
```

With a custom route prefix, endpoints become `/api/v1/health`, `/api/v1/query`, `/api/v1/api/{table}`, etc.

### Custom Caddyfile

Mount your own Caddyfile for advanced configuration:

```bash
docker run -d \
  --name caddy-duckdb \
  -p 8080:8080 \
  -v $(pwd)/data:/data \
  -v $(pwd)/my-caddyfile:/etc/caddy/Caddyfile:ro \
  caddy-duckdb
```

## Quick Start Guide

### Creating Tables

Use the **raw SQL query endpoint** to create tables:

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, email VARCHAR, age INTEGER)"
  }'
```

**Requirements:**
- Requires `can_query` permission (admin role by default)
- Endpoint: `POST /duckdb/query`

### Adding Records

You have **two options**:

#### Option 1: CRUD API (Recommended for simple inserts)

```bash
curl -X POST http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 1
}
```

- Requires `can_create` permission
- Endpoint: `POST /duckdb/api/{table}`
- Automatically handles parameterized queries

#### Option 2: Raw SQL Query (For complex inserts)

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
    "params": ["Jane Doe", "jane@example.com", 28]
  }'
```

- Requires `can_query` permission
- Useful for batch inserts or complex SQL statements
- Supports parameterized queries for safety

### Key Points

- **Authentication**: All endpoints require the `X-API-Key` header
- **Permissions**: Role-based access control (admin, editor, reader)
- **Security**: Uses parameterized queries and input validation to prevent SQL injection
- **Transactions**: All write operations use transactions for atomicity

The CRUD API (`/duckdb/api/{table}`) is simpler for basic operations, while the raw SQL endpoint (`/duckdb/query`) gives you full DuckDB SQL power for complex operations.

## Authentication & Authorization

### Auth Database Setup

The auth database must be initialized **before** starting the server. Use the `auth-db` CLI tool:

```bash
# Build the CLI tool first (or use make targets which build automatically)
make build-tools

# Initialize auth database with default roles (admin, editor, reader)
make auth-init

# Or use the CLI directly
./tools/auth-db init -d /path/to/auth.db

# View all available commands
./tools/auth-db --help
```

### Built-in Roles

- **admin**: Full CRUD access + raw SQL queries on all tables
- **editor**: CRUD access on all tables, no raw SQL
- **reader**: Read-only access on all tables

### Creating API Keys

Use the `auth-db` CLI tool or Make targets:

```bash
# Using Make (recommended)
make auth-add-key ROLE=admin

# Using CLI directly
./tools/auth-db key add -d /path/to/auth.db -r admin

# With custom key (instead of auto-generated)
./tools/auth-db key add -d /path/to/auth.db -r admin -k my-secret-key

# With expiration
./tools/auth-db key add -d /path/to/auth.db -r admin -e 2025-12-31T23:59:59Z
```

### Managing API Keys

```bash
# List all API keys
make auth-list-keys

# Remove an API key
./tools/auth-db key remove -d /path/to/auth.db -k <api-key>
```

### Custom Roles

```bash
# Create a custom role
make auth-add-role NAME=analyst DESC="Data analyst with read and query access"

# Or using CLI
./tools/auth-db role add -d /path/to/auth.db -n analyst --desc "Data analyst"

# Grant permissions (operations: c=create, r=read, u=update, d=delete, q=query)
make auth-add-perm ROLE=analyst TABLE=reports OPS=r,q

# Or using CLI
./tools/auth-db permission add -d /path/to/auth.db -r analyst -t reports -o r,q

# Grant all CRUD operations (no raw query)
./tools/auth-db permission add -d /path/to/auth.db -r analyst -t "*" -o crud

# List all roles and permissions
make auth-list-roles
make auth-list-perms
```

### Auth Database Info

```bash
# Show auth database statistics
make auth-info
```

## API Endpoints

### CRUD Operations

Base path: `/duckdb/api/{table}`

All requests require the `X-API-Key` header.

#### Create (POST)

```bash
curl -X POST http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }'
```

Response:
```json
{
  "success": true,
  "rows_affected": 1
}
```

#### Read (GET)

```bash
# Basic query
curl http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: your-api-key"

# With pagination
curl "http://localhost:8080/duckdb/api/users?page=1&limit=50" \
  -H "X-API-Key: your-api-key"

# With filters
curl "http://localhost:8080/duckdb/api/users?filter=age:gt:18,status:eq:active" \
  -H "X-API-Key: your-api-key"

# With sorting
curl "http://localhost:8080/duckdb/api/users?sort=created_at:desc,name:asc" \
  -H "X-API-Key: your-api-key"

# Combined
curl "http://localhost:8080/duckdb/api/users?page=1&limit=20&filter=age:gt:18&sort=name:asc" \
  -H "X-API-Key: your-api-key"
```

Response:
```json
{
  "data": [
    {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com",
      "age": 30
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total_rows": 1,
    "total_pages": 1
  }
}
```

##### HATEOAS Navigation Links

Add `links=true` to include navigation links in paginated responses:

```bash
curl "http://localhost:8080/duckdb/api/users?page=2&limit=10&links=true" \
  -H "X-API-Key: your-api-key"
```

Response with links:
```json
{
  "data": [...],
  "pagination": {
    "page": 2,
    "limit": 10,
    "total_rows": 100,
    "total_pages": 10
  },
  "_links": {
    "self": "/duckdb/api/users?limit=10&page=2&links=true",
    "first": "/duckdb/api/users?limit=10&page=1&links=true",
    "prev": "/duckdb/api/users?limit=10&page=1&links=true",
    "next": "/duckdb/api/users?limit=10&page=3&links=true",
    "last": "/duckdb/api/users?limit=10&page=10&links=true"
  }
}
```

Link types:
- `self`: Current page URL (always included)
- `first`: First page URL (always included)
- `last`: Last page URL (when total_pages > 0)
- `prev`: Previous page URL (when page > 1)
- `next`: Next page URL (when page < total_pages)

##### Filter Operators

- `eq`: Equal
- `ne`: Not equal
- `gt`: Greater than
- `gte`: Greater than or equal
- `lt`: Less than
- `lte`: Less than or equal
- `like`: SQL LIKE pattern
- `in`: IN clause (use pipe `|` to separate values)

Example: `filter=status:in:active|pending`

#### Update (PUT)

```bash
curl -X PUT http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "where": {
      "id": 1
    },
    "set": {
      "age": 31,
      "updated_at": "2025-01-01T00:00:00Z"
    }
  }'
```

Response:
```json
{
  "success": true,
  "rows_affected": 1
}
```

#### Delete (DELETE)

```bash
curl -X DELETE "http://localhost:8080/duckdb/api/users?where=id:eq:1" \
  -H "X-API-Key: your-api-key"
```

Response:
```json
{
  "success": true,
  "rows_affected": 1
}
```

### Raw SQL Queries

Endpoint: `/duckdb/query` — Requires `can_query` permission (admin role by default).

**POST Method** (supports parameterized queries):

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users WHERE age > ?", "params": [18]}'
```

**GET Method** (read-only, bookmarkable):

```bash
# Pattern: /duckdb/query/{urlEncodedSQL}/result.{format}
curl "http://localhost:8080/duckdb/query/SELECT%20*%20FROM%20users/result.json" \
  -H "X-API-Key: your-api-key"

# With jq for URL encoding
curl "http://localhost:8080/duckdb/query/$(echo 'SELECT * FROM users' | jq -sRr @uri)/result.csv" \
  -H "X-API-Key: your-api-key" -o users.csv
```

GET is limited to SELECT, SHOW, DESCRIBE, EXPLAIN queries.

**Response** (same format as CRUD API):
```json
{
  "data": [
    {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30}
  ]
}
```

### Response Formats

Both `/api` and `/query` endpoints support multiple output formats:

| Format | Accept Header | File module | Best For |
|--------|---------------|----------------|----------|
| JSON | `application/json` | `.json` | Web APIs, debugging |
| CSV | `text/csv` | `.csv` | Spreadsheets, simple exports |
| Parquet | `application/parquet` | `.parquet` | Analytics, data lakes (5-10x smaller) |
| Arrow IPC | `application/vnd.apache.arrow.stream` | `.arrow` | Data pipelines, zero-copy transfers |

```bash
# Using Accept header (CRUD or POST query)
curl http://localhost:8080/duckdb/api/users -H "X-API-Key: key" -H "Accept: text/csv"
curl http://localhost:8080/duckdb/api/users -H "X-API-Key: key" -H "Accept: application/parquet" -o data.parquet

# Using file module (GET query)
curl "http://localhost:8080/duckdb/query/SELECT%20*%20FROM%20users/result.parquet" -H "X-API-Key: key" -o data.parquet
```

**Reading exported files in Python:**
```python
import pyarrow.parquet as pq
import pyarrow.ipc as ipc

# Parquet
df = pq.read_table('data.parquet').to_pandas()

# Arrow IPC
with open('data.arrow', 'rb') as f:
    df = ipc.open_stream(f).read_all().to_pandas()
```

## Security Features

1. **SQL Injection Protection**: All queries use parameterized statements
2. **Input Validation**: Table and column names are sanitized
3. **Internal Table Protection**: Auth tables cannot be accessed via API (hardened with SQL comment stripping and word-boundary matching)
4. **API Key Hashing**: Keys are stored as bcrypt hashes
5. **Transactional Writes**: All modifications are atomic
6. **Query Timeouts**: Prevents long-running queries
7. **Role-Based Access**: Fine-grained permissions at table level
8. **Request ID Tracing**: All requests include a unique request ID for distributed tracing and log correlation

### Rate Limiting

Rate limiting is intentionally **not** implemented in this module. Caddy has excellent rate limiting plugins that should be used instead:

```caddyfile
:8080 {
    # Use Caddy's rate_limit directive (requires caddy-rate-limit plugin)
    rate_limit {
        zone duckdb_api {
            key {remote_host}
            events 100
            window 1m
        }
    }

    route /duckdb/* {
        duckdb {
            # ... your config
        }
    }
}
```

Recommended plugins:
- [caddy-rate-limit](https://github.com/mholt/caddy-ratelimit) - Token bucket rate limiting
- [caddy-security](https://github.com/greenpau/caddy-security) - Comprehensive security including rate limiting

This separation of concerns allows you to configure rate limiting consistently across all your Caddy routes, not just the DuckDB endpoints.

### Request ID Tracing

All API requests include a unique request ID for distributed tracing and log correlation:

**How it works:**
- If you provide an `X-Request-ID` header, the API uses your ID
- If no header is provided, a UUID v4 is automatically generated
- The request ID is always returned in the `X-Request-ID` response header
- All log entries include the request ID for correlation

**Example:**
```bash
curl -H "X-API-Key: your-api-key" \
     -H "X-Request-ID: my-trace-123" \
     http://localhost:8080/duckdb/api/users
```

Response headers:
```
X-Request-ID: my-trace-123
```

The request ID is only in the response header, not in the JSON body. This keeps response payloads clean while still providing full traceability.

This enables you to:
- Correlate client requests with server-side logs
- Track requests across distributed systems
- Debug issues by searching logs with the request ID
- Build observability dashboards with request tracing

### OpenAPI Specification

A complete OpenAPI 3.0 specification is available at `/duckdb/openapi.json`. This endpoint is publicly accessible (no authentication required) to allow easy access to API documentation.

```bash
# Access the OpenAPI specification
curl http://localhost:8080/duckdb/openapi.json
```

**Features:**
- Complete API documentation for all CRUD and query endpoints
- Request/response schema definitions with examples
- Security scheme documentation (API key authentication)
- All filter operators, pagination parameters, and response formats documented
- HATEOAS links and request ID tracing documented

**Usage with API tools:**

```bash
# Download the spec for use with Swagger UI
curl http://localhost:8080/duckdb/openapi.json -o openapi.json

# Import into Postman, Insomnia, or other API tools
# Or generate client SDKs using openapi-generator-cli
npx @openapitools/openapi-generator-cli generate -i openapi.json -g python -o ./python-client
```

**Integration with Swagger UI:**

You can serve Swagger UI alongside your API for interactive documentation:

```caddyfile
:8080 {
    # Serve Swagger UI (download swagger-ui-dist to /var/www/swagger)
    handle /swagger/* {
        root * /var/www/swagger
        file_server
    }

    route /duckdb/* {
        duckdb {
            database_path /data/main.db
            auth_database_path /data/auth.db
        }
    }
}
```

## Example Usage

### 1. Setup Auth Database

Initialize the auth database and create an API key before starting the server:

```bash
# Initialize auth database with default roles
make auth-init

# Create an admin API key
make auth-add-key ROLE=admin
# Output: ✓ API key created successfully!
#         API Key: <your-generated-key>
#         Role: admin
```

### 2. Start the Server

```bash
make run
```

### 3. Create Tables

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: my-api-key-id" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, email VARCHAR, age INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
  }'
```

### 4. Use CRUD Operations

See the API Endpoints section above for examples.

## Development

### Project Structure

```
caddyserver-duckdb-module/
├── module.go              # Main Caddy module
├── config.go              # Configuration structs
├── database/
│   ├── manager.go         # Database connection manager
│   └── operations.go      # CRUD operations
├── auth/
│   ├── models.go          # Auth data structures
│   ├── authorizer.go      # Authorization logic
│   └── middleware.go      # Auth middleware
├── handlers/
│   ├── crud.go            # CRUD handlers
│   ├── query.go           # Query handler
│   ├── params.go          # Parameter parsing
│   └── openapi.go         # OpenAPI 3.0 specification handler
├── formats/
│   ├── json.go            # JSON formatter
│   ├── csv.go             # CSV formatter
│   ├── parquet.go         # Apache Parquet formatter
│   └── arrow.go           # Apache Arrow IPC formatter
└── examples/
    └── Caddyfile          # Example configuration
```

### Make Targets

The project includes a Makefile for common development tasks:

| Target | Description |
|--------|-------------|
| `make setup` | Full setup: check prerequisites, build, init auth DB |
| `make build` | Build the Caddy binary |
| `make build-tools` | Build the auth-db CLI tool |
| `make run` | Build and run with example Caddyfile |
| `make test` | Run all tests |
| `make fmt` | Format code |
| `make vet` | Run go vet |
| `make lint` | Run fmt + vet |
| `make clean` | Remove build artifacts |
| `make docker-build` | Build Docker image |
| `make docker-run` | Run with docker-compose |
| `make help` | Show all available targets |

**Auth Database Management:**

| Target | Description |
|--------|-------------|
| `make auth-init` | Initialize auth database with default roles |
| `make auth-add-key ROLE=<role>` | Add a new API key for a role |
| `make auth-remove-key KEY=<key>` | Remove an API key |
| `make auth-list-keys` | List all API keys |
| `make auth-add-role NAME=<name>` | Add a new role |
| `make auth-remove-role NAME=<name> [FORCE=1]` | Remove a role |
| `make auth-list-roles` | List all roles |
| `make auth-add-perm ROLE=<role> TABLE=<table> OPS=<ops>` | Add permission |
| `make auth-remove-perm ROLE=<role> TABLE=<table>` | Remove a permission |
| `make auth-list-perms` | List all permissions |
| `make auth-info` | Show auth database statistics |

### Running Tests

```bash
make test
# or directly:
go test ./...
```

## Concurrency and Multi-User Support

### Concurrent Operations

This module is designed for **single-process concurrent access** and handles multiple simultaneous requests efficiently:

**What Works Well:**
- **Concurrent Reads**: Multiple users can read simultaneously without blocking each other
- **Concurrent Inserts**: Multiple users can insert records into the same or different tables
- **Mixed Read/Write**: Reads are never blocked by writes thanks to DuckDB's MVCC (Multi-Version Concurrency Control)
- **Connection Pooling**: Configured to support `threads * 2` concurrent connections for optimal throughput

**Transaction Conflict Handling:**
- Write operations (INSERT/UPDATE/DELETE) automatically retry on conflicts with exponential backoff (up to 3 attempts)
- When multiple users update the same row simultaneously, one succeeds and others retry automatically
- Conflicts are rare for typical workloads but handled gracefully when they occur

### Important Limitations

**Single-Process Only:**
DuckDB is designed for single-process access. This module works within one Caddy server instance but **does not support**:
- Multiple Caddy instances writing to the same database file
- Distributed deployments with shared database files
- Multi-process concurrent writes

**For Production Multi-Instance Deployments:**
If you need to run multiple Caddy instances:
1. Use **read-only replicas**: Configure additional instances with `access_mode read_only`
2. Use a **single writer** instance for all write operations
3. Consider **horizontal partitioning**: Different instances handle different databases
4. Use **external databases**: For true multi-writer scenarios, delegate writes to PostgreSQL, MySQL, or similar RDBMS

### Performance Characteristics

- **Read Performance**: Excellent - scales linearly with configured threads
- **Write Performance**: Good for typical web workloads with automatic conflict resolution
- **High-Contention Scenarios**: If many users frequently update the same rows, consider application-level locking or optimistic locking patterns

## Limitations

- **Multi-Process Writes**: Not supported - only one Caddy instance can write to a database file
- Internal auth tables (`api_keys`, `roles`, `permissions`) cannot be queried via the API
- DELETE operations require a WHERE clause for safety
- Network connectivity required for initial dependency download

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

[MIT License](LICENSE)

## Credits

Built with:
- [Caddy](https://caddyserver.com/)
- [DuckDB](https://duckdb.org/)
- [duckdb-go](https://github.com/duckdb/duckdb-go)
