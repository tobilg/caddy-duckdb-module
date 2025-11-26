# Example Queries

This document provides end-to-end curl examples for all API endpoints. Each section builds on the previous one, demonstrating a complete workflow.

**Prerequisites**: Make sure the server is running (`make run`) and you have an API key. Replace `YOUR_API_KEY` with your actual key in all examples.

## Table of Contents

- [Setup: Create a Table](#setup-create-a-table)
- [CRUD API Examples](#crud-api-examples)
  - [Create Records (POST)](#create-records-post)
  - [Read Records (GET)](#read-records-get)
  - [Update Records (PUT)](#update-records-put)
  - [Delete Records (DELETE)](#delete-records-delete)
- [Query API Examples](#query-api-examples)
  - [POST Query Endpoint](#post-query-endpoint)
  - [GET Query Endpoint](#get-query-endpoint)
- [Response Formats](#response-formats)
- [Advanced Examples](#advanced-examples)

---

## Setup: Create a Table

First, create a table using the query endpoint:

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, email VARCHAR, age INTEGER, status VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 0,
  "execution_time_ms": 5
}
```

---

## CRUD API Examples

The CRUD API provides RESTful operations on tables at `/duckdb/api/{table}`.

### Create Records (POST)

#### Insert a single record

```bash
curl -X POST http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1,
    "name": "Alice Johnson",
    "email": "alice@example.com",
    "age": 28,
    "status": "active"
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 1
}
```

#### Insert more records for testing

```bash
# Insert Bob
curl -X POST http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active"}'

# Insert Charlie
curl -X POST http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "age": 22, "status": "pending"}'

# Insert Diana
curl -X POST http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active"}'

# Insert Eve
curl -X POST http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"id": 5, "name": "Eve Wilson", "email": "eve@example.com", "age": 31, "status": "inactive"}'
```

---

### Read Records (GET)

#### Get all records

```bash
curl http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"},
    {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "age": 22, "status": "pending", "created_at": "2025-01-15T10:30:02Z"},
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"},
    {"id": 5, "name": "Eve Wilson", "email": "eve@example.com", "age": 31, "status": "inactive", "created_at": "2025-01-15T10:30:04Z"}
  ],
  "meta": {
    "total_rows": 5
  }
}
```

#### Pagination

```bash
# Get page 1 with 2 records per page
curl "http://localhost:8080/duckdb/api/users?page=1&limit=2" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"}
  ],
  "meta": {
    "page": 1,
    "limit": 2,
    "total_rows": 5,
    "total_pages": 3
  }
}
```

#### Pagination with HATEOAS links

```bash
curl "http://localhost:8080/duckdb/api/users?page=2&limit=2&links=true" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "age": 22, "status": "pending", "created_at": "2025-01-15T10:30:02Z"},
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"}
  ],
  "meta": {
    "page": 2,
    "limit": 2,
    "total_rows": 5,
    "total_pages": 3
  },
  "links": {
    "self": "/duckdb/api/users?limit=2&page=2",
    "first": "/duckdb/api/users?limit=2&page=1",
    "prev": "/duckdb/api/users?limit=2&page=1",
    "next": "/duckdb/api/users?limit=2&page=3",
    "last": "/duckdb/api/users?limit=2&page=3"
  }
}
```

#### Filtering

**Filter operators:** `eq` (equals), `ne` (not equals), `gt` (greater than), `gte` (greater or equal), `lt` (less than), `lte` (less or equal), `like` (pattern match), `in` (in list)

```bash
# Filter by exact match (status = 'active')
curl "http://localhost:8080/duckdb/api/users?filter=status:eq:active" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"},
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"}
  ],
  "meta": {
    "total_rows": 3
  }
}
```

```bash
# Filter by age greater than 30
curl "http://localhost:8080/duckdb/api/users?filter=age:gt:30" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"},
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"},
    {"id": 5, "name": "Eve Wilson", "email": "eve@example.com", "age": 31, "status": "inactive", "created_at": "2025-01-15T10:30:04Z"}
  ],
  "meta": {
    "total_rows": 3
  }
}
```

```bash
# Filter with LIKE pattern (names containing 'son')
curl "http://localhost:8080/duckdb/api/users?filter=name:like:%25son%25" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Note:** `%25` is URL-encoded `%` for the LIKE wildcard.

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 5, "name": "Eve Wilson", "email": "eve@example.com", "age": 31, "status": "inactive", "created_at": "2025-01-15T10:30:04Z"}
  ],
  "meta": {
    "total_rows": 2
  }
}
```

```bash
# Filter with IN operator (status in 'active' or 'pending')
curl "http://localhost:8080/duckdb/api/users?filter=status:in:active|pending" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"},
    {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "age": 22, "status": "pending", "created_at": "2025-01-15T10:30:02Z"},
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"}
  ],
  "meta": {
    "total_rows": 4
  }
}
```

```bash
# Multiple filters (AND logic): active users over 30
curl "http://localhost:8080/duckdb/api/users?filter=status:eq:active,age:gt:30" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"},
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"}
  ],
  "meta": {
    "total_rows": 2
  }
}
```

#### Sorting

```bash
# Sort by age descending
curl "http://localhost:8080/duckdb/api/users?sort=age:desc" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"},
    {"id": 5, "name": "Eve Wilson", "email": "eve@example.com", "age": 31, "status": "inactive", "created_at": "2025-01-15T10:30:04Z"},
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "age": 22, "status": "pending", "created_at": "2025-01-15T10:30:02Z"}
  ],
  "meta": {
    "total_rows": 5
  }
}
```

```bash
# Sort by multiple columns: status ascending, then age descending
curl "http://localhost:8080/duckdb/api/users?sort=status:asc,age:desc" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"},
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 5, "name": "Eve Wilson", "email": "eve@example.com", "age": 31, "status": "inactive", "created_at": "2025-01-15T10:30:04Z"},
    {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "age": 22, "status": "pending", "created_at": "2025-01-15T10:30:02Z"}
  ],
  "meta": {
    "total_rows": 5
  }
}
```

#### Combining Filter, Sort, and Pagination

```bash
# Get active users, sorted by age descending, page 1 with 2 per page
curl "http://localhost:8080/duckdb/api/users?filter=status:eq:active&sort=age:desc&page=1&limit=2" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 4, "name": "Diana Ross", "email": "diana@example.com", "age": 45, "status": "active", "created_at": "2025-01-15T10:30:03Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "active", "created_at": "2025-01-15T10:30:01Z"}
  ],
  "meta": {
    "page": 1,
    "limit": 2,
    "total_rows": 3,
    "total_pages": 2
  }
}
```

---

### Update Records (PUT)

The PUT endpoint requires both `where` (filter conditions) and `set` (new values) in the request body.

#### Update a single record by ID

```bash
curl -X PUT http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "where": [{"column": "id", "op": "eq", "value": 3}],
    "set": {"status": "active"}
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 1
}
```

#### Update multiple records with a condition

```bash
# Set all users over 30 to premium status
curl -X PUT http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "where": [{"column": "age", "op": "gt", "value": 30}],
    "set": {"status": "premium"}
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 3
}
```

#### Update with multiple conditions

```bash
# Update inactive users named Eve to active
curl -X PUT http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "where": [
      {"column": "status", "op": "eq", "value": "inactive"},
      {"column": "name", "op": "like", "value": "%Eve%"}
    ],
    "set": {"status": "active"}
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 1
}
```

#### Update multiple fields at once

```bash
curl -X PUT http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "where": [{"column": "id", "op": "eq", "value": 1}],
    "set": {
      "name": "Alice Smith",
      "email": "alice.smith@example.com",
      "age": 29
    }
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 1
}
```

---

### Delete Records (DELETE)

The DELETE endpoint requires a `where` clause in the query parameters to prevent accidental mass deletions.

#### Delete with dry run (preview)

Use `dry_run=true` to see how many records would be deleted without actually deleting them:

```bash
curl -X DELETE "http://localhost:8080/duckdb/api/users?where=status:eq:pending&dry_run=true" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "dry_run": true,
  "affected_rows": 1
}
```

#### Delete a single record by ID

```bash
curl -X DELETE "http://localhost:8080/duckdb/api/users?where=id:eq:5" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 1
}
```

#### Delete with multiple conditions

```bash
# Delete all inactive users over 40
curl -X DELETE "http://localhost:8080/duckdb/api/users?where=status:eq:inactive,age:gt:40" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 0
}
```

#### Delete using IN operator

```bash
# Delete specific users by ID
curl -X DELETE "http://localhost:8080/duckdb/api/users?where=id:in:3|4" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 2
}
```

---

## Query API Examples

The Query API allows executing raw SQL queries at `/duckdb/query`.

### POST Query Endpoint

#### Simple SELECT query

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users WHERE age > 25 ORDER BY age"}'
```

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice Smith", "email": "alice.smith@example.com", "age": 29, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "premium", "created_at": "2025-01-15T10:30:01Z"}
  ]
}
```

#### Parameterized query

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM users WHERE age > ? AND status = ?",
    "params": [25, "active"]
  }'
```

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice Smith", "email": "alice.smith@example.com", "age": 29, "status": "active", "created_at": "2025-01-15T10:30:00Z"}
  ]
}
```

#### Aggregation query

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT status, COUNT(*) as count, AVG(age) as avg_age FROM users GROUP BY status"}'
```

**Response:**
```json
{
  "data": [
    {"status": "active", "count": 1, "avg_age": 29.0},
    {"status": "premium", "count": 1, "avg_age": 35.0}
  ]
}
```

#### INSERT query

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "INSERT INTO users (id, name, email, age, status) VALUES (?, ?, ?, ?, ?)",
    "params": [6, "Frank Miller", "frank@example.com", 42, "active"]
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 1,
  "execution_time_ms": 3
}
```

#### CREATE TABLE query

```bash
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product VARCHAR, amount DECIMAL(10,2), order_date DATE)"
  }'
```

**Response:**
```json
{
  "success": true,
  "rows_affected": 0,
  "execution_time_ms": 4
}
```

#### JOIN query

```bash
# First, insert some orders
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO orders VALUES (1, 1, '\''Laptop'\'', 999.99, '\''2025-01-15'\''), (2, 1, '\''Mouse'\'', 29.99, '\''2025-01-15'\''), (3, 2, '\''Keyboard'\'', 79.99, '\''2025-01-16'\'')"}'

# Then query with JOIN
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT u.name, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC"}'
```

**Response:**
```json
{
  "data": [
    {"name": "Alice Smith", "product": "Laptop", "amount": 999.99},
    {"name": "Bob Smith", "product": "Keyboard", "amount": 79.99},
    {"name": "Alice Smith", "product": "Mouse", "amount": 29.99}
  ]
}
```

#### DuckDB-specific features

```bash
# Use DuckDB's EXPLAIN ANALYZE
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "EXPLAIN ANALYZE SELECT * FROM users WHERE age > 30"}'

# Use DuckDB's DESCRIBE
curl -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "DESCRIBE users"}'
```

**DESCRIBE Response:**
```json
{
  "data": [
    {"column_name": "id", "column_type": "INTEGER", "null": "YES", "key": "PRI", "default": null, "extra": null},
    {"column_name": "name", "column_type": "VARCHAR", "null": "YES", "key": null, "default": null, "extra": null},
    {"column_name": "email", "column_type": "VARCHAR", "null": "YES", "key": null, "default": null, "extra": null},
    {"column_name": "age", "column_type": "INTEGER", "null": "YES", "key": null, "default": null, "extra": null},
    {"column_name": "status", "column_type": "VARCHAR", "null": "YES", "key": null, "default": null, "extra": null},
    {"column_name": "created_at", "column_type": "TIMESTAMP", "null": "YES", "key": null, "default": "CURRENT_TIMESTAMP", "extra": null}
  ]
}
```

### GET Query Endpoint

The GET endpoint allows read-only queries via URL. The SQL must be URL-encoded.

**Pattern:** `/duckdb/query/{urlEncodedSQL}/result.{format}`

#### Simple GET query

```bash
# URL-encode: SELECT * FROM users
curl "http://localhost:8080/duckdb/query/SELECT%20%2A%20FROM%20users/result.json" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice Smith", "email": "alice.smith@example.com", "age": 29, "status": "active", "created_at": "2025-01-15T10:30:00Z"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "status": "premium", "created_at": "2025-01-15T10:30:01Z"},
    {"id": 6, "name": "Frank Miller", "email": "frank@example.com", "age": 42, "status": "active", "created_at": "2025-01-15T11:00:00Z"}
  ]
}
```

#### GET query with WHERE clause

```bash
# URL-encode: SELECT name, age FROM users WHERE age > 30
curl "http://localhost:8080/duckdb/query/SELECT%20name%2C%20age%20FROM%20users%20WHERE%20age%20%3E%2030/result.json" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response:**
```json
{
  "data": [
    {"name": "Bob Smith", "age": 35},
    {"name": "Frank Miller", "age": 42}
  ]
}
```

**Note:** GET requests only support read-only queries (SELECT, SHOW, DESCRIBE, EXPLAIN). Attempting to run INSERT/UPDATE/DELETE via GET will return an error.

---

## Response Formats

Both CRUD and Query endpoints support multiple output formats.

### JSON (default)

```bash
curl http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Accept: application/json"
```

### CSV

```bash
curl http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Accept: text/csv"
```

**Response:**
```csv
id,name,email,age,status,created_at
1,Alice Smith,alice.smith@example.com,29,active,2025-01-15T10:30:00Z
2,Bob Smith,bob@example.com,35,premium,2025-01-15T10:30:01Z
6,Frank Miller,frank@example.com,42,active,2025-01-15T11:00:00Z
```

### Parquet

```bash
# Save to file
curl http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Accept: application/parquet" \
  -o users.parquet

# Or via GET query endpoint
curl "http://localhost:8080/duckdb/query/SELECT%20%2A%20FROM%20users/result.parquet" \
  -H "X-API-Key: YOUR_API_KEY" \
  -o users.parquet
```

### Apache Arrow IPC

```bash
curl http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -o users.arrow
```

---

## Advanced Examples

### Create a complete workflow

```bash
#!/bin/bash
API_KEY="YOUR_API_KEY"
BASE_URL="http://localhost:8080"

# 1. Create products table
curl -X POST "$BASE_URL/duckdb/query" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name VARCHAR, price DECIMAL(10,2), category VARCHAR, stock INTEGER)"}'

# 2. Insert products via CRUD API
for i in {1..5}; do
  curl -X POST "$BASE_URL/duckdb/api/products" \
    -H "X-API-Key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"id\": $i, \"name\": \"Product $i\", \"price\": $((i * 10)).99, \"category\": \"Category $((i % 3 + 1))\", \"stock\": $((i * 10))}"
done

# 3. Query products by category
curl "$BASE_URL/duckdb/api/products?filter=category:eq:Category%201&sort=price:desc" \
  -H "X-API-Key: $API_KEY"

# 4. Update stock for low-stock items
curl -X PUT "$BASE_URL/duckdb/api/products" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"where": [{"column": "stock", "op": "lt", "value": 20}], "set": {"stock": 100}}'

# 5. Export to Parquet
curl "$BASE_URL/duckdb/query/SELECT%20%2A%20FROM%20products/result.parquet" \
  -H "X-API-Key: $API_KEY" \
  -o products.parquet

# 6. Clean up - delete low-value products
curl -X DELETE "$BASE_URL/duckdb/api/products?where=price:lt:20" \
  -H "X-API-Key: $API_KEY"
```

### Using with jq for JSON processing

```bash
# Get all users and format with jq
curl -s http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY" | jq '.data[] | {name, age}'

# Get count of users by status
curl -s -X POST http://localhost:8080/duckdb/query \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT status, COUNT(*) as count FROM users GROUP BY status"}' | jq '.data'
```

---

## Error Responses

### Missing API Key

```bash
curl http://localhost:8080/duckdb/api/users
```

**Response (401):**
```json
{
  "error": "Unauthorized",
  "message": "Missing API key",
  "code": 401
}
```

### Invalid API Key

```bash
curl http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: invalid-key"
```

**Response (401):**
```json
{
  "error": "Unauthorized",
  "message": "Invalid API key",
  "code": 401
}
```

### Table Not Found

```bash
curl http://localhost:8080/duckdb/api/nonexistent \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response (404):**
```json
{
  "error": "Not Found",
  "message": "Table 'nonexistent' does not exist",
  "code": 404
}
```

### Missing WHERE clause for DELETE

```bash
curl -X DELETE http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: YOUR_API_KEY"
```

**Response (400):**
```json
{
  "error": "Bad Request",
  "message": "WHERE clause is required for DELETE operation (use ?where=column:operator:value)",
  "code": 400
}
```

### Insufficient Permissions

```bash
# Using a reader role key to try to insert
curl -X POST http://localhost:8080/duckdb/api/users \
  -H "X-API-Key: READER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"id": 99, "name": "Test"}'
```

**Response (403):**
```json
{
  "error": "Forbidden",
  "message": "Forbidden: insufficient permissions for CREATE operation",
  "code": 403
}
```
