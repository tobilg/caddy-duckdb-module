package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tobilg/caddyserver-duckdb-module/auth"
	"github.com/tobilg/caddyserver-duckdb-module/database"
	"go.uber.org/zap"
)

// setupQueryHandler creates a QueryHandler with a test database
func setupQueryHandler(t *testing.T) (*QueryHandler, *database.Manager, func()) {
	cfg := database.Config{
		MainDBPath:   ":memory:",
		AuthDBPath:   ":memory:",
		Threads:      1,
		AccessMode:   "read_write",
		QueryTimeout: 30 * time.Second,
		Logger:       zap.NewNop(),
	}

	mgr, err := database.NewManagerForTesting(cfg)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create test table
	_, err = mgr.ExecMain(`
		CREATE TABLE test_query (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			value DOUBLE
		)
	`)
	if err != nil {
		mgr.Close()
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = mgr.ExecMain(`
		INSERT INTO test_query VALUES
			(1, 'Alice', 100.5),
			(2, 'Bob', 200.75),
			(3, 'Charlie', 300.25)
	`)
	if err != nil {
		mgr.Close()
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Note: database.Manager already creates default roles (admin, reader, editor)
	// and default permissions for these roles
	authorizer := auth.NewAuthorizer(mgr.AuthDB())

	handler := NewQueryHandler(mgr, authorizer, zap.NewNop())

	cleanup := func() {
		mgr.Close()
	}

	return handler, mgr, cleanup
}

// addQueryAuthContext adds the role and request ID to the request context
func addQueryAuthContext(r *http.Request, role string) *http.Request {
	ctx := context.WithValue(r.Context(), auth.ContextKeyRole, role)
	ctx = context.WithValue(ctx, auth.ContextKeyRequestID, "test-request-id")
	return r.WithContext(ctx)
}

func TestNewQueryHandler(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	if handler == nil {
		t.Fatal("Expected non-nil handler")
	}
	if handler.dbMgr == nil {
		t.Error("Expected non-nil dbMgr")
	}
	if handler.authorizer == nil {
		t.Error("Expected non-nil authorizer")
	}
	if handler.logger == nil {
		t.Error("Expected non-nil logger")
	}
}

func TestQueryHandler_POST_SelectQuery(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "SELECT * FROM test_query ORDER BY id"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify JSON response
	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	data, ok := result["data"].([]interface{})
	if !ok {
		t.Fatal("Expected 'data' array in response")
	}
	if len(data) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(data))
	}
}

func TestQueryHandler_POST_SelectWithParams(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "SELECT * FROM test_query WHERE id = ?", "params": [2]}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	data := result["data"].([]interface{})
	if len(data) != 1 {
		t.Errorf("Expected 1 row, got %d", len(data))
	}
}

func TestQueryHandler_POST_InsertQuery(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "INSERT INTO test_query VALUES (4, 'Dave', 400.0)"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	if result["success"] != true {
		t.Error("Expected success to be true")
	}
	if result["rows_affected"].(float64) != 1 {
		t.Errorf("Expected rows_affected to be 1, got %v", result["rows_affected"])
	}
}

func TestQueryHandler_POST_UpdateQuery(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "UPDATE test_query SET value = 999.0 WHERE id = 1"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	if result["success"] != true {
		t.Error("Expected success to be true")
	}
}

func TestQueryHandler_POST_DeleteQuery(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "DELETE FROM test_query WHERE id = 3"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	if result["success"] != true {
		t.Error("Expected success to be true")
	}
}

func TestQueryHandler_POST_EmptySQL(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": ""}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestQueryHandler_POST_InvalidJSON(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{invalid json}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestQueryHandler_POST_InvalidSQL(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "SELEKT * FORM invalid_syntax"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", rec.Code)
	}
}

func TestQueryHandler_GET_SelectQuery(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	// URL-encode the SQL query
	sql := url.QueryEscape("SELECT * FROM test_query ORDER BY id")
	req := httptest.NewRequest("GET", "/duckdb/query/"+sql+"/result.json", nil)
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	data := result["data"].([]interface{})
	if len(data) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(data))
	}
}

func TestQueryHandler_GET_CSVFormat(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	sql := url.QueryEscape("SELECT id, name FROM test_query ORDER BY id")
	req := httptest.NewRequest("GET", "/duckdb/query/"+sql+"/result.csv", nil)
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/csv") {
		t.Errorf("Expected Content-Type text/csv, got %s", contentType)
	}

	// Verify CSV contains header and data
	body := rec.Body.String()
	if !strings.Contains(body, "id") || !strings.Contains(body, "name") {
		t.Error("Expected CSV header row")
	}
	if !strings.Contains(body, "Alice") {
		t.Error("Expected data in CSV output")
	}
}

func TestQueryHandler_GET_DMLQuery_NotAllowed(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	// GET requests should not allow DML queries
	sql := url.QueryEscape("INSERT INTO test_query VALUES (5, 'Eve', 500.0)")
	req := httptest.NewRequest("GET", "/duckdb/query/"+sql+"/result.json", nil)
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryHandler_GET_InvalidPath(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	// Invalid path format
	req := httptest.NewRequest("GET", "/duckdb/query/invalid-path", nil)
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestQueryHandler_MethodNotAllowed(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	req := httptest.NewRequest("PUT", "/duckdb/query", nil)
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", rec.Code)
	}
}

func TestQueryHandler_InternalTableAccess_Forbidden(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	// Test various internal table access attempts
	forbiddenQueries := []string{
		`{"sql": "SELECT * FROM api_keys"}`,
		`{"sql": "SELECT * FROM roles"}`,
		`{"sql": "SELECT * FROM permissions"}`,
		`{"sql": "SELECT * FROM API_KEYS"}`,
		`{"sql": "DELETE FROM api_keys WHERE 1=1"}`,
	}

	for _, body := range forbiddenQueries {
		req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = addQueryAuthContext(req, "admin")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("Expected status 403 for query %s, got %d", body, rec.Code)
		}
	}
}

func TestQueryHandler_Forbidden_NoQueryPermission(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	// Use reader role which doesn't have query permission by default
	body := `{"sql": "SELECT * FROM test_query"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "reader")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryHandler_POST_WithQuery(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	// Test WITH clause (CTE)
	body := `{"sql": "WITH cte AS (SELECT * FROM test_query WHERE id > 1) SELECT * FROM cte"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryHandler_POST_ShowTables(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "SHOW TABLES"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryHandler_POST_Describe(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "DESCRIBE test_query"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryHandler_POST_Explain(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "EXPLAIN SELECT * FROM test_query"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryHandler_POST_CreateTable(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "CREATE TABLE new_table (id INTEGER, name VARCHAR)"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryHandler_AcceptHeader_CSV(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "SELECT * FROM test_query ORDER BY id"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/csv")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/csv") {
		t.Errorf("Expected Content-Type text/csv, got %s", contentType)
	}
}

func TestQueryHandler_AcceptHeader_Parquet(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "SELECT * FROM test_query ORDER BY id"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/parquet")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/parquet" {
		t.Errorf("Expected Content-Type application/parquet, got %s", contentType)
	}
}

func TestQueryHandler_AcceptHeader_Arrow(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	body := `{"sql": "SELECT * FROM test_query ORDER BY id"}`
	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.apache.arrow")
	req = addQueryAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
}

func TestIsSelectQuery(t *testing.T) {
	handler, _, cleanup := setupQueryHandler(t)
	defer cleanup()

	tests := []struct {
		sql      string
		expected bool
	}{
		{"SELECT * FROM test", true},
		{"select * from test", true},
		{"  SELECT * FROM test", true},
		{"WITH cte AS (...) SELECT * FROM cte", true},
		{"SHOW TABLES", true},
		{"DESCRIBE table_name", true},
		{"EXPLAIN SELECT * FROM test", true},
		{"INSERT INTO test VALUES (1)", false},
		{"UPDATE test SET x = 1", false},
		{"DELETE FROM test", false},
		{"CREATE TABLE test (id INT)", false},
		{"DROP TABLE test", false},
		{"ALTER TABLE test ADD COLUMN x INT", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			result := handler.isSelectQuery(tt.sql)
			if result != tt.expected {
				t.Errorf("isSelectQuery(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

// Benchmark tests
func BenchmarkQueryHandler_POST_Select(b *testing.B) {
	cfg := database.Config{
		MainDBPath:   ":memory:",
		AuthDBPath:   ":memory:",
		Threads:      1,
		AccessMode:   "read_write",
		QueryTimeout: 30 * time.Second,
		Logger:       zap.NewNop(),
	}

	mgr, _ := database.NewManagerForTesting(cfg)
	defer mgr.Close()

	mgr.ExecMain(`CREATE TABLE bench_query (id INTEGER, name VARCHAR)`)
	for i := 0; i < 100; i++ {
		mgr.ExecMain("INSERT INTO bench_query VALUES (?, ?)", i, "Test")
	}

	authorizer := auth.NewAuthorizer(mgr.AuthDB())
	handler := NewQueryHandler(mgr, authorizer, zap.NewNop())

	body := []byte(`{"sql": "SELECT * FROM bench_query"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/duckdb/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = addQueryAuthContext(req, "admin")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func TestStripSQLComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no comments",
			input:    "SELECT * FROM users",
			expected: "SELECT * FROM users",
		},
		{
			name:     "single line comment at end",
			input:    "SELECT * FROM users -- this is a comment",
			expected: "SELECT * FROM users  ",
		},
		{
			name:     "single line comment in middle",
			input:    "SELECT * FROM users -- comment\nWHERE id = 1",
			expected: "SELECT * FROM users  \nWHERE id = 1",
		},
		{
			name:     "block comment",
			input:    "SELECT * FROM /* hidden */ users",
			expected: "SELECT * FROM   users",
		},
		{
			name:     "block comment spanning lines",
			input:    "SELECT * FROM /* this\nis a\nmulti-line\ncomment */ users",
			expected: "SELECT * FROM   users",
		},
		{
			name:     "multiple block comments",
			input:    "SELECT /* col */ * FROM /* table */ users",
			expected: "SELECT   * FROM   users",
		},
		{
			name:     "mixed comments",
			input:    "SELECT * /* block */ FROM users -- line comment",
			expected: "SELECT *   FROM users  ",
		},
		{
			name:     "comment inside table name attempt",
			input:    "SELECT * FROM api/**/keys",
			expected: "SELECT * FROM api keys",
		},
		{
			name:     "nested-looking comments",
			input:    "SELECT * FROM /* outer /* inner */ */ users",
			expected: "SELECT * FROM   */ users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripSQLComments(tt.input)
			if result != tt.expected {
				t.Errorf("stripSQLComments(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestContainsInternalTables(t *testing.T) {
	// Create a minimal QueryHandler for testing
	h := &QueryHandler{}

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		// Basic detection tests
		{
			name:     "direct api_keys reference",
			sql:      "SELECT * FROM api_keys",
			expected: true,
		},
		{
			name:     "direct roles reference",
			sql:      "SELECT * FROM roles",
			expected: true,
		},
		{
			name:     "direct permissions reference",
			sql:      "SELECT * FROM permissions",
			expected: true,
		},
		{
			name:     "safe query",
			sql:      "SELECT * FROM users",
			expected: false,
		},

		// Case insensitivity tests
		{
			name:     "uppercase API_KEYS",
			sql:      "SELECT * FROM API_KEYS",
			expected: true,
		},
		{
			name:     "mixed case Api_Keys",
			sql:      "SELECT * FROM Api_Keys",
			expected: true,
		},
		{
			name:     "uppercase ROLES",
			sql:      "SELECT * FROM ROLES",
			expected: true,
		},
		{
			name:     "uppercase PERMISSIONS",
			sql:      "SELECT * FROM PERMISSIONS",
			expected: true,
		},

		// Comment bypass attempts
		{
			name:     "block comment in table name api/**/keys",
			sql:      "SELECT * FROM api/**/keys",
			expected: false, // After stripping comment, becomes "api keys" (two words)
		},
		{
			name:     "block comment in table name api/* comment */keys",
			sql:      "SELECT * FROM api/* bypass */keys",
			expected: false, // After stripping, becomes "api keys"
		},
		{
			name:     "line comment bypass attempt",
			sql:      "SELECT * FROM api_keys -- harmless comment",
			expected: true,
		},
		{
			name:     "multiline block comment with api_keys",
			sql:      "SELECT /* selecting all */ * FROM api_keys /* from keys table */",
			expected: true,
		},

		// Word boundary tests
		{
			name:     "table name containing roles (user_roles)",
			sql:      "SELECT * FROM user_roles",
			expected: false, // "roles" is part of "user_roles", not a word boundary match
		},
		{
			name:     "table name containing roles prefix (roles_mapping)",
			sql:      "SELECT * FROM roles_mapping",
			expected: false, // "roles" followed by underscore, not word boundary
		},
		{
			name:     "column named roles in different table",
			sql:      "SELECT roles FROM user_settings",
			expected: true, // "roles" as column name still matches (conservative)
		},
		{
			name:     "table name api_keys_backup",
			sql:      "SELECT * FROM api_keys_backup",
			expected: false, // "api_keys" is part of larger word
		},
		{
			name:     "permissions_log table",
			sql:      "SELECT * FROM permissions_log",
			expected: false, // "permissions" is part of larger word
		},

		// Whitespace variations
		{
			name:     "extra spaces around table name",
			sql:      "SELECT * FROM   api_keys   WHERE id = 1",
			expected: true,
		},
		{
			name:     "tabs around table name",
			sql:      "SELECT * FROM\t\tapi_keys\t\tWHERE id = 1",
			expected: true,
		},
		{
			name:     "newlines in query",
			sql:      "SELECT *\nFROM api_keys\nWHERE id = 1",
			expected: true,
		},

		// JOIN queries
		{
			name:     "join with api_keys",
			sql:      "SELECT u.* FROM users u JOIN api_keys a ON u.id = a.user_id",
			expected: true,
		},
		{
			name:     "join with safe tables",
			sql:      "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id",
			expected: false,
		},

		// Subqueries
		{
			name:     "subquery with api_keys",
			sql:      "SELECT * FROM users WHERE id IN (SELECT user_id FROM api_keys)",
			expected: true,
		},
		{
			name:     "nested subquery with permissions",
			sql:      "SELECT * FROM (SELECT * FROM permissions) AS p",
			expected: true,
		},

		// UNION queries
		{
			name:     "union with api_keys",
			sql:      "SELECT id FROM users UNION SELECT id FROM api_keys",
			expected: true,
		},

		// CTEs (WITH clauses)
		{
			name:     "CTE with api_keys",
			sql:      "WITH key_data AS (SELECT * FROM api_keys) SELECT * FROM key_data",
			expected: true,
		},
		{
			name:     "safe CTE",
			sql:      "WITH user_data AS (SELECT * FROM users) SELECT * FROM user_data",
			expected: false,
		},

		// INSERT/UPDATE/DELETE operations
		{
			name:     "insert into api_keys",
			sql:      "INSERT INTO api_keys (key, role_name) VALUES ('test', 'admin')",
			expected: true,
		},
		{
			name:     "update api_keys",
			sql:      "UPDATE api_keys SET is_active = false WHERE key = 'test'",
			expected: true,
		},
		{
			name:     "delete from permissions",
			sql:      "DELETE FROM permissions WHERE role_name = 'test'",
			expected: true,
		},

		// Edge cases
		{
			name:     "empty query",
			sql:      "",
			expected: false,
		},
		{
			name:     "only whitespace",
			sql:      "   \t\n  ",
			expected: false,
		},
		{
			name:     "table name in string literal (still blocked - conservative)",
			sql:      "SELECT * FROM users WHERE name = 'api_keys'",
			expected: true, // Conservative approach blocks this too
		},
		{
			name:     "EXPLAIN query with api_keys",
			sql:      "EXPLAIN SELECT * FROM api_keys",
			expected: true,
		},
		{
			name:     "DESCRIBE api_keys",
			sql:      "DESCRIBE api_keys",
			expected: true,
		},

		// Schema-qualified names
		{
			name:     "schema-qualified api_keys",
			sql:      "SELECT * FROM main.api_keys",
			expected: true,
		},
		{
			name:     "schema-qualified roles",
			sql:      "SELECT * FROM public.roles",
			expected: true,
		},

		// Aliased tables
		{
			name:     "aliased api_keys",
			sql:      "SELECT a.* FROM api_keys a",
			expected: true,
		},
		{
			name:     "aliased api_keys with AS",
			sql:      "SELECT a.* FROM api_keys AS a",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.containsInternalTables(tt.sql)
			if result != tt.expected {
				t.Errorf("containsInternalTables(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestContainsInternalTables_AllInternalTables(t *testing.T) {
	h := &QueryHandler{}

	internalTables := []string{"api_keys", "roles", "permissions"}

	for _, table := range internalTables {
		t.Run("blocks_"+table, func(t *testing.T) {
			sql := "SELECT * FROM " + table
			if !h.containsInternalTables(sql) {
				t.Errorf("Expected containsInternalTables to block access to %s", table)
			}
		})
	}
}

func TestContainsInternalTables_CommentBypassAttempts(t *testing.T) {
	h := &QueryHandler{}

	bypassAttempts := []struct {
		name        string
		sql         string
		shouldBlock bool
	}{
		// These should be blocked (they still reference internal tables after cleanup)
		{
			name:        "simple reference with trailing comment",
			sql:         "SELECT * FROM api_keys -- bypass",
			shouldBlock: true,
		},
		{
			name:        "reference surrounded by block comments",
			sql:         "/* start */ SELECT * FROM api_keys /* end */",
			shouldBlock: true,
		},

		// These should NOT be blocked (the comment actually splits the table name)
		{
			name:        "block comment splits api_keys",
			sql:         "SELECT * FROM api/**/keys",
			shouldBlock: false, // becomes "api  keys" after stripping
		},
		{
			name:        "block comment splits roles",
			sql:         "SELECT * FROM ro/*split*/les",
			shouldBlock: false, // becomes "ro les" after stripping
		},
		{
			name:        "block comment splits permissions",
			sql:         "SELECT * FROM permiss/**/ions",
			shouldBlock: false, // becomes "permiss ions" after stripping
		},
	}

	for _, tt := range bypassAttempts {
		t.Run(tt.name, func(t *testing.T) {
			result := h.containsInternalTables(tt.sql)
			if result != tt.shouldBlock {
				if tt.shouldBlock {
					t.Errorf("Expected to block %q, but it was allowed", tt.sql)
				} else {
					t.Errorf("Expected to allow %q, but it was blocked", tt.sql)
				}
			}
		})
	}
}

func BenchmarkContainsInternalTables(b *testing.B) {
	h := &QueryHandler{}
	sql := "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.containsInternalTables(sql)
	}
}

func BenchmarkContainsInternalTables_WithComments(b *testing.B) {
	h := &QueryHandler{}
	sql := "SELECT /* columns */ u.*, o.* FROM users u /* user table */ JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' -- filter active users"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.containsInternalTables(sql)
	}
}

func BenchmarkStripSQLComments(b *testing.B) {
	sql := "SELECT /* columns */ u.*, o.* FROM users u /* user table */ JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' -- filter active users"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stripSQLComments(sql)
	}
}
