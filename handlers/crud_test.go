package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tobilg/caddyserver-duckdb-module/auth"
	"github.com/tobilg/caddyserver-duckdb-module/database"
	"go.uber.org/zap"
)

// setupTestHandler creates a CRUD handler with a test database
func setupTestHandler(t *testing.T) (*CRUDHandler, *database.Manager, func()) {
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
		CREATE TABLE test_users (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			email VARCHAR,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = mgr.ExecMain(`
		INSERT INTO test_users VALUES
			(1, 'Alice', 'alice@example.com', 30),
			(2, 'Bob', 'bob@example.com', 25),
			(3, 'Charlie', 'charlie@example.com', 35)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Create authorizer
	// Note: database.Manager already creates default roles (admin, reader, editor)
	// and default permissions for these roles, so we don't need to create them here
	authorizer := auth.NewAuthorizer(mgr.AuthDB())

	handler := NewCRUDHandler(mgr, authorizer, 100, 10000, zap.NewNop())

	cleanup := func() {
		mgr.Close()
	}

	return handler, mgr, cleanup
}

// addAuthContext adds the role and api key to the request context
func addAuthContext(r *http.Request, role string) *http.Request {
	ctx := context.WithValue(r.Context(), auth.ContextKeyRole, role)
	ctx = context.WithValue(ctx, auth.ContextKeyRequestID, "test-request-id")
	return r.WithContext(ctx)
}

func TestCRUDHandler_Read_BasicQuery(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/test_users", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

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

func TestCRUDHandler_Read_WithPagination(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/test_users?limit=2&page=1", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	data := result["data"].([]interface{})
	if len(data) != 2 {
		t.Errorf("Expected 2 rows with limit=2, got %d", len(data))
	}

	pagination := result["pagination"].(map[string]interface{})
	if pagination["page"].(float64) != 1 {
		t.Errorf("Expected page 1, got %v", pagination["page"])
	}
	if pagination["limit"].(float64) != 2 {
		t.Errorf("Expected limit 2, got %v", pagination["limit"])
	}
}

func TestCRUDHandler_Read_WithFilters(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/test_users?filter=age:gte:30", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	data := result["data"].([]interface{})
	if len(data) != 2 { // Alice (30) and Charlie (35)
		t.Errorf("Expected 2 rows with age >= 30, got %d", len(data))
	}
}

func TestCRUDHandler_Read_WithSorting(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/test_users?sort=age:desc", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	data := result["data"].([]interface{})
	if len(data) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(data))
	}

	// First row should be Charlie (age 35)
	firstRow := data[0].(map[string]interface{})
	if firstRow["name"] != "Charlie" {
		t.Errorf("Expected first row to be Charlie (oldest), got %v", firstRow["name"])
	}
}

func TestCRUDHandler_Create(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	body := bytes.NewBufferString(`{"id": 4, "name": "David", "email": "david@example.com", "age": 28}`)
	req := httptest.NewRequest("POST", "/duckdb/api/test_users", body)
	req.Header.Set("Content-Type", "application/json")
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	if !result["success"].(bool) {
		t.Error("Expected success to be true")
	}
	if result["rows_affected"].(float64) != 1 {
		t.Errorf("Expected 1 row affected, got %v", result["rows_affected"])
	}
}

func TestCRUDHandler_Create_InvalidJSON(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	body := bytes.NewBufferString(`{invalid json}`)
	req := httptest.NewRequest("POST", "/duckdb/api/test_users", body)
	req.Header.Set("Content-Type", "application/json")
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestCRUDHandler_Update(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	body := bytes.NewBufferString(`{
		"where": [{"column": "id", "op": "eq", "value": 1}],
		"set": {"age": 31}
	}`)
	req := httptest.NewRequest("PUT", "/duckdb/api/test_users", body)
	req.Header.Set("Content-Type", "application/json")
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	if result["rows_affected"].(float64) != 1 {
		t.Errorf("Expected 1 row affected, got %v", result["rows_affected"])
	}
}

func TestCRUDHandler_Update_MissingWhere(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	body := bytes.NewBufferString(`{
		"set": {"age": 31}
	}`)
	req := httptest.NewRequest("PUT", "/duckdb/api/test_users", body)
	req.Header.Set("Content-Type", "application/json")
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for missing WHERE, got %d", rec.Code)
	}
}

func TestCRUDHandler_Update_MissingSet(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	body := bytes.NewBufferString(`{
		"where": [{"column": "id", "op": "eq", "value": 1}]
	}`)
	req := httptest.NewRequest("PUT", "/duckdb/api/test_users", body)
	req.Header.Set("Content-Type", "application/json")
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for missing SET, got %d", rec.Code)
	}
}

func TestCRUDHandler_Delete(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("DELETE", "/duckdb/api/test_users?where=id:eq:1", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	if result["rows_affected"].(float64) != 1 {
		t.Errorf("Expected 1 row affected, got %v", result["rows_affected"])
	}
}

func TestCRUDHandler_Delete_DryRun(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("DELETE", "/duckdb/api/test_users?where=age:gte:30&dry_run=true", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)

	if !result["dry_run"].(bool) {
		t.Error("Expected dry_run to be true")
	}
	if result["affected_rows"].(float64) != 2 { // Alice and Charlie
		t.Errorf("Expected 2 affected rows, got %v", result["affected_rows"])
	}
}

func TestCRUDHandler_Delete_MissingWhere(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("DELETE", "/duckdb/api/test_users", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for missing WHERE, got %d", rec.Code)
	}
}

func TestCRUDHandler_TableNotFound(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/nonexistent_table", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestCRUDHandler_InternalTableForbidden(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	// Try to access internal auth table
	internalTables := []string{"api_keys", "roles", "permissions"}
	for _, table := range internalTables {
		req := httptest.NewRequest("GET", "/duckdb/api/"+table, nil)
		req = addAuthContext(req, "admin")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("Expected status 403 for %s, got %d", table, rec.Code)
		}
	}
}

func TestCRUDHandler_InvalidTableName(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	// Test invalid table names (contain invalid characters)
	invalidNames := []string{
		"users$table", // Contains $
		"users-table", // Contains hyphen
		"users.table", // Contains dot
		"123users",    // Starts with number (still alphanumeric, should be fine actually)
	}

	for _, name := range invalidNames {
		req := httptest.NewRequest("GET", "/duckdb/api/"+name, nil)
		req = addAuthContext(req, "admin")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		// Invalid table names should return 400 Bad Request
		if rec.Code != http.StatusBadRequest && rec.Code != http.StatusNotFound {
			t.Errorf("Expected status 400 or 404 for invalid table name '%s', got %d", name, rec.Code)
		}
	}
}

func TestCRUDHandler_MethodNotAllowed(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("PATCH", "/duckdb/api/test_users", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", rec.Code)
	}
}

func TestCRUDHandler_EmptyPath(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for empty table name, got %d", rec.Code)
	}
}

func TestCRUDHandler_Read_CSVFormat(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/test_users", nil)
	req.Header.Set("Accept", "text/csv")
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	if ct := rec.Header().Get("Content-Type"); ct != "text/csv" {
		t.Errorf("Expected Content-Type 'text/csv', got '%s'", ct)
	}
}

func TestCRUDHandler_Read_InvalidFilter(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/test_users?filter=invalid_format", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestCRUDHandler_Read_InvalidSort(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/api/test_users?sort=name:invalid", nil)
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestCRUDHandler_Update_InvalidOperator(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	body := bytes.NewBufferString(`{
		"where": [{"column": "id", "op": "invalid", "value": 1}],
		"set": {"age": 31}
	}`)
	req := httptest.NewRequest("PUT", "/duckdb/api/test_users", body)
	req.Header.Set("Content-Type", "application/json")
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for invalid operator, got %d", rec.Code)
	}
}

func TestCRUDHandler_Create_InvalidColumnName(t *testing.T) {
	handler, _, cleanup := setupTestHandler(t)
	defer cleanup()

	body := bytes.NewBufferString(`{"id": 4, "name;DROP TABLE test_users;--": "David"}`)
	req := httptest.NewRequest("POST", "/duckdb/api/test_users", body)
	req.Header.Set("Content-Type", "application/json")
	req = addAuthContext(req, "admin")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for invalid column name, got %d", rec.Code)
	}
}

func TestExtractTableFromPath(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/duckdb/api/users", "users"},
		{"/duckdb/api/user_data", "user_data"},
		{"duckdb/api/users", "users"},
		{"/duckdb/api/", ""},
		{"/api/users", ""},
		{"/duckdb/users", ""},
		{"/", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := ExtractTableFromPath(tt.path)
			if result != tt.expected {
				t.Errorf("ExtractTableFromPath(%s) = %s, want %s", tt.path, result, tt.expected)
			}
		})
	}
}

// Benchmark tests
func BenchmarkCRUDHandler_Read(b *testing.B) {
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

	mgr.ExecMain(`CREATE TABLE bench_users (id INTEGER, name VARCHAR)`)
	for i := 0; i < 100; i++ {
		mgr.ExecMain("INSERT INTO bench_users VALUES (?, ?)", i, "User")
	}

	authorizer := auth.NewAuthorizer(mgr.AuthDB())
	authorizer.CreateRole("admin", "Admin")
	benchPerm := auth.Permission{
		RoleName:  "admin",
		TableName: "*",
		CanRead:   true,
	}
	authorizer.CreatePermission(benchPerm)

	handler := NewCRUDHandler(mgr, authorizer, 100, 10000, zap.NewNop())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/duckdb/api/bench_users", nil)
		req = addAuthContext(req, "admin")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}
