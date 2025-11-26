package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tobilg/caddyserver-duckdb-module/database"
	"go.uber.org/zap"
)

// setupMiddlewareTest creates a middleware with a test database
func setupMiddlewareTest(t *testing.T) (*Middleware, *Authorizer, func()) {
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

	authorizer := NewAuthorizer(mgr.AuthDB())

	// Note: database.Manager already creates default roles (admin, reader, editor)
	// So we don't need to create the admin role - just the API key

	// Create test API key
	err = authorizer.CreateAPIKey("test-key", "admin", nil)
	if err != nil {
		t.Fatalf("Failed to create API key: %v", err)
	}

	// Note: database.Manager already creates default permissions for admin role (CRUD on all tables)
	// So we don't need to create permissions here

	middleware := NewMiddleware(authorizer)

	cleanup := func() {
		mgr.Close()
	}

	return middleware, authorizer, cleanup
}

func TestMiddleware_Authenticate_ValidKey(t *testing.T) {
	mw, _, cleanup := setupMiddlewareTest(t)
	defer cleanup()

	// Create a test handler that will be called if auth succeeds
	called := false
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		// Verify context has role
		role := GetRoleFromContext(r.Context())
		if role != "admin" {
			t.Errorf("Expected role 'admin', got '%s'", role)
		}
		// Verify context has API key
		apiKey := GetAPIKeyFromContext(r.Context())
		if apiKey == nil {
			t.Error("Expected API key in context")
		}
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with authenticate middleware
	handler := mw.Authenticate(testHandler)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "test-key")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !called {
		t.Error("Expected handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
}

func TestMiddleware_Authenticate_MissingKey(t *testing.T) {
	mw, _, cleanup := setupMiddlewareTest(t)
	defer cleanup()

	called := false
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	handler := mw.Authenticate(testHandler)

	req := httptest.NewRequest("GET", "/", nil)
	// No X-API-Key header

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if called {
		t.Error("Handler should not be called without API key")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", rec.Code)
	}

	// Verify error response format
	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)
	if result["code"].(float64) != 401 {
		t.Error("Expected code 401 in response body")
	}
}

func TestMiddleware_Authenticate_InvalidKey(t *testing.T) {
	mw, _, cleanup := setupMiddlewareTest(t)
	defer cleanup()

	called := false
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	handler := mw.Authenticate(testHandler)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "invalid-key-that-doesnt-exist")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if called {
		t.Error("Handler should not be called with invalid API key")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", rec.Code)
	}
}

func TestMiddleware_Authorize_Allowed(t *testing.T) {
	mw, _, cleanup := setupMiddlewareTest(t)
	defer cleanup()

	called := false
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with authorize middleware for "users" table, READ operation
	handler := mw.Authorize("users", OperationRead)(testHandler)

	// Create request with admin role in context
	req := httptest.NewRequest("GET", "/", nil)
	ctx := context.WithValue(req.Context(), ContextKeyRole, "admin")
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !called {
		t.Error("Expected handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
}

func TestMiddleware_Authorize_NoRole(t *testing.T) {
	mw, _, cleanup := setupMiddlewareTest(t)
	defer cleanup()

	called := false
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	handler := mw.Authorize("users", OperationRead)(testHandler)

	// Request without role in context
	req := httptest.NewRequest("GET", "/", nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if called {
		t.Error("Handler should not be called without role")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", rec.Code)
	}
}

func TestMiddleware_Authorize_Forbidden(t *testing.T) {
	mw, authorizer, cleanup := setupMiddlewareTest(t)
	defer cleanup()

	// Create a restricted role without write permission
	authorizer.CreateRole("reader", "Read-only role")
	readerPerm := Permission{
		RoleName:  "reader",
		TableName: "*",
		CanRead:   true,
	}
	authorizer.CreatePermission(readerPerm)

	called := false
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	// Try to access with CREATE operation as reader
	handler := mw.Authorize("users", OperationCreate)(testHandler)

	req := httptest.NewRequest("POST", "/", nil)
	ctx := context.WithValue(req.Context(), ContextKeyRole, "reader")
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if called {
		t.Error("Handler should not be called without permission")
	}
	if rec.Code != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d", rec.Code)
	}
}

func TestGetRoleFromContext(t *testing.T) {
	tests := []struct {
		name     string
		role     interface{}
		expected string
	}{
		{"valid role", "admin", "admin"},
		{"empty role", "", ""},
		{"no role", nil, ""},
		{"wrong type", 123, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.role != nil {
				ctx = context.WithValue(ctx, ContextKeyRole, tt.role)
			}

			result := GetRoleFromContext(ctx)
			if result != tt.expected {
				t.Errorf("GetRoleFromContext() = '%s', want '%s'", result, tt.expected)
			}
		})
	}
}

func TestGetAPIKeyFromContext(t *testing.T) {
	ctx := context.Background()

	// No API key in context
	key := GetAPIKeyFromContext(ctx)
	if key != nil {
		t.Error("Expected nil when no API key in context")
	}

	// With API key in context
	apiKey := &APIKey{Key: "hash", RoleName: "admin"}
	ctx = context.WithValue(ctx, ContextKeyAPIKey, apiKey)
	key = GetAPIKeyFromContext(ctx)
	if key == nil {
		t.Error("Expected API key from context")
	}
	if key.RoleName != "admin" {
		t.Errorf("Expected role 'admin', got '%s'", key.RoleName)
	}
}

func TestExtractTableName(t *testing.T) {
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
		{"/duckdb/api/users/123", "users"},
		{"/", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := ExtractTableName(tt.path)
			if result != tt.expected {
				t.Errorf("ExtractTableName(%s) = '%s', want '%s'", tt.path, result, tt.expected)
			}
		})
	}
}

func TestIsInternalTable(t *testing.T) {
	tests := []struct {
		table    string
		expected bool
	}{
		{"api_keys", true},
		{"roles", true},
		{"permissions", true},
		{"users", false},
		{"data", false},
		{"API_KEYS", false}, // case sensitive
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			result := IsInternalTable(tt.table)
			if result != tt.expected {
				t.Errorf("IsInternalTable(%s) = %v, want %v", tt.table, result, tt.expected)
			}
		})
	}
}

func TestSetContextValues(t *testing.T) {
	ctx := context.Background()
	apiKey := &APIKey{Key: "test-key", RoleName: "admin"}

	ctx = SetContextValues(ctx, apiKey, "admin")

	// Verify values are set
	role := GetRoleFromContext(ctx)
	if role != "admin" {
		t.Errorf("Expected role 'admin', got '%s'", role)
	}

	key := GetAPIKeyFromContext(ctx)
	if key == nil || key.Key != "test-key" {
		t.Error("API key not properly set in context")
	}
}

func TestSetRequestID(t *testing.T) {
	ctx := context.Background()
	requestID := "test-request-123"

	ctx = SetRequestID(ctx, requestID)

	result := GetRequestIDFromContext(ctx)
	if result != requestID {
		t.Errorf("Expected request ID '%s', got '%s'", requestID, result)
	}
}

func TestGetRequestIDFromContext(t *testing.T) {
	tests := []struct {
		name      string
		requestID interface{}
		expected  string
	}{
		{"valid request ID", "req-123", "req-123"},
		{"empty request ID", "", ""},
		{"no request ID", nil, ""},
		{"wrong type", 123, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.requestID != nil {
				ctx = context.WithValue(ctx, ContextKeyRequestID, tt.requestID)
			}

			result := GetRequestIDFromContext(ctx)
			if result != tt.expected {
				t.Errorf("GetRequestIDFromContext() = '%s', want '%s'", result, tt.expected)
			}
		})
	}
}

func TestNewMiddleware(t *testing.T) {
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
	defer mgr.Close()

	authorizer := NewAuthorizer(mgr.AuthDB())
	mw := NewMiddleware(authorizer)

	if mw == nil {
		t.Fatal("Expected non-nil middleware")
	}
	if mw.authorizer != authorizer {
		t.Error("Middleware authorizer not properly set")
	}
}

// Benchmark tests
func BenchmarkMiddleware_Authenticate(b *testing.B) {
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

	authorizer := NewAuthorizer(mgr.AuthDB())
	// admin role already exists from database initialization
	authorizer.CreateAPIKey("bench-key", "admin", nil)

	mw := NewMiddleware(authorizer)
	handler := mw.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-API-Key", "bench-key")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func BenchmarkMiddleware_Authorize(b *testing.B) {
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

	authorizer := NewAuthorizer(mgr.AuthDB())
	// admin role already exists from database initialization
	benchPerm := Permission{
		RoleName:  "admin",
		TableName: "*",
		CanRead:   true,
	}
	authorizer.CreatePermission(benchPerm)

	mw := NewMiddleware(authorizer)
	handler := mw.Authorize("users", OperationRead)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		ctx := context.WithValue(req.Context(), ContextKeyRole, "admin")
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}
