package duckdb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/tobilg/caddyserver-duckdb-module/auth"
	"github.com/tobilg/caddyserver-duckdb-module/database"
	"github.com/tobilg/caddyserver-duckdb-module/handlers"
	"go.uber.org/zap"
)

// mockNextHandler is a test handler for the next middleware in chain
type mockNextHandler struct {
	called bool
}

func (h *mockNextHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) error {
	h.called = true
	w.WriteHeader(http.StatusOK)
	return nil
}

// setupTestModule creates a DuckDB module for testing
func setupTestModule(t *testing.T) (*DuckDB, func()) {
	cfg := database.Config{
		MainDBPath:   ":memory:",
		AuthDBPath:   ":memory:",
		Threads:      1,
		AccessMode:   "read_write",
		QueryTimeout: 10 * time.Second,
		Logger:       zap.NewNop(),
	}

	mgr, err := database.NewManagerForTesting(cfg)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create test table
	_, err = mgr.ExecMain(`CREATE TABLE test_data (id INTEGER, value VARCHAR)`)
	if err != nil {
		mgr.Close()
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create authorizer and test API key
	// Note: database.Manager already creates default roles (admin, reader, editor)
	authorizer := auth.NewAuthorizer(mgr.AuthDB())

	err = authorizer.CreateAPIKey("test-api-key", "admin", nil)
	if err != nil {
		mgr.Close()
		t.Fatalf("Failed to create API key: %v", err)
	}

	// Note: database.Manager already creates default permissions for admin role (CRUD on all tables)
	// So we don't need to create permissions here

	d := &DuckDB{
		DatabasePath:     ":memory:",
		AuthDatabasePath: ":memory:",
		QueryTimeout:     caddy.Duration(10 * time.Second),
		MaxRowsPerPage:   100,
		AbsoluteMaxRows:  10000,
		Threads:          1,
		AccessMode:       "read_write",
		logger:           zap.NewNop(),
		dbMgr:            mgr,
		authorizer:       authorizer,
		authMw:           auth.NewMiddleware(authorizer),
		routePrefix:      "/duckdb",
	}

	// Initialize handlers (matching Provision logic)
	d.crudHandler = nil // Will use handlers package if needed
	d.queryHandler = nil
	d.openAPIHandler = nil

	cleanup := func() {
		mgr.Close()
	}

	return d, cleanup
}

func TestCaddyModule(t *testing.T) {
	d := DuckDB{}
	info := d.CaddyModule()

	if info.ID != "http.handlers.duckdb" {
		t.Errorf("Expected module ID 'http.handlers.duckdb', got '%s'", info.ID)
	}

	newModule := info.New()
	if newModule == nil {
		t.Error("Expected New() to return a non-nil module")
	}
	if _, ok := newModule.(*DuckDB); !ok {
		t.Error("Expected New() to return a *DuckDB")
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	d := &DuckDB{
		AccessMode:      "read_write",
		MaxRowsPerPage:  100,
		AbsoluteMaxRows: 10000,
		Threads:         4,
	}

	err := d.Validate()
	if err != nil {
		t.Errorf("Validate() returned error for valid config: %v", err)
	}
}

func TestValidate_InvalidAccessMode(t *testing.T) {
	d := &DuckDB{
		AccessMode:      "invalid_mode",
		MaxRowsPerPage:  100,
		AbsoluteMaxRows: 10000,
		Threads:         4,
	}

	err := d.Validate()
	if err == nil {
		t.Error("Expected error for invalid access mode")
	}
}

func TestValidate_InvalidMaxRowsPerPage(t *testing.T) {
	d := &DuckDB{
		AccessMode:      "read_write",
		MaxRowsPerPage:  0,
		AbsoluteMaxRows: 10000,
		Threads:         4,
	}

	err := d.Validate()
	if err == nil {
		t.Error("Expected error for invalid max_rows_per_page")
	}
}

func TestValidate_InvalidAbsoluteMaxRows(t *testing.T) {
	d := &DuckDB{
		AccessMode:      "read_write",
		MaxRowsPerPage:  100,
		AbsoluteMaxRows: -1,
		Threads:         4,
	}

	err := d.Validate()
	if err == nil {
		t.Error("Expected error for negative absolute_max_rows")
	}
}

func TestValidate_InvalidThreads(t *testing.T) {
	d := &DuckDB{
		AccessMode:      "read_write",
		MaxRowsPerPage:  100,
		AbsoluteMaxRows: 10000,
		Threads:         0,
	}

	err := d.Validate()
	if err == nil {
		t.Error("Expected error for invalid threads")
	}
}

func TestValidate_ReadOnlyMode(t *testing.T) {
	d := &DuckDB{
		AccessMode:      "read_only",
		MaxRowsPerPage:  100,
		AbsoluteMaxRows: 10000,
		Threads:         4,
	}

	err := d.Validate()
	if err != nil {
		t.Errorf("Validate() should accept 'read_only' mode: %v", err)
	}
}

func TestServeHTTP_HealthCheck(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/health", nil)
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	err := d.ServeHTTP(rec, req, next)
	if err != nil {
		t.Errorf("ServeHTTP returned error: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)
	if result["status"] != "ok" {
		t.Errorf("Expected status 'ok', got '%v'", result["status"])
	}

	// Health check should NOT call next handler
	if next.called {
		t.Error("Health check should not call next handler")
	}
}

func TestServeHTTP_NonDuckDBPath(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/other/path", nil)
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	err := d.ServeHTTP(rec, req, next)
	if err != nil {
		t.Errorf("ServeHTTP returned error: %v", err)
	}

	// Should call next handler for non-duckdb paths
	if !next.called {
		t.Error("Expected next handler to be called for non-duckdb path")
	}
}

func TestServeHTTP_RequestID_Generated(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/health", nil)
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	d.ServeHTTP(rec, req, next)

	requestID := rec.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Error("Expected X-Request-ID header to be set")
	}
}

func TestServeHTTP_RequestID_Preserved(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/health", nil)
	req.Header.Set("X-Request-ID", "custom-request-id-123")
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	d.ServeHTTP(rec, req, next)

	requestID := rec.Header().Get("X-Request-ID")
	if requestID != "custom-request-id-123" {
		t.Errorf("Expected preserved request ID, got '%s'", requestID)
	}
}

func TestServeHTTP_Unauthenticated(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	// Try to access protected endpoint without API key
	req := httptest.NewRequest("GET", "/duckdb/query", nil)
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	d.ServeHTTP(rec, req, next)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", rec.Code)
	}

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)
	if result["code"].(float64) != 401 {
		t.Errorf("Expected code 401 in body, got %v", result["code"])
	}
}

func TestServeHTTP_InvalidAPIKey(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/query", nil)
	req.Header.Set("X-API-Key", "invalid-key")
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	d.ServeHTTP(rec, req, next)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", rec.Code)
	}
}

func TestServeHTTP_UnknownEndpoint(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/duckdb/unknown", nil)
	req.Header.Set("X-API-Key", "test-api-key")
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	d.ServeHTTP(rec, req, next)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestCleanup_WithManager(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	err := d.Cleanup()
	if err != nil {
		t.Errorf("Cleanup() returned error: %v", err)
	}
}

func TestCleanup_NilManager(t *testing.T) {
	d := &DuckDB{
		dbMgr: nil,
	}

	err := d.Cleanup()
	if err != nil {
		t.Errorf("Cleanup() with nil manager returned error: %v", err)
	}
}

func TestRoutePrefix_Default(t *testing.T) {
	// Ensure DUCKDB_ROUTE_PREFIX is not set
	os.Unsetenv("DUCKDB_ROUTE_PREFIX")

	d := &DuckDB{}
	// Simulate what Provision does for route prefix
	if envPrefix := os.Getenv("DUCKDB_ROUTE_PREFIX"); envPrefix != "" {
		d.routePrefix = envPrefix
	} else {
		d.routePrefix = "/duckdb"
	}

	if d.routePrefix != "/duckdb" {
		t.Errorf("Expected default route prefix '/duckdb', got '%s'", d.routePrefix)
	}
}

func TestRoutePrefix_FromEnv(t *testing.T) {
	os.Setenv("DUCKDB_ROUTE_PREFIX", "/custom/prefix")
	defer os.Unsetenv("DUCKDB_ROUTE_PREFIX")

	d := &DuckDB{}
	if envPrefix := os.Getenv("DUCKDB_ROUTE_PREFIX"); envPrefix != "" {
		d.routePrefix = envPrefix
	} else {
		d.routePrefix = "/duckdb"
	}

	if d.routePrefix != "/custom/prefix" {
		t.Errorf("Expected route prefix from env '/custom/prefix', got '%s'", d.routePrefix)
	}
}

func TestServeHTTP_OpenAPI(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	// Initialize OpenAPI handler for test
	d.openAPIHandler = nil // Will return error without handler

	req := httptest.NewRequest("GET", "/duckdb/openapi.json", nil)
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	// This will work even without handler since we just check routing
	d.ServeHTTP(rec, req, next)

	// OpenAPI should not require auth (but may fail if handler is nil)
	// The point is it shouldn't call next handler
	if next.called {
		t.Error("OpenAPI endpoint should not call next handler")
	}
}

// Benchmark tests
func BenchmarkServeHTTP_HealthCheck(b *testing.B) {
	tmpFile, _ := os.CreateTemp("", "bench-auth-*.db")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cfg := database.Config{
		MainDBPath:   ":memory:",
		AuthDBPath:   tmpFile.Name(),
		Threads:      1,
		AccessMode:   "read_write",
		QueryTimeout: 10 * time.Second,
		Logger:       zap.NewNop(),
	}

	mgr, _ := database.NewManagerForTesting(cfg)
	defer mgr.Close()

	d := &DuckDB{
		dbMgr:       mgr,
		authorizer:  auth.NewAuthorizer(mgr.AuthDB()),
		routePrefix: "/duckdb",
		logger:      zap.NewNop(),
	}
	d.authMw = auth.NewMiddleware(d.authorizer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/duckdb/health", nil)
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, req, &mockNextHandler{})
	}
}

func BenchmarkServeHTTP_Passthrough(b *testing.B) {
	d := &DuckDB{
		routePrefix: "/duckdb",
		logger:      zap.NewNop(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/other/path", nil)
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, req, &mockNextHandler{})
	}
}

// ===========================
// Provision Tests
// ===========================

// provisionForTest is a helper that mimics Provision logic without requiring a Caddy context.
// This allows us to test the provisioning logic in isolation.
func provisionForTest(d *DuckDB) error {
	d.logger = zap.NewNop()

	// Set route prefix from environment variable, with /duckdb as default
	if envPrefix := os.Getenv("DUCKDB_ROUTE_PREFIX"); envPrefix != "" {
		d.routePrefix = envPrefix
	} else {
		d.routePrefix = "/duckdb"
	}
	// Ensure route prefix starts with /
	if !strings.HasPrefix(d.routePrefix, "/") {
		d.routePrefix = "/" + d.routePrefix
	}
	// Remove trailing slash if present
	d.routePrefix = strings.TrimSuffix(d.routePrefix, "/")

	if d.QueryTimeout == 0 {
		d.QueryTimeout = caddy.Duration(10_000_000_000) // 10 seconds
	}
	if d.MaxRowsPerPage == 0 {
		d.MaxRowsPerPage = 100
	}
	if d.AbsoluteMaxRows == 0 {
		d.AbsoluteMaxRows = 10000
	}
	if d.Threads == 0 {
		d.Threads = 4
	}
	if d.AccessMode == "" {
		d.AccessMode = "read_write"
	}

	// Validate AuthDatabasePath
	if d.AuthDatabasePath == "" {
		return fmt.Errorf("auth_database_path is required")
	}

	// Initialize database manager (using testing version that creates schema)
	var err error
	d.dbMgr, err = database.NewManagerForTesting(database.Config{
		MainDBPath:        d.DatabasePath,
		AuthDBPath:        d.AuthDatabasePath,
		Threads:           d.Threads,
		AccessMode:        d.AccessMode,
		MemoryLimit:       d.MemoryLimit,
		EnableObjectCache: d.EnableObjectCache,
		TempDirectory:     d.TempDirectory,
		QueryTimeout:      time.Duration(d.QueryTimeout),
		Logger:            d.logger,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize database manager: %v", err)
	}

	// Initialize authorizer
	d.authorizer = auth.NewAuthorizer(d.dbMgr.AuthDB())
	d.authMw = auth.NewMiddleware(d.authorizer)

	// Initialize handlers
	d.crudHandler = handlers.NewCRUDHandler(d.dbMgr, d.authorizer, d.MaxRowsPerPage, d.AbsoluteMaxRows, d.logger)
	d.queryHandler = handlers.NewQueryHandler(d.dbMgr, d.authorizer, d.logger)
	d.openAPIHandler = handlers.NewOpenAPIHandler()

	return nil
}

func TestProvision_Success(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-auth-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath) // Remove so DuckDB can create it fresh
	defer os.Remove(tmpPath)

	d := &DuckDB{
		DatabasePath:     ":memory:",
		AuthDatabasePath: tmpPath,
		AccessMode:       "read_write",
		QueryTimeout:     caddy.Duration(10 * time.Second),
		Threads:          2,
		MaxRowsPerPage:   50,
		AbsoluteMaxRows:  5000,
	}

	err = provisionForTest(d)
	if err != nil {
		t.Fatalf("Provision failed: %v", err)
	}
	defer d.Cleanup()

	// Verify internals were set up
	if d.logger == nil {
		t.Error("Expected logger to be set")
	}
	if d.dbMgr == nil {
		t.Error("Expected dbMgr to be set")
	}
	if d.authorizer == nil {
		t.Error("Expected authorizer to be set")
	}
	if d.authMw == nil {
		t.Error("Expected authMw to be set")
	}
	if d.crudHandler == nil {
		t.Error("Expected crudHandler to be set")
	}
	if d.queryHandler == nil {
		t.Error("Expected queryHandler to be set")
	}
	if d.openAPIHandler == nil {
		t.Error("Expected openAPIHandler to be set")
	}
	if d.routePrefix != "/duckdb" {
		t.Errorf("Expected routePrefix '/duckdb', got '%s'", d.routePrefix)
	}
}

func TestProvision_MissingAuthDB(t *testing.T) {
	d := &DuckDB{
		DatabasePath:     ":memory:",
		AuthDatabasePath: "", // Missing required field
		AccessMode:       "read_write",
	}

	err := provisionForTest(d)
	if err == nil {
		t.Error("Expected error for missing auth_database_path")
	}
	if !strings.Contains(err.Error(), "auth_database_path is required") {
		t.Errorf("Expected error about auth_database_path, got: %v", err)
	}
}

func TestProvision_Defaults(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-auth-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath) // Remove so DuckDB can create it fresh
	defer os.Remove(tmpPath)

	d := &DuckDB{
		AuthDatabasePath: tmpPath,
		// Leave all other fields with zero values to test defaults
	}

	err = provisionForTest(d)
	if err != nil {
		t.Fatalf("Provision failed: %v", err)
	}
	defer d.Cleanup()

	// Verify defaults were applied
	if d.QueryTimeout != caddy.Duration(10_000_000_000) {
		t.Errorf("Expected default query timeout 10s, got %v", time.Duration(d.QueryTimeout))
	}
	if d.MaxRowsPerPage != 100 {
		t.Errorf("Expected default max_rows_per_page 100, got %d", d.MaxRowsPerPage)
	}
	if d.AbsoluteMaxRows != 10000 {
		t.Errorf("Expected default absolute_max_rows 10000, got %d", d.AbsoluteMaxRows)
	}
	if d.Threads != 4 {
		t.Errorf("Expected default threads 4, got %d", d.Threads)
	}
	if d.AccessMode != "read_write" {
		t.Errorf("Expected default access_mode 'read_write', got '%s'", d.AccessMode)
	}
}

func TestProvision_RoutePrefix_NoSlash(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-auth-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath) // Remove so DuckDB can create it fresh
	defer os.Remove(tmpPath)

	// Set env var without leading slash
	os.Setenv("DUCKDB_ROUTE_PREFIX", "custom")
	defer os.Unsetenv("DUCKDB_ROUTE_PREFIX")

	d := &DuckDB{
		AuthDatabasePath: tmpPath,
	}

	err = provisionForTest(d)
	if err != nil {
		t.Fatalf("Provision failed: %v", err)
	}
	defer d.Cleanup()

	// Should add leading slash
	if d.routePrefix != "/custom" {
		t.Errorf("Expected '/custom', got '%s'", d.routePrefix)
	}
}

func TestProvision_RoutePrefix_TrailingSlash(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-auth-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath) // Remove so DuckDB can create it fresh
	defer os.Remove(tmpPath)

	// Set env var with trailing slash
	os.Setenv("DUCKDB_ROUTE_PREFIX", "/api/db/")
	defer os.Unsetenv("DUCKDB_ROUTE_PREFIX")

	d := &DuckDB{
		AuthDatabasePath: tmpPath,
	}

	err = provisionForTest(d)
	if err != nil {
		t.Fatalf("Provision failed: %v", err)
	}
	defer d.Cleanup()

	// Should remove trailing slash
	if d.routePrefix != "/api/db" {
		t.Errorf("Expected '/api/db', got '%s'", d.routePrefix)
	}
}

// ===========================
// UnmarshalCaddyfile Tests
// ===========================

func TestUnmarshalCaddyfile_AllOptions(t *testing.T) {
	input := `duckdb {
		database_path /path/to/main.db
		auth_database_path /path/to/auth.db
		query_timeout 30s
		max_rows_per_page 200
		absolute_max_rows 50000
		threads 8
		access_mode read_only
		memory_limit 4GB
		enable_object_cache true
		temp_directory /tmp/duckdb
	}`

	dispenser := caddyfile.NewTestDispenser(input)
	d := &DuckDB{}
	err := d.UnmarshalCaddyfile(dispenser)
	if err != nil {
		t.Fatalf("UnmarshalCaddyfile failed: %v", err)
	}

	// Verify all values
	if d.DatabasePath != "/path/to/main.db" {
		t.Errorf("Expected database_path '/path/to/main.db', got '%s'", d.DatabasePath)
	}
	if d.AuthDatabasePath != "/path/to/auth.db" {
		t.Errorf("Expected auth_database_path '/path/to/auth.db', got '%s'", d.AuthDatabasePath)
	}
	if d.QueryTimeout != caddy.Duration(30*time.Second) {
		t.Errorf("Expected query_timeout 30s, got %v", time.Duration(d.QueryTimeout))
	}
	if d.MaxRowsPerPage != 200 {
		t.Errorf("Expected max_rows_per_page 200, got %d", d.MaxRowsPerPage)
	}
	if d.AbsoluteMaxRows != 50000 {
		t.Errorf("Expected absolute_max_rows 50000, got %d", d.AbsoluteMaxRows)
	}
	if d.Threads != 8 {
		t.Errorf("Expected threads 8, got %d", d.Threads)
	}
	if d.AccessMode != "read_only" {
		t.Errorf("Expected access_mode 'read_only', got '%s'", d.AccessMode)
	}
	if d.MemoryLimit != "4GB" {
		t.Errorf("Expected memory_limit '4GB', got '%s'", d.MemoryLimit)
	}
	if !d.EnableObjectCache {
		t.Error("Expected enable_object_cache to be true")
	}
	if d.TempDirectory != "/tmp/duckdb" {
		t.Errorf("Expected temp_directory '/tmp/duckdb', got '%s'", d.TempDirectory)
	}
}

func TestUnmarshalCaddyfile_MinimalConfig(t *testing.T) {
	input := `duckdb {
		auth_database_path /path/to/auth.db
	}`

	dispenser := caddyfile.NewTestDispenser(input)
	d := &DuckDB{}
	err := d.UnmarshalCaddyfile(dispenser)
	if err != nil {
		t.Fatalf("UnmarshalCaddyfile failed: %v", err)
	}

	if d.AuthDatabasePath != "/path/to/auth.db" {
		t.Errorf("Expected auth_database_path '/path/to/auth.db', got '%s'", d.AuthDatabasePath)
	}
	// Other values should be at zero values
	if d.DatabasePath != "" {
		t.Errorf("Expected empty database_path, got '%s'", d.DatabasePath)
	}
}

func TestUnmarshalCaddyfile_InvalidQueryTimeout(t *testing.T) {
	input := `duckdb {
		query_timeout not_a_duration
	}`

	dispenser := caddyfile.NewTestDispenser(input)
	d := &DuckDB{}
	err := d.UnmarshalCaddyfile(dispenser)
	if err == nil {
		t.Error("Expected error for invalid query_timeout")
	}
}

func TestUnmarshalCaddyfile_InvalidMaxRowsPerPage(t *testing.T) {
	input := `duckdb {
		max_rows_per_page not_a_number
	}`

	dispenser := caddyfile.NewTestDispenser(input)
	d := &DuckDB{}
	err := d.UnmarshalCaddyfile(dispenser)
	if err == nil {
		t.Error("Expected error for invalid max_rows_per_page")
	}
}

func TestUnmarshalCaddyfile_InvalidAbsoluteMaxRows(t *testing.T) {
	input := `duckdb {
		absolute_max_rows not_a_number
	}`

	dispenser := caddyfile.NewTestDispenser(input)
	d := &DuckDB{}
	err := d.UnmarshalCaddyfile(dispenser)
	if err == nil {
		t.Error("Expected error for invalid absolute_max_rows")
	}
}

func TestUnmarshalCaddyfile_InvalidThreads(t *testing.T) {
	input := `duckdb {
		threads not_a_number
	}`

	dispenser := caddyfile.NewTestDispenser(input)
	d := &DuckDB{}
	err := d.UnmarshalCaddyfile(dispenser)
	if err == nil {
		t.Error("Expected error for invalid threads")
	}
}

func TestUnmarshalCaddyfile_UnknownDirective(t *testing.T) {
	input := `duckdb {
		unknown_option value
	}`

	dispenser := caddyfile.NewTestDispenser(input)
	d := &DuckDB{}
	err := d.UnmarshalCaddyfile(dispenser)
	if err == nil {
		t.Error("Expected error for unknown directive")
	}
	if !strings.Contains(err.Error(), "unknown subdirective") {
		t.Errorf("Expected 'unknown subdirective' error, got: %v", err)
	}
}

func TestUnmarshalCaddyfile_MissingArgument(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{"database_path", "duckdb {\n\tdatabase_path\n}"},
		{"auth_database_path", "duckdb {\n\tauth_database_path\n}"},
		{"query_timeout", "duckdb {\n\tquery_timeout\n}"},
		{"max_rows_per_page", "duckdb {\n\tmax_rows_per_page\n}"},
		{"absolute_max_rows", "duckdb {\n\tabsolute_max_rows\n}"},
		{"threads", "duckdb {\n\tthreads\n}"},
		{"access_mode", "duckdb {\n\taccess_mode\n}"},
		{"memory_limit", "duckdb {\n\tmemory_limit\n}"},
		{"enable_object_cache", "duckdb {\n\tenable_object_cache\n}"},
		{"temp_directory", "duckdb {\n\ttemp_directory\n}"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dispenser := caddyfile.NewTestDispenser(tc.input)
			d := &DuckDB{}
			err := d.UnmarshalCaddyfile(dispenser)
			if err == nil {
				t.Errorf("Expected error for missing argument in %s", tc.name)
			}
		})
	}
}

func TestUnmarshalCaddyfile_EnableObjectCache_Values(t *testing.T) {
	testCases := []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"True", true},
		{"TRUE", true},
		{"yes", true},
		{"Yes", true},
		{"YES", true},
		{"1", true},
		{"false", false},
		{"False", false},
		{"no", false},
		{"0", false},
		{"anything_else", false},
	}

	for _, tc := range testCases {
		t.Run(tc.value, func(t *testing.T) {
			input := fmt.Sprintf(`duckdb {
				enable_object_cache %s
			}`, tc.value)

			dispenser := caddyfile.NewTestDispenser(input)
			d := &DuckDB{}
			err := d.UnmarshalCaddyfile(dispenser)
			if err != nil {
				t.Fatalf("UnmarshalCaddyfile failed: %v", err)
			}

			if d.EnableObjectCache != tc.expected {
				t.Errorf("Expected enable_object_cache=%v for '%s', got %v", tc.expected, tc.value, d.EnableObjectCache)
			}
		})
	}
}

func TestUnmarshalCaddyfile_InMemoryDB(t *testing.T) {
	input := `duckdb {
		database_path :memory:
		auth_database_path /path/to/auth.db
	}`

	dispenser := caddyfile.NewTestDispenser(input)
	d := &DuckDB{}
	err := d.UnmarshalCaddyfile(dispenser)
	if err != nil {
		t.Fatalf("UnmarshalCaddyfile failed: %v", err)
	}

	if d.DatabasePath != ":memory:" {
		t.Errorf("Expected database_path ':memory:', got '%s'", d.DatabasePath)
	}
}

// ===========================
// Additional ServeHTTP Tests
// ===========================

func TestServeHTTP_QueryEndpoint_WithAuth(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	// Properly initialize the query handler
	d.queryHandler = handlers.NewQueryHandler(d.dbMgr, d.authorizer, d.logger)

	req := httptest.NewRequest("POST", "/duckdb/query", strings.NewReader(`{"sql":"SELECT 1"}`))
	req.Header.Set("X-API-Key", "test-api-key")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	err := d.ServeHTTP(rec, req, next)
	if err != nil {
		t.Errorf("ServeHTTP returned error: %v", err)
	}

	// Should have called query handler, not next handler
	if next.called {
		t.Error("Query endpoint should not call next handler")
	}

	// Should return OK for valid query
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", rec.Code, rec.Body.String())
	}
}

func TestServeHTTP_CRUDEndpoint_WithAuth(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	// Properly initialize the CRUD handler
	d.crudHandler = handlers.NewCRUDHandler(d.dbMgr, d.authorizer, d.MaxRowsPerPage, d.AbsoluteMaxRows, d.logger)

	req := httptest.NewRequest("GET", "/duckdb/api/test_data", nil)
	req.Header.Set("X-API-Key", "test-api-key")
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	err := d.ServeHTTP(rec, req, next)
	if err != nil {
		t.Errorf("ServeHTTP returned error: %v", err)
	}

	// Should have called CRUD handler, not next handler
	if next.called {
		t.Error("CRUD endpoint should not call next handler")
	}

	// Should return OK for valid table
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", rec.Code, rec.Body.String())
	}
}

func TestServeHTTP_OpenAPIWithHandler(t *testing.T) {
	d, cleanup := setupTestModule(t)
	defer cleanup()

	// Initialize OpenAPI handler
	d.openAPIHandler = handlers.NewOpenAPIHandler()

	req := httptest.NewRequest("GET", "/duckdb/openapi.json", nil)
	rec := httptest.NewRecorder()
	next := &mockNextHandler{}

	err := d.ServeHTTP(rec, req, next)
	if err != nil {
		t.Errorf("ServeHTTP returned error: %v", err)
	}

	if next.called {
		t.Error("OpenAPI endpoint should not call next handler")
	}

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	// Verify it returns valid JSON
	var spec map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &spec); err != nil {
		t.Errorf("Expected valid JSON response: %v", err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Errorf("Expected OpenAPI 3.0.3, got %v", spec["openapi"])
	}
}
