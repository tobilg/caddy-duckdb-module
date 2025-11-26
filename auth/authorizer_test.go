package auth

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	// Create schema
	schema := `
		CREATE TABLE IF NOT EXISTS roles (
			role_name VARCHAR PRIMARY KEY,
			description VARCHAR
		);

		CREATE TABLE IF NOT EXISTS api_keys (
			key VARCHAR PRIMARY KEY,
			role_name VARCHAR NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP,
			is_active BOOLEAN DEFAULT true,
			FOREIGN KEY (role_name) REFERENCES roles(role_name)
		);

		CREATE TABLE IF NOT EXISTS permissions (
			id INTEGER PRIMARY KEY,
			role_name VARCHAR NOT NULL,
			table_name VARCHAR NOT NULL,
			can_create BOOLEAN DEFAULT false,
			can_read BOOLEAN DEFAULT false,
			can_update BOOLEAN DEFAULT false,
			can_delete BOOLEAN DEFAULT false,
			can_query BOOLEAN DEFAULT false,
			FOREIGN KEY (role_name) REFERENCES roles(role_name),
			UNIQUE(role_name, table_name)
		);

		CREATE SEQUENCE IF NOT EXISTS permissions_id_seq START 1;

		INSERT INTO roles (role_name, description)
		VALUES ('admin', 'Full access');

		INSERT INTO roles (role_name, description)
		VALUES ('reader', 'Read-only access');
	`

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	return db
}

func TestAuthenticateAPIKey_Valid(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	// Create a test API key
	testKey := "test-api-key-12345"
	err := auth.CreateAPIKey(testKey, "admin", nil)
	if err != nil {
		t.Fatalf("Failed to create API key: %v", err)
	}

	// Test authentication
	apiKey, err := auth.AuthenticateAPIKey(testKey)
	if err != nil {
		t.Fatalf("Expected authentication to succeed, got error: %v", err)
	}

	if apiKey.Key != testKey {
		t.Errorf("Expected key %s, got %s", testKey, apiKey.Key)
	}

	if apiKey.RoleName != "admin" {
		t.Errorf("Expected role admin, got %s", apiKey.RoleName)
	}

	if !apiKey.IsActive {
		t.Error("Expected key to be active")
	}
}

func TestAuthenticateAPIKey_Invalid(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	// Test with non-existent key
	_, err := auth.AuthenticateAPIKey("non-existent-key")
	if err == nil {
		t.Error("Expected authentication to fail for non-existent key")
	}
}

func TestAuthenticateAPIKey_Expired(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	// Create an expired API key
	testKey := "expired-key-12345"
	pastTime := time.Now().Add(-24 * time.Hour)
	err := auth.CreateAPIKey(testKey, "admin", &pastTime)
	if err != nil {
		t.Fatalf("Failed to create API key: %v", err)
	}

	// Test authentication should fail
	_, err = auth.AuthenticateAPIKey(testKey)
	if err == nil {
		t.Error("Expected authentication to fail for expired key")
	}
}

func TestAuthenticateAPIKey_Inactive(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	// Create and then revoke a key
	testKey := "revoked-key-12345"
	err := auth.CreateAPIKey(testKey, "admin", nil)
	if err != nil {
		t.Fatalf("Failed to create API key: %v", err)
	}

	err = auth.RevokeAPIKey(testKey)
	if err != nil {
		t.Fatalf("Failed to revoke API key: %v", err)
	}

	// Test authentication should fail
	_, err = auth.AuthenticateAPIKey(testKey)
	if err == nil {
		t.Error("Expected authentication to fail for revoked key")
	}
}

func TestCreateAPIKey(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	testKey := "new-api-key-12345"
	err := auth.CreateAPIKey(testKey, "admin", nil)
	if err != nil {
		t.Fatalf("Failed to create API key: %v", err)
	}

	// Verify it was created
	apiKey, err := auth.AuthenticateAPIKey(testKey)
	if err != nil {
		t.Fatalf("Failed to retrieve created API key: %v", err)
	}

	if apiKey.Key != testKey {
		t.Errorf("Expected key %s, got %s", testKey, apiKey.Key)
	}
}

func TestRevokeAPIKey(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	testKey := "key-to-revoke-12345"
	err := auth.CreateAPIKey(testKey, "admin", nil)
	if err != nil {
		t.Fatalf("Failed to create API key: %v", err)
	}

	// Revoke the key
	err = auth.RevokeAPIKey(testKey)
	if err != nil {
		t.Fatalf("Failed to revoke API key: %v", err)
	}

	// Verify it can't be used
	_, err = auth.AuthenticateAPIKey(testKey)
	if err == nil {
		t.Error("Expected authentication to fail after revocation")
	}
}

func TestRevokeAPIKey_NonExistent(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	// Try to revoke non-existent key
	err := auth.RevokeAPIKey("non-existent-key")
	if err == nil {
		t.Error("Expected error when revoking non-existent key")
	}
}

func TestCheckPermission_Admin(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	// Add admin permission
	_, err := db.Exec(`
		INSERT INTO permissions (id, role_name, table_name, can_create, can_read, can_update, can_delete, can_query)
		VALUES (nextval('permissions_id_seq'), 'admin', '*', true, true, true, true, true)
	`)
	if err != nil {
		t.Fatalf("Failed to insert permission: %v", err)
	}

	// Test all operations
	operations := []Operation{OperationCreate, OperationRead, OperationUpdate, OperationDelete, OperationQuery}
	for _, op := range operations {
		allowed, err := auth.CheckPermission("admin", "users", op)
		if err != nil {
			t.Errorf("Failed to check permission for %s: %v", op, err)
		}
		if !allowed {
			t.Errorf("Expected admin to have %s permission", op)
		}
	}
}

func TestCheckPermission_Reader(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	// Add reader permission (read-only)
	_, err := db.Exec(`
		INSERT INTO permissions (id, role_name, table_name, can_create, can_read, can_update, can_delete, can_query)
		VALUES (nextval('permissions_id_seq'), 'reader', '*', false, true, false, false, false)
	`)
	if err != nil {
		t.Fatalf("Failed to insert permission: %v", err)
	}

	// Should have read permission
	allowed, err := auth.CheckPermission("reader", "users", OperationRead)
	if err != nil {
		t.Fatalf("Failed to check permission: %v", err)
	}
	if !allowed {
		t.Error("Expected reader to have read permission")
	}

	// Should not have write permissions
	writeOps := []Operation{OperationCreate, OperationUpdate, OperationDelete, OperationQuery}
	for _, op := range writeOps {
		allowed, err := auth.CheckPermission("reader", "users", op)
		if err != nil {
			t.Errorf("Failed to check permission for %s: %v", op, err)
		}
		if allowed {
			t.Errorf("Expected reader to not have %s permission", op)
		}
	}
}

func TestCreateRole(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	err := auth.CreateRole("custom", "Custom role for testing")
	if err != nil {
		t.Fatalf("Failed to create role: %v", err)
	}

	// Verify role exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM roles WHERE role_name = ?", "custom").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query role: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 role, got %d", count)
	}
}

func TestCreatePermission(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	perm := Permission{
		RoleName:  "admin",
		TableName: "users",
		CanCreate: true,
		CanRead:   true,
		CanUpdate: true,
		CanDelete: false,
		CanQuery:  false,
	}

	err := auth.CreatePermission(perm)
	if err != nil {
		t.Fatalf("Failed to create permission: %v", err)
	}

	// Verify permission exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM permissions WHERE role_name = ? AND table_name = ?", "admin", "users").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query permission: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 permission, got %d", count)
	}
}

func TestGetPermissions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	auth := NewAuthorizer(db)

	// Add some permissions
	_, err := db.Exec(`
		INSERT INTO permissions (id, role_name, table_name, can_create, can_read, can_update, can_delete, can_query)
		VALUES
			(nextval('permissions_id_seq'), 'admin', 'users', true, true, true, true, true),
			(nextval('permissions_id_seq'), 'admin', 'posts', true, true, true, true, true)
	`)
	if err != nil {
		t.Fatalf("Failed to insert permissions: %v", err)
	}

	perms, err := auth.GetPermissions("admin")
	if err != nil {
		t.Fatalf("Failed to get permissions: %v", err)
	}

	if len(perms) != 2 {
		t.Errorf("Expected 2 permissions, got %d", len(perms))
	}
}
