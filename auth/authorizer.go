package auth

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// Default cache TTL - permissions and API keys are re-validated after this duration
// This provides a safety net even if cache invalidation is missed
const defaultCacheTTL = 5 * time.Minute

// Authorizer handles authentication and authorization.
type Authorizer struct {
	authDB          *sql.DB
	permissionCache *expirable.LRU[string, bool]
	apiKeyCache     *expirable.LRU[string, *APIKey]
}

// NewAuthorizer creates a new authorizer with permission and API key caching.
// Cache entries expire after 5 minutes as a safety net.
func NewAuthorizer(authDB *sql.DB) *Authorizer {
	// Create expirable LRU cache with capacity for 1000 permission entries
	// and a 5-minute TTL for safety
	// This covers typical scenarios: ~10 roles * ~20 tables * 5 operations = 1000 entries
	permCache := expirable.NewLRU[string, bool](1000, nil, defaultCacheTTL)

	// Create expirable LRU cache for API keys
	// Capacity of 500 should be sufficient for most deployments
	apiKeyCache := expirable.NewLRU[string, *APIKey](500, nil, defaultCacheTTL)

	return &Authorizer{
		authDB:          authDB,
		permissionCache: permCache,
		apiKeyCache:     apiKeyCache,
	}
}

// NewAuthorizerWithTTL creates a new authorizer with custom cache TTL.
func NewAuthorizerWithTTL(authDB *sql.DB, cacheTTL time.Duration) *Authorizer {
	permCache := expirable.NewLRU[string, bool](1000, nil, cacheTTL)
	apiKeyCache := expirable.NewLRU[string, *APIKey](500, nil, cacheTTL)

	return &Authorizer{
		authDB:          authDB,
		permissionCache: permCache,
		apiKeyCache:     apiKeyCache,
	}
}

// AuthenticateAPIKey validates an API key and returns the associated role.
// Results are cached in memory for performance - cache is invalidated on API key changes.
func (a *Authorizer) AuthenticateAPIKey(apiKey string) (*APIKey, error) {
	// Check cache first
	if cached, ok := a.apiKeyCache.Get(apiKey); ok {
		// Re-check expiration on cached keys (time may have passed since caching)
		if cached.ExpiresAt != nil && cached.ExpiresAt.Before(time.Now()) {
			// Key has expired since caching, remove from cache and return error
			a.apiKeyCache.Remove(apiKey)
			return nil, fmt.Errorf("API key has expired")
		}
		return cached, nil
	}

	// Cache miss - query database
	key, err := a.authenticateAPIKeyDB(apiKey)
	if err != nil {
		return nil, err
	}

	// Store in cache
	a.apiKeyCache.Add(apiKey, key)

	return key, nil
}

// authenticateAPIKeyDB performs the actual database lookup for API key authentication.
func (a *Authorizer) authenticateAPIKeyDB(apiKey string) (*APIKey, error) {
	query := `
		SELECT key, role_name, created_at, expires_at, is_active
		FROM api_keys
		WHERE key = $1 AND is_active = true
	`

	var key APIKey
	var expiresAt sql.NullTime

	err := a.authDB.QueryRow(query, apiKey).Scan(
		&key.Key,
		&key.RoleName,
		&key.CreatedAt,
		&expiresAt,
		&key.IsActive,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("invalid API key")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query API key: %w", err)
	}

	// Check expiration
	if expiresAt.Valid && expiresAt.Time.Before(time.Now()) {
		return nil, fmt.Errorf("API key has expired")
	}

	if expiresAt.Valid {
		key.ExpiresAt = &expiresAt.Time
	}

	return &key, nil
}

// CheckPermission checks if a role has permission to perform an operation on a table.
// Results are cached in memory for performance - cache is invalidated on permission changes.
func (a *Authorizer) CheckPermission(roleName string, tableName string, operation Operation) (bool, error) {
	// Generate cache key
	cacheKey := fmt.Sprintf("%s:%s:%s", roleName, tableName, operation)

	// Check cache first
	if cached, ok := a.permissionCache.Get(cacheKey); ok {
		return cached, nil
	}

	// Cache miss - query database
	allowed, err := a.checkPermissionDB(roleName, tableName, operation)
	if err != nil {
		return false, err
	}

	// Store in cache
	a.permissionCache.Add(cacheKey, allowed)

	return allowed, nil
}

// checkPermissionDB performs the actual database lookup for permissions.
func (a *Authorizer) checkPermissionDB(roleName string, tableName string, operation Operation) (bool, error) {
	query := `
		SELECT can_create, can_read, can_update, can_delete, can_query
		FROM permissions
		WHERE role_name = $1 AND (table_name = $2 OR table_name = '*')
		ORDER BY CASE WHEN table_name = $2 THEN 1 ELSE 2 END
		LIMIT 1
	`

	var perm Permission
	err := a.authDB.QueryRow(query, roleName, tableName).Scan(
		&perm.CanCreate,
		&perm.CanRead,
		&perm.CanUpdate,
		&perm.CanDelete,
		&perm.CanQuery,
	)

	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to query permissions: %w", err)
	}

	// Check the specific operation
	switch operation {
	case OperationCreate:
		return perm.CanCreate, nil
	case OperationRead:
		return perm.CanRead, nil
	case OperationUpdate:
		return perm.CanUpdate, nil
	case OperationDelete:
		return perm.CanDelete, nil
	case OperationQuery:
		return perm.CanQuery, nil
	default:
		return false, fmt.Errorf("unknown operation: %s", operation)
	}
}

// InvalidatePermissionCache clears the permission cache.
// Call this when permissions are modified to ensure cache consistency.
func (a *Authorizer) InvalidatePermissionCache() {
	a.permissionCache.Purge()
}

// InvalidateAPIKeyCache clears the entire API key cache.
// Call this when API keys are modified to ensure cache consistency.
func (a *Authorizer) InvalidateAPIKeyCache() {
	a.apiKeyCache.Purge()
}

// InvalidateAPIKey removes a specific API key from the cache.
// More efficient than purging the entire cache when only one key changes.
func (a *Authorizer) InvalidateAPIKey(apiKey string) {
	a.apiKeyCache.Remove(apiKey)
}

// CreateAPIKey creates a new API key with the specified role.
func (a *Authorizer) CreateAPIKey(apiKey, roleName string, expiresAt *time.Time) error {
	query := `
		INSERT INTO api_keys (key, role_name, expires_at)
		VALUES ($1, $2, $3)
	`

	_, err := a.authDB.Exec(query, apiKey, roleName, expiresAt)
	if err != nil {
		return fmt.Errorf("failed to create API key: %w", err)
	}

	return nil
}

// RevokeAPIKey revokes an API key by setting is_active to false.
// Invalidates the specific API key from cache.
func (a *Authorizer) RevokeAPIKey(apiKey string) error {
	query := `
		UPDATE api_keys
		SET is_active = false
		WHERE key = $1
	`

	result, err := a.authDB.Exec(query, apiKey)
	if err != nil {
		return fmt.Errorf("failed to revoke API key: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("API key not found")
	}

	// Invalidate the specific key from cache
	a.InvalidateAPIKey(apiKey)

	return nil
}

// CreateRole creates a new role.
func (a *Authorizer) CreateRole(roleName, description string) error {
	query := `
		INSERT INTO roles (role_name, description)
		VALUES ($1, $2)
	`

	_, err := a.authDB.Exec(query, roleName, description)
	if err != nil {
		return fmt.Errorf("failed to create role: %w", err)
	}

	return nil
}

// CreatePermission creates a new permission and invalidates the cache.
func (a *Authorizer) CreatePermission(perm Permission) error {
	query := `
		INSERT INTO permissions (id, role_name, table_name, can_create, can_read, can_update, can_delete, can_query)
		VALUES (nextval('permissions_id_seq'), $1, $2, $3, $4, $5, $6, $7)
	`

	_, err := a.authDB.Exec(query,
		perm.RoleName,
		perm.TableName,
		perm.CanCreate,
		perm.CanRead,
		perm.CanUpdate,
		perm.CanDelete,
		perm.CanQuery,
	)
	if err != nil {
		return fmt.Errorf("failed to create permission: %w", err)
	}

	// Invalidate cache since permissions have changed
	a.InvalidatePermissionCache()

	return nil
}

// UpdatePermission updates an existing permission and invalidates the cache.
func (a *Authorizer) UpdatePermission(perm Permission) error {
	query := `
		UPDATE permissions
		SET can_create = $1, can_read = $2, can_update = $3, can_delete = $4, can_query = $5
		WHERE role_name = $6 AND table_name = $7
	`

	result, err := a.authDB.Exec(query,
		perm.CanCreate,
		perm.CanRead,
		perm.CanUpdate,
		perm.CanDelete,
		perm.CanQuery,
		perm.RoleName,
		perm.TableName,
	)
	if err != nil {
		return fmt.Errorf("failed to update permission: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("permission not found for role '%s' and table '%s'", perm.RoleName, perm.TableName)
	}

	// Invalidate cache since permissions have changed
	a.InvalidatePermissionCache()

	return nil
}

// DeletePermission deletes a permission and invalidates the cache.
func (a *Authorizer) DeletePermission(roleName, tableName string) error {
	query := `
		DELETE FROM permissions
		WHERE role_name = $1 AND table_name = $2
	`

	result, err := a.authDB.Exec(query, roleName, tableName)
	if err != nil {
		return fmt.Errorf("failed to delete permission: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("permission not found for role '%s' and table '%s'", roleName, tableName)
	}

	// Invalidate cache since permissions have changed
	a.InvalidatePermissionCache()

	return nil
}

// DeleteRole deletes a role and all its permissions, then invalidates the caches.
func (a *Authorizer) DeleteRole(roleName string) error {
	// First delete all permissions for this role
	permQuery := `DELETE FROM permissions WHERE role_name = $1`
	_, err := a.authDB.Exec(permQuery, roleName)
	if err != nil {
		return fmt.Errorf("failed to delete role permissions: %w", err)
	}

	// Then delete all API keys for this role
	keyQuery := `DELETE FROM api_keys WHERE role_name = $1`
	_, err = a.authDB.Exec(keyQuery, roleName)
	if err != nil {
		return fmt.Errorf("failed to delete role API keys: %w", err)
	}

	// Finally delete the role itself
	roleQuery := `DELETE FROM roles WHERE role_name = $1`
	result, err := a.authDB.Exec(roleQuery, roleName)
	if err != nil {
		return fmt.Errorf("failed to delete role: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("role '%s' not found", roleName)
	}

	// Invalidate caches since permissions and API keys have changed
	a.InvalidatePermissionCache()
	a.InvalidateAPIKeyCache() // Purge all since we don't know which keys belonged to this role

	return nil
}

// GetPermissions returns all permissions for a role.
func (a *Authorizer) GetPermissions(roleName string) ([]Permission, error) {
	query := `
		SELECT id, role_name, table_name, can_create, can_read, can_update, can_delete, can_query
		FROM permissions
		WHERE role_name = $1
	`

	rows, err := a.authDB.Query(query, roleName)
	if err != nil {
		return nil, fmt.Errorf("failed to query permissions: %w", err)
	}
	defer rows.Close()

	var permissions []Permission
	for rows.Next() {
		var perm Permission
		err := rows.Scan(
			&perm.ID,
			&perm.RoleName,
			&perm.TableName,
			&perm.CanCreate,
			&perm.CanRead,
			&perm.CanUpdate,
			&perm.CanDelete,
			&perm.CanQuery,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan permission: %w", err)
		}
		permissions = append(permissions, perm)
	}

	return permissions, nil
}
