package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// contextKey is a custom type for context keys to avoid collisions.
type contextKey string

const (
	// ContextKeyAPIKey is the context key for the authenticated API key.
	ContextKeyAPIKey contextKey = "api_key"
	// ContextKeyRole is the context key for the user's role.
	ContextKeyRole contextKey = "role"
	// ContextKeyRequestID is the context key for the request ID (for tracing).
	ContextKeyRequestID contextKey = "request_id"
)

// Middleware provides authentication and authorization middleware.
type Middleware struct {
	authorizer *Authorizer
}

// NewMiddleware creates a new auth middleware.
func NewMiddleware(authorizer *Authorizer) *Middleware {
	return &Middleware{
		authorizer: authorizer,
	}
}

// Authenticate extracts and validates the API key from the request.
func (m *Middleware) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract API key from header
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			m.sendError(w, "Missing X-API-Key header", http.StatusUnauthorized)
			return
		}

		// Validate API key
		key, err := m.authorizer.AuthenticateAPIKey(apiKey)
		if err != nil {
			m.sendError(w, "Invalid or expired API key", http.StatusUnauthorized)
			return
		}

		// Add API key and role to context
		ctx := context.WithValue(r.Context(), ContextKeyAPIKey, key)
		ctx = context.WithValue(ctx, ContextKeyRole, key.RoleName)

		// Call next handler
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Authorize checks if the user has permission to perform the requested operation.
func (m *Middleware) Authorize(tableName string, operation Operation) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get role from context
			role, ok := r.Context().Value(ContextKeyRole).(string)
			if !ok {
				m.sendError(w, "Unauthorized: no role found", http.StatusUnauthorized)
				return
			}

			// Check permission
			allowed, err := m.authorizer.CheckPermission(role, tableName, operation)
			if err != nil {
				m.sendError(w, "Failed to check permissions", http.StatusInternalServerError)
				return
			}

			if !allowed {
				m.sendError(w, "Forbidden: insufficient permissions", http.StatusForbidden)
				return
			}

			// Call next handler
			next.ServeHTTP(w, r)
		})
	}
}

// sendError sends a JSON error response.
func (m *Middleware) sendError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":   http.StatusText(statusCode),
		"message": message,
		"code":    statusCode,
	})
}

// GetRoleFromContext retrieves the role from the request context.
func GetRoleFromContext(ctx context.Context) string {
	role, _ := ctx.Value(ContextKeyRole).(string)
	return role
}

// GetAPIKeyFromContext retrieves the API key from the request context.
func GetAPIKeyFromContext(ctx context.Context) *APIKey {
	key, _ := ctx.Value(ContextKeyAPIKey).(*APIKey)
	return key
}

// ExtractTableName extracts the table name from the request path.
// Expects paths like /duckdb/api/{table}
func ExtractTableName(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "duckdb" && parts[1] == "api" {
		return parts[2]
	}
	return ""
}

// IsInternalTable checks if a table is an internal auth table.
func IsInternalTable(tableName string) bool {
	internalTables := map[string]bool{
		"api_keys":    true,
		"roles":       true,
		"permissions": true,
	}
	return internalTables[tableName]
}

// SetContextValues sets the API key and role in the context.
func SetContextValues(ctx context.Context, apiKey *APIKey, role string) context.Context {
	ctx = context.WithValue(ctx, ContextKeyAPIKey, apiKey)
	ctx = context.WithValue(ctx, ContextKeyRole, role)
	return ctx
}

// SetRequestID sets the request ID in the context.
func SetRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, ContextKeyRequestID, requestID)
}

// GetRequestIDFromContext retrieves the request ID from the request context.
func GetRequestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(ContextKeyRequestID).(string)
	return requestID
}
