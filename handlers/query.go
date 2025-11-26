package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/tobilg/caddyserver-duckdb-module/auth"
	"github.com/tobilg/caddyserver-duckdb-module/database"
	"github.com/tobilg/caddyserver-duckdb-module/formats"
	"go.uber.org/zap"
)

// QueryHandler handles raw SQL query execution.
type QueryHandler struct {
	dbMgr      *database.Manager
	authorizer *auth.Authorizer
	logger     *zap.Logger
}

// NewQueryHandler creates a new query handler.
func NewQueryHandler(dbMgr *database.Manager, authorizer *auth.Authorizer, logger *zap.Logger) *QueryHandler {
	return &QueryHandler{
		dbMgr:      dbMgr,
		authorizer: authorizer,
		logger:     logger,
	}
}

// ServeHTTP handles HTTP requests for raw SQL queries.
// Supports both POST (with JSON body) and GET (with URL-encoded SQL in path).
func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := auth.GetRequestIDFromContext(r.Context())

	// Check authorization for raw SQL queries
	role := auth.GetRoleFromContext(r.Context())
	allowed, err := h.authorizer.CheckPermission(role, "*", auth.OperationQuery)
	if err != nil {
		h.logger.Error("Failed to check permission", zap.Error(err), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, "Failed to check permission", http.StatusInternalServerError)
		return
	}
	if !allowed {
		h.sendErrorWithRequest(w, r, "Forbidden: insufficient permissions for raw SQL queries", http.StatusForbidden)
		return
	}

	var sqlQuery string
	var params []interface{}
	var format string

	// Handle different HTTP methods
	switch r.Method {
	case http.MethodPost:
		// POST request with JSON body
		defer r.Body.Close()

		var req struct {
			SQL    string        `json:"sql"`
			Params []interface{} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.sendErrorWithRequest(w, r, "Invalid JSON in request body", http.StatusBadRequest)
			return
		}

		if req.SQL == "" {
			h.sendErrorWithRequest(w, r, "SQL query is required", http.StatusBadRequest)
			return
		}

		sqlQuery = req.SQL
		params = req.Params
		format = GetAcceptFormat(r)

	case http.MethodGet:
		// GET request with URL-encoded SQL in path
		// Pattern: /duckdb/query/{urlEncodedSQL}/result.{format}
		parsedSQL, parsedFormat, err := ParseGETQueryPath(r.URL.Path)
		if err != nil {
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid GET query path: %s", err.Error()), http.StatusBadRequest)
			return
		}

		sqlQuery = parsedSQL
		params = nil // GET requests don't support parameterized queries
		format = parsedFormat

	default:
		h.sendErrorWithRequest(w, r, "Method not allowed. Use POST or GET to execute queries.", http.StatusMethodNotAllowed)
		return
	}

	// Prevent access to internal auth tables
	if h.containsInternalTables(sqlQuery) {
		h.sendErrorWithRequest(w, r, "Access to internal auth tables is forbidden", http.StatusForbidden)
		return
	}

	// Log the query (be careful with sensitive data in production)
	h.logger.Info("Executing query",
		zap.String("role", role),
		zap.String("method", r.Method),
		zap.String("sql", sqlQuery),
		zap.String("format", format),
		zap.String("request_id", requestID),
	)

	// Execute query with read-write separation for optimal performance
	// Read-only queries (SELECT) don't use transactions, while write queries use ExecMain
	startTime := time.Now()

	if h.isSelectQuery(sqlQuery) {
		// Read-only query - use QueryMain for better concurrency (no transaction overhead)
		rows, err := h.dbMgr.QueryMain(sqlQuery, params...)
		_ = time.Since(startTime) // execution time tracked but not used in response

		if err != nil {
			h.logger.Error("Failed to execute query", zap.Error(err), zap.String("sql", sqlQuery), zap.String("request_id", requestID))
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Query execution failed: %s", err.Error()), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Format and return results (same format as /api endpoint)
		if err := h.formatQueryResponse(w, rows, format); err != nil {
			h.logger.Error("Failed to format response", zap.Error(err), zap.String("request_id", requestID))
			h.sendErrorWithRequest(w, r, "Failed to format response", http.StatusInternalServerError)
		}
	} else {
		// Write query (INSERT, UPDATE, DELETE, CREATE, etc.)
		// Only allowed for POST requests to prevent accidental modifications via GET
		if r.Method == http.MethodGet {
			h.sendErrorWithRequest(w, r, "GET requests can only execute read-only queries (SELECT, SHOW, DESCRIBE, EXPLAIN)", http.StatusMethodNotAllowed)
			return
		}

		// Use ExecMain for write queries
		result, err := h.dbMgr.ExecMain(sqlQuery, params...)
		executionTime := time.Since(startTime)

		if err != nil {
			h.logger.Error("Failed to execute DML query", zap.Error(err), zap.String("sql", sqlQuery), zap.String("request_id", requestID))
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Query execution failed: %s", err.Error()), http.StatusInternalServerError)
			return
		}

		rowsAffected, _ := result.RowsAffected()
		h.sendDMLResponseWithRequest(w, r, rowsAffected, executionTime)
	}
}

// formatQueryResponse formats the query result.
// Uses the same JSON format as the CRUD /api endpoint for consistency.
func (h *QueryHandler) formatQueryResponse(w http.ResponseWriter, rows *sql.Rows, format string) error {
	switch format {
	case "csv":
		return formats.WriteCSV(w, rows)
	case "json":
		// Use same format as /api endpoint: data as array of objects, no pagination
		return formats.WriteJSON(w, rows, 1, 0, 0, false, 0, nil)
	case "parquet":
		return formats.WriteParquet(w, rows)
	case "arrow":
		return formats.WriteArrowIPC(w, rows)
	default:
		// Use same format as /api endpoint: data as array of objects, no pagination
		return formats.WriteJSON(w, rows, 1, 0, 0, false, 0, nil)
	}
}

// sendDMLResponseWithRequest sends a response for DML queries.
// The request ID is available in the X-Request-ID response header.
func (h *QueryHandler) sendDMLResponseWithRequest(w http.ResponseWriter, r *http.Request, rowsAffected int64, executionTime time.Duration) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":           true,
		"rows_affected":     rowsAffected,
		"execution_time_ms": executionTime.Milliseconds(),
	})
}

// sendErrorWithRequest sends an error response.
// The request ID is available in the X-Request-ID response header.
func (h *QueryHandler) sendErrorWithRequest(w http.ResponseWriter, r *http.Request, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":   http.StatusText(statusCode),
		"message": message,
		"code":    statusCode,
	})
}

// isSelectQuery checks if the SQL query is a SELECT query.
func (h *QueryHandler) isSelectQuery(sql string) bool {
	trimmed := strings.TrimSpace(strings.ToUpper(sql))
	return strings.HasPrefix(trimmed, "SELECT") ||
		strings.HasPrefix(trimmed, "WITH") ||
		strings.HasPrefix(trimmed, "SHOW") ||
		strings.HasPrefix(trimmed, "DESCRIBE") ||
		strings.HasPrefix(trimmed, "EXPLAIN")
}

// Pre-compiled regexes for internal table protection (compiled once at package init)
var (
	// SQL comment patterns
	blockCommentRegex = regexp.MustCompile(`/\*[\s\S]*?\*/`)
	lineCommentRegex  = regexp.MustCompile(`--[^\n]*`)
	whitespaceRegex   = regexp.MustCompile(`\s+`)

	// Internal table patterns with word boundaries
	internalTablePatterns = []*regexp.Regexp{
		regexp.MustCompile(`\bapi_keys\b`),
		regexp.MustCompile(`\broles\b`),
		regexp.MustCompile(`\bpermissions\b`),
	}
)

// containsInternalTables checks if the SQL query references internal auth tables.
// Uses comment stripping and word-boundary matching to prevent bypass attempts
// like SQL comments (api/**/keys) or whitespace variations.
func (h *QueryHandler) containsInternalTables(sql string) bool {
	// Strip SQL comments to prevent bypass via api/**/keys or similar
	cleaned := stripSQLComments(sql)

	// Normalize whitespace (collapse multiple spaces/tabs/newlines into single space)
	cleaned = whitespaceRegex.ReplaceAllString(cleaned, " ")

	// Convert to lowercase for case-insensitive matching
	lowerSQL := strings.ToLower(cleaned)

	// Check against pre-compiled patterns
	for _, pattern := range internalTablePatterns {
		if pattern.MatchString(lowerSQL) {
			return true
		}
	}

	return false
}

// stripSQLComments removes SQL comments from a query string.
// Handles both block comments (/* ... */) and line comments (-- ...).
func stripSQLComments(sql string) string {
	// Remove block comments /* ... */ (non-greedy to handle multiple comments)
	sql = blockCommentRegex.ReplaceAllString(sql, " ")

	// Remove line comments -- ... (everything from -- to end of line)
	sql = lineCommentRegex.ReplaceAllString(sql, " ")

	return sql
}
