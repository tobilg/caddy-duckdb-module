package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/tobilg/caddyserver-duckdb-module/auth"
	"github.com/tobilg/caddyserver-duckdb-module/database"
	"github.com/tobilg/caddyserver-duckdb-module/formats"
	"go.uber.org/zap"
)

// CRUDHandler handles CRUD operations on tables.
type CRUDHandler struct {
	dbMgr           *database.Manager
	authorizer      *auth.Authorizer
	maxRowsPerPage  int
	absoluteMaxRows int
	logger          *zap.Logger
}

// NewCRUDHandler creates a new CRUD handler.
func NewCRUDHandler(dbMgr *database.Manager, authorizer *auth.Authorizer, maxRowsPerPage int, absoluteMaxRows int, logger *zap.Logger) *CRUDHandler {
	return &CRUDHandler{
		dbMgr:           dbMgr,
		authorizer:      authorizer,
		maxRowsPerPage:  maxRowsPerPage,
		absoluteMaxRows: absoluteMaxRows,
		logger:          logger,
	}
}

// ServeHTTP handles HTTP requests for CRUD operations.
func (h *CRUDHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := auth.GetRequestIDFromContext(r.Context())

	// Extract table name from path: /duckdb/api/{table}
	tableName := auth.ExtractTableName(r.URL.Path)
	if tableName == "" {
		h.sendErrorWithRequest(w, r, "Invalid path: table name required", http.StatusBadRequest)
		return
	}

	// Sanitize table name
	if err := SanitizeTableName(tableName); err != nil {
		h.sendErrorWithRequest(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	// Block access to internal auth tables
	if auth.IsInternalTable(tableName) {
		h.sendErrorWithRequest(w, r, "Access to internal tables is forbidden", http.StatusForbidden)
		return
	}

	// Check if table exists
	exists, err := h.dbMgr.TableExists(tableName)
	if err != nil {
		h.logger.Error("Failed to check table existence", zap.Error(err), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, "Failed to check table existence", http.StatusInternalServerError)
		return
	}
	if !exists {
		h.sendErrorWithRequest(w, r, fmt.Sprintf("Table '%s' does not exist", tableName), http.StatusNotFound)
		return
	}

	// Route based on HTTP method
	switch r.Method {
	case http.MethodPost:
		h.handleCreate(w, r, tableName)
	case http.MethodGet:
		h.handleRead(w, r, tableName)
	case http.MethodPut:
		h.handleUpdate(w, r, tableName)
	case http.MethodDelete:
		h.handleDelete(w, r, tableName)
	default:
		h.sendErrorWithRequest(w, r, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCreate handles INSERT operations.
func (h *CRUDHandler) handleCreate(w http.ResponseWriter, r *http.Request, tableName string) {
	requestID := auth.GetRequestIDFromContext(r.Context())

	// Check authorization
	role := auth.GetRoleFromContext(r.Context())
	allowed, err := h.authorizer.CheckPermission(role, tableName, auth.OperationCreate)
	if err != nil {
		h.logger.Error("Failed to check permission", zap.Error(err), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, "Failed to check permission", http.StatusInternalServerError)
		return
	}
	if !allowed {
		h.sendErrorWithRequest(w, r, "Forbidden: insufficient permissions for CREATE operation", http.StatusForbidden)
		return
	}

	// Parse request body using streaming decoder for better performance
	defer r.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		h.sendErrorWithRequest(w, r, "Invalid JSON in request body", http.StatusBadRequest)
		return
	}

	// Validate column names
	for col := range data {
		if err := SanitizeColumnName(col); err != nil {
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid column name '%s': %s", col, err.Error()), http.StatusBadRequest)
			return
		}
	}

	// Execute insert
	result, err := h.dbMgr.Insert(tableName, data)
	if err != nil {
		h.logger.Error("Failed to insert data", zap.Error(err), zap.String("table", tableName), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, fmt.Sprintf("Failed to insert data: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	h.sendSuccessWithRequest(w, r, result.RowsAffected, http.StatusCreated)
}

// handleRead handles SELECT operations.
func (h *CRUDHandler) handleRead(w http.ResponseWriter, r *http.Request, tableName string) {
	requestID := auth.GetRequestIDFromContext(r.Context())

	// Check authorization
	role := auth.GetRoleFromContext(r.Context())
	allowed, err := h.authorizer.CheckPermission(role, tableName, auth.OperationRead)
	if err != nil {
		h.logger.Error("Failed to check permission", zap.Error(err), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, "Failed to check permission", http.StatusInternalServerError)
		return
	}
	if !allowed {
		h.sendErrorWithRequest(w, r, "Forbidden: insufficient permissions for READ operation", http.StatusForbidden)
		return
	}

	// Parse pagination
	limit, offset, page, paginationRequested := ParsePagination(r, h.maxRowsPerPage, h.absoluteMaxRows)

	// Apply safety limit if pagination not requested and absoluteMaxRows is configured
	safetyLimit := limit
	if !paginationRequested && h.absoluteMaxRows > 0 {
		safetyLimit = h.absoluteMaxRows
	}

	// Parse filters
	filters, err := ParseFilters(r)
	if err != nil {
		h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid filters: %s", err.Error()), http.StatusBadRequest)
		return
	}

	// Validate filter column names
	for _, f := range filters {
		if err := SanitizeColumnName(f.Column); err != nil {
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid filter column '%s': %s", f.Column, err.Error()), http.StatusBadRequest)
			return
		}
	}

	// Parse sorts
	sorts, err := ParseSorts(r)
	if err != nil {
		h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid sort: %s", err.Error()), http.StatusBadRequest)
		return
	}

	// Validate sort column names
	for _, s := range sorts {
		if err := SanitizeColumnName(s.Column); err != nil {
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid sort column '%s': %s", s.Column, err.Error()), http.StatusBadRequest)
			return
		}
	}

	// Execute query with safety limit
	rows, err := h.dbMgr.Select(tableName, filters, sorts, safetyLimit, offset)
	if err != nil {
		h.logger.Error("Failed to query data", zap.Error(err), zap.String("table", tableName), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, fmt.Sprintf("Failed to query data: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Get total count for pagination
	totalRows, err := h.dbMgr.Count(tableName, filters)
	if err != nil {
		h.logger.Error("Failed to count rows", zap.Error(err), zap.String("request_id", requestID))
		// Continue without count
		totalRows = 0
	}

	// Determine response format
	format := GetAcceptFormat(r)

	// Build links config if requested
	var linksConfig *formats.LinksConfig
	if ParseLinks(r) {
		linksConfig = &formats.LinksConfig{
			Enabled:  true,
			BasePath: r.URL.Path,
			Query:    r.URL.Query(),
		}
	}

	// Format response
	if err := h.formatResponse(w, rows, format, page, limit, totalRows, paginationRequested, safetyLimit, linksConfig); err != nil {
		h.logger.Error("Failed to format response", zap.Error(err), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, "Failed to format response", http.StatusInternalServerError)
	}
}

// UpdateRequestFilter represents a filter condition in the update request body.
type UpdateRequestFilter struct {
	Column   string      `json:"column"`
	Operator string      `json:"op"`
	Value    interface{} `json:"value"`
}

// handleUpdate handles UPDATE operations.
// WHERE clause supports all filter operators: eq, ne, gt, gte, lt, lte, like, in
// Request body format:
//
//	{
//	  "where": [{"column": "age", "op": "gt", "value": 18}],
//	  "set": {"status": "adult"}
//	}
func (h *CRUDHandler) handleUpdate(w http.ResponseWriter, r *http.Request, tableName string) {
	requestID := auth.GetRequestIDFromContext(r.Context())

	// Check authorization
	role := auth.GetRoleFromContext(r.Context())
	allowed, err := h.authorizer.CheckPermission(role, tableName, auth.OperationUpdate)
	if err != nil {
		h.logger.Error("Failed to check permission", zap.Error(err), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, "Failed to check permission", http.StatusInternalServerError)
		return
	}
	if !allowed {
		h.sendErrorWithRequest(w, r, "Forbidden: insufficient permissions for UPDATE operation", http.StatusForbidden)
		return
	}

	// Parse request body using streaming decoder for better performance
	defer r.Body.Close()

	var req struct {
		Where []UpdateRequestFilter  `json:"where"`
		Set   map[string]interface{} `json:"set"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendErrorWithRequest(w, r, "Invalid JSON in request body", http.StatusBadRequest)
		return
	}

	// Validate WHERE clause is provided
	if len(req.Where) == 0 {
		h.sendErrorWithRequest(w, r, "WHERE clause is required for UPDATE operation", http.StatusBadRequest)
		return
	}

	// Validate SET clause is provided
	if len(req.Set) == 0 {
		h.sendErrorWithRequest(w, r, "SET clause is required for UPDATE operation", http.StatusBadRequest)
		return
	}

	// Valid operators
	validOperators := map[string]bool{
		"eq": true, "ne": true, "gt": true, "gte": true,
		"lt": true, "lte": true, "like": true, "in": true,
	}

	// Convert request filters to database.Filter and validate
	filters := make([]database.Filter, 0, len(req.Where))
	for _, f := range req.Where {
		// Validate column name
		if err := SanitizeColumnName(f.Column); err != nil {
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid WHERE column '%s': %s", f.Column, err.Error()), http.StatusBadRequest)
			return
		}

		// Validate operator
		if !validOperators[f.Operator] {
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid operator '%s': supported operators are eq, ne, gt, gte, lt, lte, like, in", f.Operator), http.StatusBadRequest)
			return
		}

		filters = append(filters, database.Filter{
			Column:   f.Column,
			Operator: f.Operator,
			Value:    f.Value,
		})
	}

	// Validate SET column names
	for col := range req.Set {
		if err := SanitizeColumnName(col); err != nil {
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid SET column '%s': %s", col, err.Error()), http.StatusBadRequest)
			return
		}
	}

	// Execute update with filters
	result, err := h.dbMgr.UpdateWithFilters(tableName, req.Set, filters)
	if err != nil {
		h.logger.Error("Failed to update data", zap.Error(err), zap.String("table", tableName), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, fmt.Sprintf("Failed to update data: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	h.sendSuccessWithRequest(w, r, result.RowsAffected, http.StatusOK)
}

// handleDelete handles DELETE operations.
// Supports dry_run=true parameter to preview affected rows without deleting.
// WHERE clause supports all filter operators: eq, ne, gt, gte, lt, lte, like, in
func (h *CRUDHandler) handleDelete(w http.ResponseWriter, r *http.Request, tableName string) {
	requestID := auth.GetRequestIDFromContext(r.Context())

	// Check authorization
	role := auth.GetRoleFromContext(r.Context())
	allowed, err := h.authorizer.CheckPermission(role, tableName, auth.OperationDelete)
	if err != nil {
		h.logger.Error("Failed to check permission", zap.Error(err), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, "Failed to check permission", http.StatusInternalServerError)
		return
	}
	if !allowed {
		h.sendErrorWithRequest(w, r, "Forbidden: insufficient permissions for DELETE operation", http.StatusForbidden)
		return
	}

	// Parse WHERE clause from query parameters (now returns []database.Filter)
	filters, err := ParseWhereClause(r)
	if err != nil {
		h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid where clause: %s", err.Error()), http.StatusBadRequest)
		return
	}

	if filters == nil || len(filters) == 0 {
		h.sendErrorWithRequest(w, r, "WHERE clause is required for DELETE operation (use ?where=column:operator:value)", http.StatusBadRequest)
		return
	}

	// Validate column names
	for _, f := range filters {
		if err := SanitizeColumnName(f.Column); err != nil {
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Invalid WHERE column '%s': %s", f.Column, err.Error()), http.StatusBadRequest)
			return
		}
	}

	// Check for dry_run parameter
	dryRun := ParseDryRun(r)

	if dryRun {
		// Dry run: just count affected rows without deleting
		count, err := h.dbMgr.CountWithFilters(tableName, filters)
		if err != nil {
			h.logger.Error("Failed to count rows for dry run", zap.Error(err), zap.String("table", tableName), zap.String("request_id", requestID))
			h.sendErrorWithRequest(w, r, fmt.Sprintf("Failed to count rows: %s", err.Error()), http.StatusInternalServerError)
			return
		}
		h.sendDryRunResultWithRequest(w, r, count)
		return
	}

	// Execute delete with filters
	result, err := h.dbMgr.DeleteWithFilters(tableName, filters)
	if err != nil {
		h.logger.Error("Failed to delete data", zap.Error(err), zap.String("table", tableName), zap.String("request_id", requestID))
		h.sendErrorWithRequest(w, r, fmt.Sprintf("Failed to delete data: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	h.sendSuccessWithRequest(w, r, result.RowsAffected, http.StatusOK)
}

// formatResponse formats the query result based on the requested format.
func (h *CRUDHandler) formatResponse(w http.ResponseWriter, rows *sql.Rows, format string, page, limit int, totalRows int64, paginationRequested bool, safetyLimit int, linksConfig *formats.LinksConfig) error {
	switch format {
	case "csv":
		return formats.WriteCSV(w, rows)
	case "json":
		return formats.WriteJSON(w, rows, page, limit, totalRows, paginationRequested, safetyLimit, linksConfig)
	case "parquet":
		return formats.WriteParquet(w, rows)
	case "arrow":
		return formats.WriteArrowIPC(w, rows)
	default:
		return formats.WriteJSON(w, rows, page, limit, totalRows, paginationRequested, safetyLimit, linksConfig)
	}
}

// sendErrorWithRequest sends an error response.
// The request ID is available in the X-Request-ID response header.
func (h *CRUDHandler) sendErrorWithRequest(w http.ResponseWriter, r *http.Request, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":   http.StatusText(statusCode),
		"message": message,
		"code":    statusCode,
	})
}

// sendError sends an error response (without request context).
// Deprecated: Use sendErrorWithRequest when request is available.
func (h *CRUDHandler) sendError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":   http.StatusText(statusCode),
		"message": message,
		"code":    statusCode,
	})
}

// sendSuccessWithRequest sends a success response.
// The request ID is available in the X-Request-ID response header.
func (h *CRUDHandler) sendSuccessWithRequest(w http.ResponseWriter, r *http.Request, rowsAffected int64, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"rows_affected": rowsAffected,
	})
}

// sendSuccess sends a success response (without request context).
// Deprecated: Use sendSuccessWithRequest when request is available.
func (h *CRUDHandler) sendSuccess(w http.ResponseWriter, rowsAffected int64, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"rows_affected": rowsAffected,
	})
}

// sendDryRunResultWithRequest sends a dry run result response.
// The request ID is available in the X-Request-ID response header.
func (h *CRUDHandler) sendDryRunResultWithRequest(w http.ResponseWriter, r *http.Request, affectedRows int64) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"dry_run":       true,
		"affected_rows": affectedRows,
	})
}

// sendDryRunResult sends a dry run result response (without request context).
// Deprecated: Use sendDryRunResultWithRequest when request is available.
func (h *CRUDHandler) sendDryRunResult(w http.ResponseWriter, affectedRows int64) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"dry_run":       true,
		"affected_rows": affectedRows,
	})
}

// ExtractTableFromPath extracts the table name from the request path.
func ExtractTableFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "duckdb" && parts[1] == "api" {
		return parts[2]
	}
	return ""
}
