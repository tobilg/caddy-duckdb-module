package database

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"go.uber.org/zap"
)

// InsertResult represents the result of an insert operation.
type InsertResult struct {
	RowsAffected int64
}

// UpdateResult represents the result of an update operation.
type UpdateResult struct {
	RowsAffected int64
}

// DeleteResult represents the result of a delete operation.
type DeleteResult struct {
	RowsAffected int64
}

const (
	maxRetries     = 3
	baseRetryDelay = 50 * time.Millisecond
)

// isTransactionConflict checks if an error is a DuckDB transaction conflict.
func isTransactionConflict(err error) bool {
	if err == nil {
		return false
	}
	// DuckDB transaction conflict errors contain "Transaction conflict" or "Conflict"
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "transaction conflict") ||
		strings.Contains(errStr, "conflict on table")
}

// retryOnConflict executes a function with exponential backoff retry on transaction conflicts.
func retryOnConflict(fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		if !isTransactionConflict(err) {
			// Not a conflict, return immediately
			return err
		}

		lastErr = err
		if attempt < maxRetries-1 {
			// Exponential backoff: 50ms, 100ms, 200ms
			delay := baseRetryDelay * time.Duration(math.Pow(2, float64(attempt)))
			time.Sleep(delay)
		}
	}
	return fmt.Errorf("transaction failed after %d retries: %w", maxRetries, lastErr)
}

// Insert inserts a single row into the specified table.
// Automatically retries on transaction conflicts with exponential backoff.
// Uses prepared statements with schema normalization for optimal performance.
// User API: clients can omit nullable columns - they will be set to NULL internally.
func (m *Manager) Insert(table string, data map[string]interface{}) (*InsertResult, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("no data provided for insert")
	}

	// Get table schema for normalization
	columns, err := m.getTableColumns(table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}

	var result *InsertResult
	err = retryOnConflict(func() error {
		// Get or create prepared statement for this table
		stmt, err := m.getOrPrepareInsert(table, columns)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement: %w", err)
		}

		// Normalize data to match all columns (NULL for omitted columns)
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			if val, ok := data[col]; ok {
				values[i] = val
			} else {
				values[i] = nil // NULL for omitted columns
			}
		}

		// Use transaction for atomicity
		tx, err := m.BeginTxMain()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback()

		// Use the prepared statement within the transaction
		txStmt := tx.Stmt(stmt)
		execResult, err := txStmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("failed to execute insert: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		rowsAffected, _ := execResult.RowsAffected()
		result = &InsertResult{RowsAffected: rowsAffected}
		return nil
	})

	return result, err
}

// getOrPrepareInsert gets or creates a prepared INSERT statement for a table.
func (m *Manager) getOrPrepareInsert(table string, columns []string) (*sql.Stmt, error) {
	stmtKey := fmt.Sprintf("%s:insert", table)

	// Check cache first
	if cached, ok := m.preparedStmts.Load(stmtKey); ok {
		return cached.(*sql.Stmt), nil
	}

	// Build INSERT query with all columns
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Prepare statement
	stmt, err := m.mainDB.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	// Store in cache
	m.preparedStmts.Store(stmtKey, stmt)

	m.logger.Debug("Prepared INSERT statement",
		zap.String("table", table),
		zap.Int("columns", len(columns)),
	)

	return stmt, nil
}

// Update updates rows in the specified table based on the where clause.
// Automatically retries on transaction conflicts with exponential backoff.
// Uses prepared statements for common UPDATE patterns (cached by column signature).
// Deprecated: Use UpdateWithFilters for full operator support.
func (m *Manager) Update(table string, set map[string]interface{}, where map[string]interface{}) (*UpdateResult, error) {
	if len(set) == 0 {
		return nil, fmt.Errorf("no data provided for update")
	}
	if len(where) == 0 {
		return nil, fmt.Errorf("no where clause provided for update (use DELETE with caution)")
	}

	var result *UpdateResult
	err := retryOnConflict(func() error {
		// Try to get or prepare an UPDATE statement for this column pattern
		stmt, setCols, whereCols, err := m.getOrPrepareUpdate(table, set, where)
		if err != nil {
			return fmt.Errorf("failed to prepare update statement: %w", err)
		}

		// Build values array in the order expected by the prepared statement
		values := make([]interface{}, 0, len(set)+len(where))
		for _, col := range setCols {
			values = append(values, set[col])
		}
		for _, col := range whereCols {
			values = append(values, where[col])
		}

		// Use transaction for atomicity
		tx, err := m.BeginTxMain()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback()

		// Use the prepared statement within the transaction
		txStmt := tx.Stmt(stmt)
		execResult, err := txStmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("failed to execute update: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		rowsAffected, _ := execResult.RowsAffected()
		result = &UpdateResult{RowsAffected: rowsAffected}
		return nil
	})

	return result, err
}

// UpdateWithFilters updates rows in the specified table based on filter conditions.
// Supports all filter operators (eq, ne, gt, gte, lt, lte, like, in).
// Automatically retries on transaction conflicts with exponential backoff.
func (m *Manager) UpdateWithFilters(table string, set map[string]interface{}, filters []Filter) (*UpdateResult, error) {
	if len(set) == 0 {
		return nil, fmt.Errorf("no data provided for update")
	}
	if len(filters) == 0 {
		return nil, fmt.Errorf("no filters provided for update (safety check)")
	}

	// Build SET clause with stable column order
	setCols := make([]string, 0, len(set))
	for col := range set {
		setCols = append(setCols, col)
	}
	// Sort for consistent query generation
	sortStrings := func(s []string) {
		for i := 0; i < len(s); i++ {
			for j := i + 1; j < len(s); j++ {
				if s[i] > s[j] {
					s[i], s[j] = s[j], s[i]
				}
			}
		}
	}
	sortStrings(setCols)

	// Build UPDATE query dynamically
	query := fmt.Sprintf("UPDATE %s SET ", table)
	values := make([]interface{}, 0)
	paramIndex := 1

	// Build SET clause
	setClauses := make([]string, len(setCols))
	for i, col := range setCols {
		setClauses[i] = fmt.Sprintf("%s = $%d", col, paramIndex)
		values = append(values, set[col])
		paramIndex++
	}
	query += strings.Join(setClauses, ", ")

	// Build WHERE clause from filters
	whereClauses := make([]string, 0, len(filters))
	for _, f := range filters {
		clause, val := f.ToSQL(paramIndex)
		whereClauses = append(whereClauses, clause)
		if val != nil {
			values = append(values, val)
			paramIndex++
		}
	}
	query += " WHERE " + strings.Join(whereClauses, " AND ")

	var result *UpdateResult
	err := retryOnConflict(func() error {
		// Use transaction for atomicity
		tx, err := m.BeginTxMain()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback()

		execResult, err := tx.Exec(query, values...)
		if err != nil {
			return fmt.Errorf("failed to execute update: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		rowsAffected, _ := execResult.RowsAffected()
		result = &UpdateResult{RowsAffected: rowsAffected}
		return nil
	})

	return result, err
}

// getOrPrepareUpdate gets or creates a prepared UPDATE statement for a specific column pattern.
// Returns the statement and the ordered column lists for SET and WHERE clauses.
func (m *Manager) getOrPrepareUpdate(table string, set map[string]interface{}, where map[string]interface{}) (*sql.Stmt, []string, []string, error) {
	// Create stable column lists (sorted to ensure consistent cache keys)
	setCols := make([]string, 0, len(set))
	for col := range set {
		setCols = append(setCols, col)
	}
	whereCols := make([]string, 0, len(where))
	for col := range where {
		whereCols = append(whereCols, col)
	}

	// Sort for cache key stability
	sortStrings := func(s []string) {
		for i := 0; i < len(s); i++ {
			for j := i + 1; j < len(s); j++ {
				if s[i] > s[j] {
					s[i], s[j] = s[j], s[i]
				}
			}
		}
	}
	sortStrings(setCols)
	sortStrings(whereCols)

	// Build cache key based on column pattern
	stmtKey := fmt.Sprintf("%s:update:set=%s:where=%s", table, strings.Join(setCols, ","), strings.Join(whereCols, ","))

	// Check cache first
	if cached, ok := m.preparedStmts.Load(stmtKey); ok {
		return cached.(*sql.Stmt), setCols, whereCols, nil
	}

	// Build UPDATE query
	setClauses := make([]string, len(setCols))
	for i, col := range setCols {
		setClauses[i] = fmt.Sprintf("%s = $%d", col, i+1)
	}

	whereClauses := make([]string, len(whereCols))
	for i, col := range whereCols {
		whereClauses[i] = fmt.Sprintf("%s = $%d", col, len(setCols)+i+1)
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		table,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	// Prepare statement
	stmt, err := m.mainDB.Prepare(query)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	// Store in cache
	m.preparedStmts.Store(stmtKey, stmt)

	m.logger.Debug("Prepared UPDATE statement",
		zap.String("table", table),
		zap.Int("set_columns", len(setCols)),
		zap.Int("where_columns", len(whereCols)),
	)

	return stmt, setCols, whereCols, nil
}

// Delete deletes rows from the specified table based on the where clause.
// Automatically retries on transaction conflicts with exponential backoff.
// Uses prepared statements for common DELETE patterns (cached by column signature).
// Deprecated: Use DeleteWithFilters for full operator support.
func (m *Manager) Delete(table string, where map[string]interface{}) (*DeleteResult, error) {
	if len(where) == 0 {
		return nil, fmt.Errorf("no where clause provided for delete (safety check)")
	}

	var result *DeleteResult
	err := retryOnConflict(func() error {
		// Try to get or prepare a DELETE statement for this column pattern
		stmt, whereCols, err := m.getOrPrepareDelete(table, where)
		if err != nil {
			return fmt.Errorf("failed to prepare delete statement: %w", err)
		}

		// Build values array in the order expected by the prepared statement
		values := make([]interface{}, len(whereCols))
		for i, col := range whereCols {
			values[i] = where[col]
		}

		// Use transaction for atomicity
		tx, err := m.BeginTxMain()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback()

		// Use the prepared statement within the transaction
		txStmt := tx.Stmt(stmt)
		execResult, err := txStmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("failed to execute delete: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		rowsAffected, _ := execResult.RowsAffected()
		result = &DeleteResult{RowsAffected: rowsAffected}
		return nil
	})

	return result, err
}

// DeleteWithFilters deletes rows from the specified table based on filter conditions.
// Supports all filter operators (eq, ne, gt, gte, lt, lte, like, in).
// Automatically retries on transaction conflicts with exponential backoff.
func (m *Manager) DeleteWithFilters(table string, filters []Filter) (*DeleteResult, error) {
	if len(filters) == 0 {
		return nil, fmt.Errorf("no filters provided for delete (safety check)")
	}

	// Build DELETE query dynamically based on filters
	query := fmt.Sprintf("DELETE FROM %s", table)
	values := make([]interface{}, 0)
	paramIndex := 1

	// Build WHERE clause from filters
	whereClauses := make([]string, 0, len(filters))
	for _, f := range filters {
		clause, val := f.ToSQL(paramIndex)
		whereClauses = append(whereClauses, clause)
		if val != nil {
			values = append(values, val)
			paramIndex++
		}
	}
	query += " WHERE " + strings.Join(whereClauses, " AND ")

	var result *DeleteResult
	err := retryOnConflict(func() error {
		// Use transaction for atomicity
		tx, err := m.BeginTxMain()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback()

		execResult, err := tx.Exec(query, values...)
		if err != nil {
			return fmt.Errorf("failed to execute delete: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		rowsAffected, _ := execResult.RowsAffected()
		result = &DeleteResult{RowsAffected: rowsAffected}
		return nil
	})

	return result, err
}

// CountWithFilters returns the count of rows matching the given filters.
// Useful for dry-run delete operations to preview affected rows.
func (m *Manager) CountWithFilters(table string, filters []Filter) (int64, error) {
	return m.Count(table, filters)
}

// getOrPrepareDelete gets or creates a prepared DELETE statement for a specific column pattern.
// Returns the statement and the ordered column list for WHERE clause.
func (m *Manager) getOrPrepareDelete(table string, where map[string]interface{}) (*sql.Stmt, []string, error) {
	// Create stable column list (sorted to ensure consistent cache keys)
	whereCols := make([]string, 0, len(where))
	for col := range where {
		whereCols = append(whereCols, col)
	}

	// Sort for cache key stability
	sortStrings := func(s []string) {
		for i := 0; i < len(s); i++ {
			for j := i + 1; j < len(s); j++ {
				if s[i] > s[j] {
					s[i], s[j] = s[j], s[i]
				}
			}
		}
	}
	sortStrings(whereCols)

	// Build cache key based on column pattern
	stmtKey := fmt.Sprintf("%s:delete:where=%s", table, strings.Join(whereCols, ","))

	// Check cache first
	if cached, ok := m.preparedStmts.Load(stmtKey); ok {
		return cached.(*sql.Stmt), whereCols, nil
	}

	// Build DELETE query
	whereClauses := make([]string, len(whereCols))
	for i, col := range whereCols {
		whereClauses[i] = fmt.Sprintf("%s = $%d", col, i+1)
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		table,
		strings.Join(whereClauses, " AND "),
	)

	// Prepare statement
	stmt, err := m.mainDB.Prepare(query)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	// Store in cache
	m.preparedStmts.Store(stmtKey, stmt)

	m.logger.Debug("Prepared DELETE statement",
		zap.String("table", table),
		zap.Int("where_columns", len(whereCols)),
	)

	return stmt, whereCols, nil
}

// Select executes a SELECT query with optional filters, sorting, and pagination.
// This is a read-only operation and does not use transactions for better performance.
func (m *Manager) Select(table string, filters []Filter, sorts []Sort, limit, offset int) (*sql.Rows, error) {
	query := fmt.Sprintf("SELECT * FROM %s", table)
	values := make([]interface{}, 0)
	paramIndex := 1

	// Add WHERE clause if filters exist
	if len(filters) > 0 {
		whereClauses := make([]string, 0, len(filters))
		for _, f := range filters {
			clause, val := f.ToSQL(paramIndex)
			whereClauses = append(whereClauses, clause)
			if val != nil {
				values = append(values, val)
				paramIndex++
			}
		}
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Add ORDER BY clause if sorts exist
	if len(sorts) > 0 {
		sortClauses := make([]string, 0, len(sorts))
		for _, s := range sorts {
			sortClauses = append(sortClauses, s.ToSQL())
		}
		query += " ORDER BY " + strings.Join(sortClauses, ", ")
	}

	// Add LIMIT and OFFSET
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}

	return m.QueryMain(query, values...)
}

// Count returns the total number of rows in a table matching the filters.
// This is a read-only operation and does not use transactions for better performance.
func (m *Manager) Count(table string, filters []Filter) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	values := make([]interface{}, 0)
	paramIndex := 1

	// Add WHERE clause if filters exist
	if len(filters) > 0 {
		whereClauses := make([]string, 0, len(filters))
		for _, f := range filters {
			clause, val := f.ToSQL(paramIndex)
			whereClauses = append(whereClauses, clause)
			if val != nil {
				values = append(values, val)
				paramIndex++
			}
		}
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	var count int64
	err := m.QueryRowScanMain(query, []interface{}{&count}, values...)
	return count, err
}

// Filter represents a query filter.
type Filter struct {
	Column   string
	Operator string
	Value    interface{}
}

// ToSQL converts the filter to SQL.
func (f Filter) ToSQL(paramIndex int) (string, interface{}) {
	switch f.Operator {
	case "eq":
		return fmt.Sprintf("%s = $%d", f.Column, paramIndex), f.Value
	case "ne":
		return fmt.Sprintf("%s != $%d", f.Column, paramIndex), f.Value
	case "gt":
		return fmt.Sprintf("%s > $%d", f.Column, paramIndex), f.Value
	case "gte":
		return fmt.Sprintf("%s >= $%d", f.Column, paramIndex), f.Value
	case "lt":
		return fmt.Sprintf("%s < $%d", f.Column, paramIndex), f.Value
	case "lte":
		return fmt.Sprintf("%s <= $%d", f.Column, paramIndex), f.Value
	case "like":
		return fmt.Sprintf("%s LIKE $%d", f.Column, paramIndex), f.Value
	case "in":
		// For IN operator, value should be a slice
		return fmt.Sprintf("%s IN $%d", f.Column, paramIndex), f.Value
	default:
		return fmt.Sprintf("%s = $%d", f.Column, paramIndex), f.Value
	}
}

// Sort represents a sort order.
type Sort struct {
	Column    string
	Direction string
}

// ToSQL converts the sort to SQL.
func (s Sort) ToSQL() string {
	dir := "ASC"
	if strings.ToLower(s.Direction) == "desc" {
		dir = "DESC"
	}
	return fmt.Sprintf("%s %s", s.Column, dir)
}

// TableExists checks if a table exists in the main database.
func (m *Manager) TableExists(table string) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_name = $1
	`
	var count int
	err := m.QueryRowScanMain(query, []interface{}{&count}, table)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
