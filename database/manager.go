package database

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"
)

// Config holds the configuration for the database manager.
type Config struct {
	MainDBPath        string
	AuthDBPath        string
	Threads           int
	AccessMode        string
	MemoryLimit       string
	EnableObjectCache bool
	TempDirectory     string
	QueryTimeout      time.Duration
	Logger            *zap.Logger
}

// Manager handles both the main database and the internal auth database.
type Manager struct {
	mainDB        *sql.DB
	authDB        *sql.DB
	authDBPath    string   // stored for error messages
	tableSchemas  sync.Map // map[string][]string - cache of table->columns
	preparedStmts sync.Map // map[string]*sql.Stmt - cache of query->statement
	queryTimeout  time.Duration
	logger        *zap.Logger
}

// NewManager creates a new database manager.
func NewManager(cfg Config) (*Manager, error) {
	mgr := &Manager{
		queryTimeout: cfg.QueryTimeout,
		logger:       cfg.Logger,
		authDBPath:   cfg.AuthDBPath,
	}

	// Initialize main database
	mainDSN := cfg.MainDBPath
	if mainDSN == "" {
		// In-memory database
		mainDSN = ":memory:"
	}
	// Add configuration parameters
	mainDSN = fmt.Sprintf("%s?threads=%d&access_mode=%s", mainDSN, cfg.Threads, cfg.AccessMode)

	// Add optional memory limit
	if cfg.MemoryLimit != "" {
		mainDSN = fmt.Sprintf("%s&memory_limit=%s", mainDSN, cfg.MemoryLimit)
	}

	// Add optional object cache
	if cfg.EnableObjectCache {
		mainDSN = fmt.Sprintf("%s&enable_object_cache=true", mainDSN)
	}

	// Add optional temp directory
	if cfg.TempDirectory != "" {
		mainDSN = fmt.Sprintf("%s&temp_directory=%s", mainDSN, cfg.TempDirectory)
	}

	var err error
	mgr.mainDB, err = sql.Open("duckdb", mainDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open main database: %w", err)
	}

	// Configure connection pool for concurrent operations
	// DuckDB supports concurrent reads/writes within a single process
	// Set max connections to threads * 2 to handle concurrent workloads
	mgr.mainDB.SetMaxOpenConns(cfg.Threads * 2)
	mgr.mainDB.SetMaxIdleConns(cfg.Threads)
	mgr.mainDB.SetConnMaxLifetime(time.Hour)

	// Test main database connection
	if err := mgr.mainDB.Ping(); err != nil {
		mgr.mainDB.Close()
		return nil, fmt.Errorf("failed to ping main database: %w", err)
	}

	mgr.logger.Info("Main database connected",
		zap.String("dsn", mainDSN),
		zap.Bool("in_memory", cfg.MainDBPath == ""),
		zap.Int("max_open_conns", cfg.Threads*2),
		zap.Int("max_idle_conns", cfg.Threads),
	)

	// Initialize auth database (always file-based)
	authDSN := fmt.Sprintf("%s?threads=%d", cfg.AuthDBPath, cfg.Threads)
	mgr.authDB, err = sql.Open("duckdb", authDSN)
	if err != nil {
		mgr.mainDB.Close()
		return nil, fmt.Errorf("failed to open auth database: %w", err)
	}

	// Configure connection pool for auth database
	mgr.authDB.SetMaxOpenConns(cfg.Threads * 2)
	mgr.authDB.SetMaxIdleConns(cfg.Threads)
	mgr.authDB.SetConnMaxLifetime(time.Hour)

	// Test auth database connection
	if err := mgr.authDB.Ping(); err != nil {
		mgr.mainDB.Close()
		mgr.authDB.Close()
		return nil, fmt.Errorf("failed to ping auth database: %w", err)
	}

	mgr.logger.Info("Auth database connected",
		zap.String("path", cfg.AuthDBPath),
		zap.Int("max_open_conns", cfg.Threads*2),
		zap.Int("max_idle_conns", cfg.Threads),
	)

	// Initialize auth database schema
	if err := mgr.initAuthSchema(); err != nil {
		mgr.mainDB.Close()
		mgr.authDB.Close()
		return nil, fmt.Errorf("failed to initialize auth schema: %w", err)
	}

	// Pre-warm connections to eliminate cold-start latency
	mgr.warmConnections()

	return mgr, nil
}

// initAuthSchema validates that the auth database has the required schema.
// The auth database must be pre-initialized using the auth-db CLI tool.
func (m *Manager) initAuthSchema() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	defer cancel()

	// Check that required tables exist
	requiredTables := []string{"roles", "api_keys", "permissions"}
	for _, table := range requiredTables {
		var exists bool
		query := `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_name = $1
			)
		`
		err := m.authDB.QueryRowContext(ctx, query, table).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check for table '%s': %w", table, err)
		}
		if !exists {
			return fmt.Errorf("auth database is missing required table '%s'. "+
				"Please initialize the auth database using: auth-db init -d %s",
				table, m.authDBPath)
		}
	}

	// Validate that at least one role exists
	var roleCount int
	err := m.authDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM roles").Scan(&roleCount)
	if err != nil {
		return fmt.Errorf("failed to count roles: %w", err)
	}
	if roleCount == 0 {
		return fmt.Errorf("auth database has no roles defined. " +
			"Please add at least one role using: auth-db role add -d <path> -n <role_name>")
	}

	m.logger.Info("Auth database schema validated",
		zap.Int("roles", roleCount),
	)
	return nil
}

// NewManagerForTesting creates a new database manager and initializes the auth schema.
// This is ONLY for use in tests - production should use the auth-db CLI tool.
func NewManagerForTesting(cfg Config) (*Manager, error) {
	mgr := &Manager{
		queryTimeout: cfg.QueryTimeout,
		logger:       cfg.Logger,
		authDBPath:   cfg.AuthDBPath,
	}

	if mgr.logger == nil {
		mgr.logger = zap.NewNop()
	}

	// Initialize main database
	mainDSN := cfg.MainDBPath
	if mainDSN == "" {
		mainDSN = ":memory:"
	}
	mainDSN = fmt.Sprintf("%s?threads=%d&access_mode=%s", mainDSN, cfg.Threads, cfg.AccessMode)

	var err error
	mgr.mainDB, err = sql.Open("duckdb", mainDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open main database: %w", err)
	}

	// Initialize auth database
	authDSN := fmt.Sprintf("%s?threads=%d", cfg.AuthDBPath, cfg.Threads)
	mgr.authDB, err = sql.Open("duckdb", authDSN)
	if err != nil {
		mgr.mainDB.Close()
		return nil, fmt.Errorf("failed to open auth database: %w", err)
	}

	// Initialize auth schema for testing (instead of validating)
	if err := mgr.InitAuthSchemaForTesting(); err != nil {
		mgr.mainDB.Close()
		mgr.authDB.Close()
		return nil, fmt.Errorf("failed to initialize auth schema: %w", err)
	}

	return mgr, nil
}

// InitAuthSchemaForTesting creates the auth schema and default data.
// This is ONLY for use in tests - production should use the auth-db CLI tool.
func (m *Manager) InitAuthSchemaForTesting() error {
	schema := `
		-- Roles table
		CREATE TABLE IF NOT EXISTS roles (
			role_name VARCHAR PRIMARY KEY,
			description VARCHAR
		);

		-- API Keys table
		CREATE TABLE IF NOT EXISTS api_keys (
			key VARCHAR PRIMARY KEY,
			role_name VARCHAR NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP,
			is_active BOOLEAN DEFAULT true,
			FOREIGN KEY (role_name) REFERENCES roles(role_name)
		);

		-- Permissions table
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

		-- Create sequence for permissions ID
		CREATE SEQUENCE IF NOT EXISTS permissions_id_seq START 1;

		-- Default roles
		INSERT INTO roles (role_name, description)
		VALUES ('admin', 'Full access to all tables and raw SQL queries')
		ON CONFLICT DO NOTHING;

		INSERT INTO roles (role_name, description)
		VALUES ('editor', 'CRUD access to all tables, no raw SQL')
		ON CONFLICT DO NOTHING;

		INSERT INTO roles (role_name, description)
		VALUES ('reader', 'Read-only access to all tables')
		ON CONFLICT DO NOTHING;

		-- Default permissions
		INSERT INTO permissions (id, role_name, table_name, can_create, can_read, can_update, can_delete, can_query)
		VALUES (nextval('permissions_id_seq'), 'admin', '*', true, true, true, true, true)
		ON CONFLICT DO NOTHING;

		INSERT INTO permissions (id, role_name, table_name, can_create, can_read, can_update, can_delete, can_query)
		VALUES (nextval('permissions_id_seq'), 'editor', '*', true, true, true, true, false)
		ON CONFLICT DO NOTHING;

		INSERT INTO permissions (id, role_name, table_name, can_create, can_read, can_update, can_delete, can_query)
		VALUES (nextval('permissions_id_seq'), 'reader', '*', false, true, false, false, false)
		ON CONFLICT DO NOTHING;
	`

	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	defer cancel()

	_, err := m.authDB.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to execute test schema: %w", err)
	}

	return nil
}

// MainDB returns the main database connection.
func (m *Manager) MainDB() *sql.DB {
	return m.mainDB
}

// AuthDB returns the auth database connection.
func (m *Manager) AuthDB() *sql.DB {
	return m.authDB
}

// QueryTimeout returns the configured query timeout.
func (m *Manager) QueryTimeout() time.Duration {
	return m.queryTimeout
}

// Close closes both database connections.
func (m *Manager) Close() error {
	var err1, err2 error
	if m.mainDB != nil {
		err1 = m.mainDB.Close()
	}
	if m.authDB != nil {
		err2 = m.authDB.Close()
	}

	if err1 != nil {
		return err1
	}
	return err2
}

// ExecMain executes a query on the main database with timeout.
func (m *Manager) ExecMain(query string, args ...interface{}) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	defer cancel()
	return m.mainDB.ExecContext(ctx, query, args...)
}

// QueryMain executes a query on the main database with timeout.
// Note: The caller is responsible for closing the returned rows.
// The context will automatically be cleaned up when the timeout expires.
func (m *Manager) QueryMain(query string, args ...interface{}) (*sql.Rows, error) {
	// We intentionally don't defer cancel() here because the context needs to
	// stay alive while the caller iterates over the rows. The context will be
	// cleaned up automatically when the timeout expires or when rows.Close()
	// is called. Using a longer timeout ensures rows can be fully read.
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	rows, err := m.mainDB.QueryContext(ctx, query, args...)
	if err != nil {
		cancel()
		return nil, err
	}
	// Note: cancel will be called when context times out; rows.Close() should
	// be called by the caller to release resources promptly
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return rows, nil
}

// QueryRowMain executes a query that returns a single row on the main database.
func (m *Manager) QueryRowMain(query string, args ...interface{}) *sql.Row {
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	defer cancel()
	return m.mainDB.QueryRowContext(ctx, query, args...)
}

// ExecAuth executes a query on the auth database with timeout.
func (m *Manager) ExecAuth(query string, args ...interface{}) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	defer cancel()
	return m.authDB.ExecContext(ctx, query, args...)
}

// QueryAuth executes a query on the auth database with timeout.
// Note: The caller is responsible for closing the returned rows.
// The context will automatically be cleaned up when the timeout expires.
func (m *Manager) QueryAuth(query string, args ...interface{}) (*sql.Rows, error) {
	// We intentionally don't defer cancel() here because the context needs to
	// stay alive while the caller iterates over the rows. The context will be
	// cleaned up automatically when the timeout expires or when rows.Close()
	// is called. Using a longer timeout ensures rows can be fully read.
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	rows, err := m.authDB.QueryContext(ctx, query, args...)
	if err != nil {
		cancel()
		return nil, err
	}
	// Note: cancel will be called when context times out; rows.Close() should
	// be called by the caller to release resources promptly
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return rows, nil
}

// QueryRowAuth executes a query that returns a single row on the auth database.
func (m *Manager) QueryRowAuth(query string, args ...interface{}) *sql.Row {
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	defer cancel()
	return m.authDB.QueryRowContext(ctx, query, args...)
}

// BeginTxMain begins a transaction on the main database.
// Note: The caller is responsible for committing or rolling back the transaction.
// Uses background context since transaction operations have individual timeouts.
func (m *Manager) BeginTxMain() (*sql.Tx, error) {
	// Use background context instead of timeout context to avoid resource leaks.
	// Individual operations within the transaction (Exec, Query) will use
	// their own timeouts via the Manager's Exec/Query methods.
	return m.mainDB.BeginTx(context.Background(), nil)
}

// QueryRowScanMain executes a query that returns a single row and scans it immediately.
// This is the safe version of QueryRowMain that avoids context cancellation race conditions.
// Use this when you need to scan a single row into variables.
func (m *Manager) QueryRowScanMain(query string, dest []interface{}, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	defer cancel()
	return m.mainDB.QueryRowContext(ctx, query, args...).Scan(dest...)
}

// QueryRowScanAuth executes a query that returns a single row and scans it immediately.
// This is the safe version of QueryRowAuth that avoids context cancellation race conditions.
func (m *Manager) QueryRowScanAuth(query string, dest []interface{}, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.queryTimeout)
	defer cancel()
	return m.authDB.QueryRowContext(ctx, query, args...).Scan(dest...)
}

// warmConnections pre-warms the connection pool to eliminate cold-start latency.
// This creates and validates connections up to the configured pool size.
func (m *Manager) warmConnections() {
	stats := m.mainDB.Stats()
	maxConns := stats.MaxOpenConnections

	m.logger.Info("Pre-warming database connections",
		zap.Int("target_connections", maxConns),
	)

	// Create connections concurrently
	for i := 0; i < maxConns; i++ {
		go func(idx int) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := m.mainDB.Conn(ctx)
			if err != nil {
				m.logger.Warn("Failed to create warm connection",
					zap.Int("connection_index", idx),
					zap.Error(err),
				)
				return
			}
			defer conn.Close()

			// Ping to ensure connection is valid
			if err := conn.PingContext(ctx); err != nil {
				m.logger.Warn("Failed to ping warm connection",
					zap.Int("connection_index", idx),
					zap.Error(err),
				)
			}
		}(i)
	}

	// Give connections time to warm up (non-blocking)
	time.Sleep(100 * time.Millisecond)

	m.logger.Info("Connection pool warmed")
}

// getTableColumns retrieves and caches the column names for a table.
// This enables prepared statement pooling by normalizing INSERT statements
// to always use the same column order, even when users omit nullable columns.
func (m *Manager) getTableColumns(table string) ([]string, error) {
	// Check cache first
	if cached, ok := m.tableSchemas.Load(table); ok {
		return cached.([]string), nil
	}

	// Cache miss - query information_schema
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := m.QueryMain(query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}
		columns = append(columns, colName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table '%s' has no columns or does not exist", table)
	}

	// Store in cache
	m.tableSchemas.Store(table, columns)

	m.logger.Debug("Cached table schema",
		zap.String("table", table),
		zap.Int("columns", len(columns)),
	)

	return columns, nil
}

// InvalidateTableSchema removes a table's schema from the cache.
// Call this when a table's structure changes (ALTER TABLE).
func (m *Manager) InvalidateTableSchema(table string) {
	m.tableSchemas.Delete(table)

	// Also invalidate prepared statements for this table
	m.preparedStmts.Range(func(key, value interface{}) bool {
		stmtKey := key.(string)
		// Check if this statement is for the invalidated table
		// Statement keys contain the table name
		if len(stmtKey) >= len(table) && stmtKey[:len(table)] == table {
			stmt := value.(*sql.Stmt)
			stmt.Close()
			m.preparedStmts.Delete(key)
		}
		return true
	})

	m.logger.Debug("Invalidated table schema cache",
		zap.String("table", table),
	)
}
