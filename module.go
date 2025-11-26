package duckdb

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/google/uuid"
	"github.com/tobilg/caddyserver-duckdb-module/auth"
	"github.com/tobilg/caddyserver-duckdb-module/database"
	"github.com/tobilg/caddyserver-duckdb-module/handlers"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(DuckDB{})
	httpcaddyfile.RegisterHandlerDirective("duckdb", parseCaddyfile)
}

// DuckDB is a Caddy module that provides a REST API for DuckDB operations.
type DuckDB struct {
	// DatabasePath is the path to the main DuckDB database file.
	// If empty, an in-memory database will be used.
	DatabasePath string `json:"database_path,omitempty"`

	// AuthDatabasePath is the path to the internal authentication database.
	// This is required and must be file-based for persistence.
	AuthDatabasePath string `json:"auth_database_path,omitempty"`

	// QueryTimeout is the maximum duration for query execution.
	// Default is 10 seconds.
	QueryTimeout caddy.Duration `json:"query_timeout,omitempty"`

	// MaxRowsPerPage is the default number of rows per page when pagination is used.
	// Default is 100.
	MaxRowsPerPage int `json:"max_rows_per_page,omitempty"`

	// AbsoluteMaxRows is the safety limit - the maximum number of rows that can be returned
	// in a single request, even when pagination is not requested.
	// Set to 0 to disable the limit (not recommended for production).
	// Default is 10000.
	AbsoluteMaxRows int `json:"absolute_max_rows,omitempty"`

	// Threads is the number of threads DuckDB should use.
	// Default is 4.
	Threads int `json:"threads,omitempty"`

	// AccessMode determines the access mode for the main database.
	// Valid values are "read_only" or "read_write" (default).
	AccessMode string `json:"access_mode,omitempty"`

	// MemoryLimit is the maximum memory DuckDB can use (e.g., "4GB", "512MB").
	// If empty, DuckDB defaults to 80% of available RAM.
	MemoryLimit string `json:"memory_limit,omitempty"`

	// EnableObjectCache enables DuckDB's object cache for faster repeated queries.
	// Default is false.
	EnableObjectCache bool `json:"enable_object_cache,omitempty"`

	// TempDirectory is the directory for DuckDB temporary files when spilling to disk.
	// If empty, uses system default.
	TempDirectory string `json:"temp_directory,omitempty"`

	logger         *zap.Logger
	dbMgr          *database.Manager
	authorizer     *auth.Authorizer
	authMw         *auth.Middleware
	crudHandler    *handlers.CRUDHandler
	queryHandler   *handlers.QueryHandler
	openAPIHandler *handlers.OpenAPIHandler
	routePrefix    string // set from DUCKDB_ROUTE_PREFIX env var, defaults to /duckdb
}

// CaddyModule returns the Caddy module information.
func (DuckDB) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.duckdb",
		New: func() caddy.Module { return new(DuckDB) },
	}
}

// Provision sets up the DuckDB module.
func (d *DuckDB) Provision(ctx caddy.Context) error {
	d.logger = ctx.Logger(d)

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
		d.QueryTimeout = caddy.Duration(10_000_000_000) // 10 seconds in nanoseconds
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

	// Initialize database manager
	var err error
	d.dbMgr, err = database.NewManager(database.Config{
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

	d.logger.Info("DuckDB module provisioned",
		zap.String("route_prefix", d.routePrefix),
		zap.String("main_db", d.DatabasePath),
		zap.String("auth_db", d.AuthDatabasePath),
		zap.Duration("query_timeout", time.Duration(d.QueryTimeout)),
		zap.Int("max_rows_per_page", d.MaxRowsPerPage),
		zap.Int("absolute_max_rows", d.AbsoluteMaxRows),
		zap.Int("threads", d.Threads),
		zap.String("access_mode", d.AccessMode),
		zap.String("memory_limit", d.MemoryLimit),
		zap.Bool("enable_object_cache", d.EnableObjectCache),
		zap.String("temp_directory", d.TempDirectory),
	)

	return nil
}

// Validate ensures the module configuration is valid.
func (d *DuckDB) Validate() error {
	if d.AccessMode != "read_only" && d.AccessMode != "read_write" {
		return fmt.Errorf("invalid access_mode: %s (must be 'read_only' or 'read_write')", d.AccessMode)
	}
	if d.MaxRowsPerPage <= 0 {
		return fmt.Errorf("max_rows_per_page must be greater than 0")
	}
	if d.AbsoluteMaxRows < 0 {
		return fmt.Errorf("absolute_max_rows must be >= 0 (0 disables the limit)")
	}
	if d.Threads <= 0 {
		return fmt.Errorf("threads must be greater than 0")
	}
	return nil
}

// ServeHTTP implements the caddyhttp.MiddlewareHandler interface.
func (d *DuckDB) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	// Check if this is a DuckDB endpoint
	if !strings.HasPrefix(r.URL.Path, d.routePrefix) {
		return next.ServeHTTP(w, r)
	}

	// Extract or generate request ID for tracing
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = uuid.New().String()
	}
	// Add request ID to context and response header
	ctx := auth.SetRequestID(r.Context(), requestID)
	r = r.WithContext(ctx)
	w.Header().Set("X-Request-ID", requestID)

	// Health check endpoint (no authentication required)
	if r.URL.Path == d.routePrefix+"/health" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
		return nil
	}

	// OpenAPI specification endpoint (no authentication required)
	if r.URL.Path == d.routePrefix+"/openapi.json" {
		d.openAPIHandler.ServeHTTP(w, r)
		return nil
	}

	// Authenticate all other requests
	authenticated := false
	apiKey := r.Header.Get("X-API-Key")
	if apiKey != "" {
		key, err := d.authorizer.AuthenticateAPIKey(apiKey)
		if err == nil && key != nil {
			// Add to context
			r = r.WithContext(auth.SetContextValues(r.Context(), key, key.RoleName))
			authenticated = true
		}
	}

	if !authenticated {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"Unauthorized","message":"Missing or invalid X-API-Key header","code":401}`))
		return nil
	}

	// Route based on path
	if strings.HasPrefix(r.URL.Path, d.routePrefix+"/query") {
		// Raw SQL query endpoint
		d.queryHandler.ServeHTTP(w, r)
		return nil
	} else if strings.HasPrefix(r.URL.Path, d.routePrefix+"/api/") {
		// CRUD operations endpoint
		d.crudHandler.ServeHTTP(w, r)
		return nil
	}

	// Unknown endpoint
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(`{"error":"Not Found","message":"Unknown DuckDB endpoint","code":404}`))
	return nil
}

// Cleanup performs cleanup when the module is unloaded.
func (d *DuckDB) Cleanup() error {
	if d.dbMgr != nil {
		return d.dbMgr.Close()
	}
	return nil
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
func (d *DuckDB) UnmarshalCaddyfile(dispenser *caddyfile.Dispenser) error {
	for dispenser.Next() {
		for dispenser.NextBlock(0) {
			switch dispenser.Val() {
			case "database_path":
				if !dispenser.Args(&d.DatabasePath) {
					return dispenser.ArgErr()
				}
			case "auth_database_path":
				if !dispenser.Args(&d.AuthDatabasePath) {
					return dispenser.ArgErr()
				}
			case "query_timeout":
				var timeout string
				if !dispenser.Args(&timeout) {
					return dispenser.ArgErr()
				}
				duration, err := caddy.ParseDuration(timeout)
				if err != nil {
					return dispenser.Errf("invalid query_timeout: %v", err)
				}
				d.QueryTimeout = caddy.Duration(duration)
			case "max_rows_per_page":
				var maxRowsStr string
				if !dispenser.Args(&maxRowsStr) {
					return dispenser.ArgErr()
				}
				maxRows, err := strconv.Atoi(maxRowsStr)
				if err != nil {
					return dispenser.Errf("invalid max_rows_per_page: %v", err)
				}
				d.MaxRowsPerPage = maxRows
			case "absolute_max_rows":
				var absMaxRowsStr string
				if !dispenser.Args(&absMaxRowsStr) {
					return dispenser.ArgErr()
				}
				absMaxRows, err := strconv.Atoi(absMaxRowsStr)
				if err != nil {
					return dispenser.Errf("invalid absolute_max_rows: %v", err)
				}
				d.AbsoluteMaxRows = absMaxRows
			case "threads":
				var threadsStr string
				if !dispenser.Args(&threadsStr) {
					return dispenser.ArgErr()
				}
				threads, err := strconv.Atoi(threadsStr)
				if err != nil {
					return dispenser.Errf("invalid threads: %v", err)
				}
				d.Threads = threads
			case "access_mode":
				if !dispenser.Args(&d.AccessMode) {
					return dispenser.ArgErr()
				}
			case "memory_limit":
				if !dispenser.Args(&d.MemoryLimit) {
					return dispenser.ArgErr()
				}
			case "enable_object_cache":
				var enableStr string
				if !dispenser.Args(&enableStr) {
					return dispenser.ArgErr()
				}
				enableStr = strings.ToLower(enableStr)
				d.EnableObjectCache = enableStr == "true" || enableStr == "yes" || enableStr == "1"
			case "temp_directory":
				if !dispenser.Args(&d.TempDirectory) {
					return dispenser.ArgErr()
				}
			default:
				return dispenser.Errf("unknown subdirective: %s", dispenser.Val())
			}
		}
	}
	return nil
}

// parseCaddyfile unmarshals tokens from h into a new Middleware.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var d DuckDB
	err := d.UnmarshalCaddyfile(h.Dispenser)
	return &d, err
}

// Interface guards
var (
	_ caddy.Module                = (*DuckDB)(nil)
	_ caddy.Provisioner           = (*DuckDB)(nil)
	_ caddy.Validator             = (*DuckDB)(nil)
	_ caddy.CleanerUpper          = (*DuckDB)(nil)
	_ caddyhttp.MiddlewareHandler = (*DuckDB)(nil)
	_ caddyfile.Unmarshaler       = (*DuckDB)(nil)
)
