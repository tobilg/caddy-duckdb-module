package duckdb

// ErrorResponse represents a standard error response.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
	Code    int    `json:"code"`
}

// SuccessResponse represents a standard success response.
type SuccessResponse struct {
	Success      bool   `json:"success"`
	RowsAffected int64  `json:"rows_affected,omitempty"`
	Message      string `json:"message,omitempty"`
}

// PaginationInfo represents pagination metadata.
type PaginationInfo struct {
	Page       int   `json:"page"`
	Limit      int   `json:"limit"`
	TotalRows  int64 `json:"total_rows"`
	TotalPages int   `json:"total_pages"`
}

// QueryRequest represents a raw SQL query request.
type QueryRequest struct {
	SQL    string        `json:"sql"`
	Params []interface{} `json:"params,omitempty"`
}

// QueryResponse represents a query result.
type QueryResponse struct {
	Columns         []string        `json:"columns,omitempty"`
	Data            [][]interface{} `json:"data,omitempty"`
	RowsAffected    int64           `json:"rows_affected,omitempty"`
	ExecutionTimeMs int64           `json:"execution_time_ms,omitempty"`
	Pagination      *PaginationInfo `json:"pagination,omitempty"`
}

// CRUDRequest represents a CRUD operation request.
type CRUDRequest struct {
	Where map[string]interface{} `json:"where,omitempty"`
	Set   map[string]interface{} `json:"set,omitempty"`
	Data  map[string]interface{} `json:"data,omitempty"` // For INSERT
}

// FilterOperator represents the supported filter operators.
type FilterOperator string

const (
	OpEqual        FilterOperator = "eq"
	OpNotEqual     FilterOperator = "ne"
	OpGreaterThan  FilterOperator = "gt"
	OpGreaterEqual FilterOperator = "gte"
	OpLessThan     FilterOperator = "lt"
	OpLessEqual    FilterOperator = "lte"
	OpLike         FilterOperator = "like"
	OpIn           FilterOperator = "in"
)

// Filter represents a single filter condition.
type Filter struct {
	Column   string
	Operator FilterOperator
	Value    interface{}
}

// SortDirection represents sort order.
type SortDirection string

const (
	SortAsc  SortDirection = "asc"
	SortDesc SortDirection = "desc"
)

// SortColumn represents a column to sort by.
type SortColumn struct {
	Column    string
	Direction SortDirection
}
