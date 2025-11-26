package auth

import "time"

// APIKey represents an API key in the system.
type APIKey struct {
	Key       string
	RoleName  string
	CreatedAt time.Time
	ExpiresAt *time.Time
	IsActive  bool
}

// Role represents a role in the system.
type Role struct {
	RoleName    string
	Description string
}

// Permission represents a permission for a role on a table.
type Permission struct {
	ID        int
	RoleName  string
	TableName string
	CanCreate bool
	CanRead   bool
	CanUpdate bool
	CanDelete bool
	CanQuery  bool
}

// Operation represents a database operation type.
type Operation string

const (
	OperationCreate Operation = "create"
	OperationRead   Operation = "read"
	OperationUpdate Operation = "update"
	OperationDelete Operation = "delete"
	OperationQuery  Operation = "query"
)
