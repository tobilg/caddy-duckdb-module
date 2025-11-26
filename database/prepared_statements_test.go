package database

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestPreparedStatementsWithNullableColumns verifies that the prepared statement
// pooling works correctly when clients omit nullable columns.
func TestPreparedStatementsWithNullableColumns(t *testing.T) {
	logger := zap.NewNop()
	cfg := Config{
		MainDBPath:   ":memory:",
		AuthDBPath:   ":memory:",
		Threads:      2,
		AccessMode:   "read_write",
		QueryTimeout: 5 * time.Second,
		Logger:       logger,
	}

	mgr, err := NewManagerForTesting(cfg)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Create a test table with nullable columns
	createTableSQL := `
		CREATE TABLE test_nullable (
			id INTEGER PRIMARY KEY,
			name VARCHAR NOT NULL,
			email VARCHAR,
			age INTEGER,
			created_at TIMESTAMP
		)
	`
	_, err = mgr.ExecMain(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test 1: Insert with all columns
	data1 := map[string]interface{}{
		"id":         1,
		"name":       "Alice",
		"email":      "alice@example.com",
		"age":        30,
		"created_at": time.Now(),
	}
	result, err := mgr.Insert("test_nullable", data1)
	if err != nil {
		t.Fatalf("Failed to insert with all columns: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	// Test 2: Insert omitting nullable columns (email, age, created_at)
	// This should reuse the same prepared statement
	data2 := map[string]interface{}{
		"id":   2,
		"name": "Bob",
	}
	result, err = mgr.Insert("test_nullable", data2)
	if err != nil {
		t.Fatalf("Failed to insert with omitted nullable columns: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	// Test 3: Insert with partial columns
	data3 := map[string]interface{}{
		"id":    3,
		"name":  "Charlie",
		"email": "charlie@example.com",
	}
	result, err = mgr.Insert("test_nullable", data3)
	if err != nil {
		t.Fatalf("Failed to insert with partial columns: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify data was inserted correctly with NULL values for omitted columns
	rows, err := mgr.QueryMain("SELECT id, name, email, age FROM test_nullable ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	expectedRows := []struct {
		id    int
		name  string
		email *string
		age   *int
	}{
		{1, "Alice", strPtr("alice@example.com"), intPtr(30)},
		{2, "Bob", nil, nil},
		{3, "Charlie", strPtr("charlie@example.com"), nil},
	}

	rowNum := 0
	for rows.Next() {
		var id int
		var name string
		var email *string
		var age *int

		if err := rows.Scan(&id, &name, &email, &age); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if rowNum >= len(expectedRows) {
			t.Fatalf("Got more rows than expected")
		}

		expected := expectedRows[rowNum]
		if id != expected.id {
			t.Errorf("Row %d: expected id=%d, got %d", rowNum, expected.id, id)
		}
		if name != expected.name {
			t.Errorf("Row %d: expected name=%s, got %s", rowNum, expected.name, name)
		}
		if !compareNullableString(email, expected.email) {
			t.Errorf("Row %d: email mismatch", rowNum)
		}
		if !compareNullableInt(age, expected.age) {
			t.Errorf("Row %d: age mismatch", rowNum)
		}

		rowNum++
	}

	if rowNum != len(expectedRows) {
		t.Errorf("Expected %d rows, got %d", len(expectedRows), rowNum)
	}
}

// TestPreparedStatementCaching verifies that prepared statements are cached and reused.
func TestPreparedStatementCaching(t *testing.T) {
	logger := zap.NewNop()
	cfg := Config{
		MainDBPath:   ":memory:",
		AuthDBPath:   ":memory:",
		Threads:      2,
		AccessMode:   "read_write",
		QueryTimeout: 5 * time.Second,
		Logger:       logger,
	}

	mgr, err := NewManagerForTesting(cfg)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Create a test table
	createTableSQL := `
		CREATE TABLE test_cache (
			id INTEGER PRIMARY KEY,
			value VARCHAR
		)
	`
	_, err = mgr.ExecMain(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert first row - this will create and cache the prepared statement
	data1 := map[string]interface{}{"id": 1, "value": "first"}
	_, err = mgr.Insert("test_cache", data1)
	if err != nil {
		t.Fatalf("Failed first insert: %v", err)
	}

	// Check that the prepared statement was cached
	stmtKey := "test_cache:insert"
	cached, ok := mgr.preparedStmts.Load(stmtKey)
	if !ok {
		t.Error("Expected prepared statement to be cached")
	}
	if cached == nil {
		t.Error("Expected non-nil cached statement")
	}

	// Insert second row - should reuse the cached prepared statement
	data2 := map[string]interface{}{"id": 2, "value": "second"}
	_, err = mgr.Insert("test_cache", data2)
	if err != nil {
		t.Fatalf("Failed second insert: %v", err)
	}

	// Verify the same statement is still cached
	cached2, ok := mgr.preparedStmts.Load(stmtKey)
	if !ok {
		t.Error("Expected prepared statement to still be cached")
	}
	if cached != cached2 {
		t.Error("Expected same prepared statement instance to be reused")
	}
}

// TestTableSchemaCaching verifies that table schemas are cached.
func TestTableSchemaCaching(t *testing.T) {
	logger := zap.NewNop()
	cfg := Config{
		MainDBPath:   ":memory:",
		AuthDBPath:   ":memory:",
		Threads:      2,
		AccessMode:   "read_write",
		QueryTimeout: 5 * time.Second,
		Logger:       logger,
	}

	mgr, err := NewManagerForTesting(cfg)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Create a test table
	createTableSQL := `
		CREATE TABLE test_schema (
			col1 INTEGER,
			col2 VARCHAR,
			col3 TIMESTAMP
		)
	`
	_, err = mgr.ExecMain(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// First call - should query the database
	columns1, err := mgr.getTableColumns("test_schema")
	if err != nil {
		t.Fatalf("Failed to get table columns: %v", err)
	}

	expectedColumns := []string{"col1", "col2", "col3"}
	if len(columns1) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns1))
	}
	for i, col := range expectedColumns {
		if columns1[i] != col {
			t.Errorf("Expected column %d to be %s, got %s", i, col, columns1[i])
		}
	}

	// Second call - should use cache
	columns2, err := mgr.getTableColumns("test_schema")
	if err != nil {
		t.Fatalf("Failed to get cached table columns: %v", err)
	}

	// Verify same result
	if len(columns2) != len(columns1) {
		t.Error("Cached columns differ in length")
	}
	for i := range columns1 {
		if columns2[i] != columns1[i] {
			t.Errorf("Cached column %d differs: expected %s, got %s", i, columns1[i], columns2[i])
		}
	}

	// Test cache invalidation
	mgr.InvalidateTableSchema("test_schema")

	// After invalidation, should query database again
	columns3, err := mgr.getTableColumns("test_schema")
	if err != nil {
		t.Fatalf("Failed to get table columns after invalidation: %v", err)
	}

	if len(columns3) != len(expectedColumns) {
		t.Errorf("Expected %d columns after invalidation, got %d", len(expectedColumns), len(columns3))
	}
}

// Helper functions
func strPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}

func compareNullableString(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func compareNullableInt(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
