package database

import (
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"
)

func setupTestManager(t *testing.T) *Manager {
	cfg := Config{
		MainDBPath:   ":memory:",
		AuthDBPath:   ":memory:",
		Threads:      1,
		AccessMode:   "read_write",
		QueryTimeout: 30 * time.Second,
		Logger:       zap.NewNop(),
	}

	mgr, err := NewManagerForTesting(cfg)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create a test table
	_, err = mgr.ExecMain(`
		CREATE TABLE test_users (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			email VARCHAR,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Small delay to ensure table is ready
	time.Sleep(10 * time.Millisecond)

	return mgr
}

func TestInsert(t *testing.T) {
	mgr := setupTestManager(t)
	defer mgr.Close()

	data := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
	}

	result, err := mgr.Insert("test_users", data)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify data was inserted
	var count int
	err = mgr.QueryRowScanMain("SELECT COUNT(*) FROM test_users", []interface{}{&count})
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row in table, got %d", count)
	}
}

func TestUpdate(t *testing.T) {
	mgr := setupTestManager(t)
	defer mgr.Close()

	// Insert test data
	data := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
	}
	_, err := mgr.Insert("test_users", data)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Update data
	set := map[string]interface{}{
		"age": 31,
	}
	where := map[string]interface{}{
		"id": 1,
	}

	result, err := mgr.Update("test_users", set, where)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify data was updated
	var age int
	err = mgr.QueryRowScanMain("SELECT age FROM test_users WHERE id = 1", []interface{}{&age})
	if err != nil {
		t.Fatalf("Failed to query updated data: %v", err)
	}
	if age != 31 {
		t.Errorf("Expected age 31, got %d", age)
	}
}

func TestDelete(t *testing.T) {
	mgr := setupTestManager(t)
	defer mgr.Close()

	// Insert test data
	data := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
	}
	_, err := mgr.Insert("test_users", data)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Delete data
	where := map[string]interface{}{
		"id": 1,
	}

	result, err := mgr.Delete("test_users", where)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify data was deleted
	var count int
	err = mgr.QueryRowScanMain("SELECT COUNT(*) FROM test_users", []interface{}{&count})
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows in table, got %d", count)
	}
}

func TestSelect(t *testing.T) {
	mgr := setupTestManager(t)
	defer mgr.Close()

	// Insert test data
	testData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "email": "alice@example.com", "age": 25},
		{"id": 2, "name": "Bob", "email": "bob@example.com", "age": 30},
		{"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35},
	}

	for _, data := range testData {
		_, err := mgr.Insert("test_users", data)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test select with no filters
	rows, err := mgr.Select("test_users", nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

func TestSelectWithFilters(t *testing.T) {
	mgr := setupTestManager(t)
	defer mgr.Close()

	// Insert test data
	testData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "email": "alice@example.com", "age": 25},
		{"id": 2, "name": "Bob", "email": "bob@example.com", "age": 30},
		{"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35},
	}

	for _, data := range testData {
		_, err := mgr.Insert("test_users", data)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test select with filter
	filters := []Filter{
		{Column: "age", Operator: "gte", Value: 30},
	}

	rows, err := mgr.Select("test_users", filters, nil, 0, 0)
	if err != nil {
		t.Fatalf("Select with filter failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 rows with age >= 30, got %d", count)
	}
}

func TestSelectWithPagination(t *testing.T) {
	mgr := setupTestManager(t)
	defer mgr.Close()

	// Insert test data
	for i := 1; i <= 10; i++ {
		data := map[string]interface{}{
			"id":    i,
			"name":  "User" + string(rune(i)),
			"email": "user" + string(rune(i)) + "@example.com",
			"age":   20 + i,
		}
		_, err := mgr.Insert("test_users", data)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test with limit
	rows, err := mgr.Select("test_users", nil, nil, 5, 0)
	if err != nil {
		t.Fatalf("Select with limit failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 5 {
		t.Errorf("Expected 5 rows with limit, got %d", count)
	}
}

func TestCount(t *testing.T) {
	mgr := setupTestManager(t)
	defer mgr.Close()

	// Insert test data
	testData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "email": "alice@example.com", "age": 25},
		{"id": 2, "name": "Bob", "email": "bob@example.com", "age": 30},
		{"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35},
	}

	for _, data := range testData {
		_, err := mgr.Insert("test_users", data)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test count
	count, err := mgr.Count("test_users", nil)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}
}

func TestTableExists(t *testing.T) {
	mgr := setupTestManager(t)
	defer mgr.Close()

	// Test existing table
	exists, err := mgr.TableExists("test_users")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Error("Expected test_users table to exist")
	}

	// Test non-existing table
	exists, err = mgr.TableExists("non_existent_table")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if exists {
		t.Error("Expected non_existent_table to not exist")
	}
}

func TestFilterToSQL(t *testing.T) {
	tests := []struct {
		filter   Filter
		expected string
	}{
		{Filter{Column: "age", Operator: "eq", Value: 30}, "age = $1"},
		{Filter{Column: "age", Operator: "ne", Value: 30}, "age != $1"},
		{Filter{Column: "age", Operator: "gt", Value: 30}, "age > $1"},
		{Filter{Column: "age", Operator: "gte", Value: 30}, "age >= $1"},
		{Filter{Column: "age", Operator: "lt", Value: 30}, "age < $1"},
		{Filter{Column: "age", Operator: "lte", Value: 30}, "age <= $1"},
		{Filter{Column: "name", Operator: "like", Value: "John%"}, "name LIKE $1"},
	}

	for _, tt := range tests {
		sql, _ := tt.filter.ToSQL(1)
		if sql != tt.expected {
			t.Errorf("Expected SQL '%s', got '%s'", tt.expected, sql)
		}
	}
}

func TestSortToSQL(t *testing.T) {
	tests := []struct {
		sort     Sort
		expected string
	}{
		{Sort{Column: "name", Direction: "asc"}, "name ASC"},
		{Sort{Column: "name", Direction: "desc"}, "name DESC"},
		{Sort{Column: "name", Direction: "ASC"}, "name ASC"},
		{Sort{Column: "name", Direction: "DESC"}, "name DESC"},
		{Sort{Column: "name", Direction: ""}, "name ASC"},
	}

	for _, tt := range tests {
		sql := tt.sort.ToSQL()
		if sql != tt.expected {
			t.Errorf("Expected SQL '%s', got '%s'", tt.expected, sql)
		}
	}
}
