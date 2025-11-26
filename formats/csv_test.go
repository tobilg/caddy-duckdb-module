package formats

import (
	"encoding/csv"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteCSV_BasicOutput(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertTestData(db); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	rows, err := getTestRows(db)
	if err != nil {
		t.Fatalf("Failed to get test rows: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteCSV(rec, rows)
	if err != nil {
		t.Fatalf("WriteCSV failed: %v", err)
	}

	// Verify response headers
	if rec.Code != 200 {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/csv" {
		t.Errorf("Expected Content-Type 'text/csv', got '%s'", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, "attachment") {
		t.Errorf("Expected Content-Disposition with attachment, got '%s'", cd)
	}

	// Parse CSV output
	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	// Check header row
	if len(records) < 1 {
		t.Fatal("Expected at least header row")
	}
	header := records[0]
	expectedCols := []string{"id", "name", "age", "score", "active", "created_at"}
	if len(header) != len(expectedCols) {
		t.Errorf("Expected %d columns, got %d", len(expectedCols), len(header))
	}

	// Check data rows (3 test records)
	if len(records) != 4 { // header + 3 data rows
		t.Errorf("Expected 4 rows (1 header + 3 data), got %d", len(records))
	}
}

func TestWriteCSV_EmptyResult(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	rows, err := getEmptyRows(db)
	if err != nil {
		t.Fatalf("Failed to get empty rows: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteCSV(rec, rows)
	if err != nil {
		t.Fatalf("WriteCSV failed: %v", err)
	}

	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	// Should have header row only
	if len(records) != 1 {
		t.Errorf("Expected 1 row (header only), got %d", len(records))
	}
}

func TestWriteCSV_NullValues(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertNullData(db); err != nil {
		t.Fatalf("Failed to insert null data: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test_data WHERE id = 4")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteCSV(rec, rows)
	if err != nil {
		t.Fatalf("WriteCSV failed: %v", err)
	}

	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(records))
	}

	dataRow := records[1]
	// NULL values should be empty strings in CSV
	// id=4, name=NULL, age=NULL, score=NULL, active=NULL, created_at=NULL
	if dataRow[1] != "" { // name column
		t.Errorf("Expected empty string for NULL name, got '%s'", dataRow[1])
	}
}

func TestWriteCSV_SpecialCharacters(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE special_chars (id INTEGER, text VARCHAR)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with special CSV characters
	testCases := []struct {
		id   int
		text string
	}{
		{1, "Hello, World"},             // comma
		{2, `He said "Hello"`},          // quotes
		{3, "Line1\nLine2"},             // newline
		{4, "Normal text"},              // no special chars
		{5, `Comma, "quotes", newline`}, // mixed
	}

	for _, tc := range testCases {
		_, err = db.Exec("INSERT INTO special_chars VALUES (?, ?)", tc.id, tc.text)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	rows, err := db.Query("SELECT * FROM special_chars ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteCSV(rec, rows)
	if err != nil {
		t.Fatalf("WriteCSV failed: %v", err)
	}

	// Parse the CSV - should handle escaping correctly
	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV with special chars: %v", err)
	}

	if len(records) != 6 { // header + 5 data rows
		t.Errorf("Expected 6 rows, got %d", len(records))
	}

	// Verify the values are preserved correctly
	for i, tc := range testCases {
		dataRow := records[i+1]
		if dataRow[1] != tc.text {
			t.Errorf("Row %d: expected '%s', got '%s'", tc.id, tc.text, dataRow[1])
		}
	}
}

func TestFormatCSVValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"bytes", []byte("world"), "world"},
		{"int", 42, "42"},
		{"int64", int64(9223372036854775807), "9223372036854775807"},
		{"float64", 3.14159, "3.141590"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"uint", uint(100), "100"},
		{"complex type fallback", struct{ Name string }{"test"}, "{test}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatCSVValue(tt.input)
			if result != tt.expected {
				t.Errorf("formatCSVValue(%v) = '%s', want '%s'", tt.input, result, tt.expected)
			}
		})
	}
}

func TestWriteCSV_AllDataTypes(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createAllTypesTable(db); err != nil {
		t.Fatalf("Failed to create all types table: %v", err)
	}
	if err := insertAllTypesData(db); err != nil {
		t.Fatalf("Failed to insert all types data: %v", err)
	}

	rows, err := getAllTypesRows(db)
	if err != nil {
		t.Fatalf("Failed to get all types rows: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteCSV(rec, rows)
	if err != nil {
		t.Fatalf("WriteCSV failed: %v", err)
	}

	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	if len(records) != 2 { // header + 1 data row
		t.Errorf("Expected 2 rows, got %d", len(records))
	}

	// Verify header has all expected columns
	expectedCols := []string{"bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col",
		"float_col", "double_col", "varchar_col", "date_col", "timestamp_col", "blob_col"}
	header := records[0]
	if len(header) != len(expectedCols) {
		t.Errorf("Expected %d columns, got %d", len(expectedCols), len(header))
	}
}

// Benchmark CSV writing
func BenchmarkWriteCSV(b *testing.B) {
	db, err := createTestDB()
	if err != nil {
		b.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		b.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertTestData(db); err != nil {
		b.Fatalf("Failed to insert test data: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := getTestRows(db)
		rec := httptest.NewRecorder()
		WriteCSV(rec, rows)
		rows.Close()
	}
}
