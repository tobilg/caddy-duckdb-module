package formats

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v18/parquet/file"
)

func TestWriteParquet_BasicOutput(t *testing.T) {
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
	err = WriteParquet(rec, rows)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	// Verify response headers
	if rec.Code != 200 {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/parquet" {
		t.Errorf("Expected Content-Type 'application/parquet', got '%s'", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, "attachment") {
		t.Errorf("Expected Content-Disposition with attachment, got '%s'", cd)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, ".parquet") {
		t.Errorf("Expected Content-Disposition with .parquet filename, got '%s'", cd)
	}

	// Verify the Parquet file is valid by reading it
	reader, err := file.NewParquetReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer reader.Close()

	// Verify schema
	schema := reader.MetaData().Schema
	if schema.NumColumns() != 6 {
		t.Errorf("Expected 6 columns in schema, got %d", schema.NumColumns())
	}

	// Verify row count
	numRows := reader.NumRows()
	if numRows != 3 {
		t.Errorf("Expected 3 rows, got %d", numRows)
	}
}

func TestWriteParquet_EmptyResult(t *testing.T) {
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
	err = WriteParquet(rec, rows)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	// Verify the Parquet file is valid (even if empty)
	reader, err := file.NewParquetReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer reader.Close()

	// Should have schema but no data
	schema := reader.MetaData().Schema
	if schema.NumColumns() != 6 {
		t.Errorf("Expected 6 columns in schema, got %d", schema.NumColumns())
	}

	numRows := reader.NumRows()
	if numRows != 0 {
		t.Errorf("Expected 0 rows, got %d", numRows)
	}
}

func TestWriteParquet_AllDataTypes(t *testing.T) {
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
	err = WriteParquet(rec, rows)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	// Verify the Parquet file is valid
	reader, err := file.NewParquetReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer reader.Close()

	schema := reader.MetaData().Schema
	if schema.NumColumns() != 11 {
		t.Errorf("Expected 11 columns in schema, got %d", schema.NumColumns())
	}
}

func TestWriteParquet_NullValues(t *testing.T) {
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
	err = WriteParquet(rec, rows)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	// Verify the Parquet file is valid
	reader, err := file.NewParquetReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", reader.NumRows())
	}
}

func TestWriteParquet_LargeDataset(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	// Create table and insert many rows
	_, err = db.Exec("CREATE TABLE large_test (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert 1000 rows
	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO large_test VALUES (?, ?)", i, "value")
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	rows, err := db.Query("SELECT * FROM large_test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteParquet(rec, rows)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	// Verify the Parquet file is valid
	reader, err := file.NewParquetReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 1000 {
		t.Errorf("Expected 1000 rows, got %d", reader.NumRows())
	}
}

func TestWriteParquet_Compression(t *testing.T) {
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
	err = WriteParquet(rec, rows)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	// Verify the file uses Snappy compression by checking metadata
	reader, err := file.NewParquetReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer reader.Close()

	// The file should be valid and readable regardless of compression
	// We can't easily check compression type without diving deeper into metadata
	// but we can verify the file is readable which means compression/decompression works
	if reader.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", reader.NumRows())
	}
}

// Benchmark Parquet writing
func BenchmarkWriteParquet(b *testing.B) {
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
		WriteParquet(rec, rows)
		rows.Close()
	}
}

func BenchmarkWriteParquet_LargeDataset(b *testing.B) {
	db, err := createTestDB()
	if err != nil {
		b.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE bench_test (id INTEGER, value VARCHAR)")
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert 1000 rows
	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO bench_test VALUES (?, ?)", i, "benchmark value")
		if err != nil {
			b.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT * FROM bench_test")
		rec := httptest.NewRecorder()
		WriteParquet(rec, rows)
		rows.Close()
	}
}
