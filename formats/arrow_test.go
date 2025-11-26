package formats

import (
	"bytes"
	"database/sql"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestWriteArrowIPC_BasicOutput(t *testing.T) {
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
	err = WriteArrowIPC(rec, rows)
	if err != nil {
		t.Fatalf("WriteArrowIPC failed: %v", err)
	}

	// Verify response headers
	if rec.Code != 200 {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/vnd.apache.arrow.stream" {
		t.Errorf("Expected Content-Type 'application/vnd.apache.arrow.stream', got '%s'", ct)
	}

	// Verify the Arrow IPC stream is valid by reading it back
	reader, err := ipc.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Arrow IPC reader: %v", err)
	}
	defer reader.Release()

	// Verify schema
	schema := reader.Schema()
	if schema.NumFields() != 6 {
		t.Errorf("Expected 6 fields in schema, got %d", schema.NumFields())
	}

	// Read all records
	totalRows := int64(0)
	for reader.Next() {
		rec := reader.Record()
		totalRows += rec.NumRows()
	}

	if totalRows != 3 {
		t.Errorf("Expected 3 rows, got %d", totalRows)
	}
}

func TestWriteArrowIPC_EmptyResult(t *testing.T) {
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
	err = WriteArrowIPC(rec, rows)
	if err != nil {
		t.Fatalf("WriteArrowIPC failed: %v", err)
	}

	// Verify the stream is valid (even if empty)
	reader, err := ipc.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Arrow IPC reader: %v", err)
	}
	defer reader.Release()

	// Should have schema but no data
	schema := reader.Schema()
	if schema.NumFields() != 6 {
		t.Errorf("Expected 6 fields in schema, got %d", schema.NumFields())
	}

	totalRows := int64(0)
	for reader.Next() {
		rec := reader.Record()
		totalRows += rec.NumRows()
	}

	if totalRows != 0 {
		t.Errorf("Expected 0 rows, got %d", totalRows)
	}
}

func TestWriteArrowIPC_AllDataTypes(t *testing.T) {
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
	err = WriteArrowIPC(rec, rows)
	if err != nil {
		t.Fatalf("WriteArrowIPC failed: %v", err)
	}

	// Verify the Arrow IPC stream is valid
	reader, err := ipc.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Arrow IPC reader: %v", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema.NumFields() != 11 {
		t.Errorf("Expected 11 fields in schema, got %d", schema.NumFields())
	}
}

func TestWriteArrow_Alias(t *testing.T) {
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
	// Test that WriteArrow is an alias for WriteArrowIPC
	err = WriteArrow(rec, rows)
	if err != nil {
		t.Fatalf("WriteArrow failed: %v", err)
	}

	if rec.Code != 200 {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
}

func TestSqlTypeToArrowType(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	// Create a table with various DuckDB types to test mapping
	_, err = db.Exec(`
		CREATE TABLE type_test (
			bool_col BOOLEAN,
			int_col INTEGER,
			bigint_col BIGINT,
			double_col DOUBLE,
			varchar_col VARCHAR,
			date_col DATE,
			timestamp_col TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create type test table: %v", err)
	}

	rows, err := db.Query("SELECT * FROM type_test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("Failed to get column types: %v", err)
	}

	expectedTypes := map[string]arrow.DataType{
		"bool_col":      arrow.FixedWidthTypes.Boolean,
		"int_col":       arrow.PrimitiveTypes.Int32,
		"bigint_col":    arrow.PrimitiveTypes.Int64,
		"double_col":    arrow.PrimitiveTypes.Float64,
		"varchar_col":   arrow.BinaryTypes.String,
		"date_col":      arrow.FixedWidthTypes.Date32,
		"timestamp_col": arrow.FixedWidthTypes.Timestamp_us,
	}

	for i, colType := range columnTypes {
		arrowType, _ := sqlTypeToArrowType(colType)
		colName := colType.Name()
		expected, ok := expectedTypes[colName]
		if !ok {
			continue // Skip columns we don't have expectations for
		}

		if arrowType.ID() != expected.ID() {
			t.Errorf("Column %d (%s): expected Arrow type %v, got %v",
				i, colName, expected, arrowType)
		}
	}
}

func TestBuildRecordBatch(t *testing.T) {
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

	rows, err := db.Query("SELECT id, name FROM test_data ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("Failed to get column types: %v", err)
	}

	// Build schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)
	pool := memory.NewGoAllocator()

	// Build a batch with all rows
	record, hasMore, err := buildRecordBatch(rows, schema, pool, 100, columnTypes)
	if err != nil {
		t.Fatalf("buildRecordBatch failed: %v", err)
	}
	if record == nil {
		t.Fatal("Expected non-nil record")
	}
	defer record.Release()

	if record.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", record.NumRows())
	}
	if record.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", record.NumCols())
	}
	if hasMore {
		t.Error("Expected hasMore to be false since we got all rows")
	}
}

func TestBuildRecordBatch_Batching(t *testing.T) {
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

	rows, err := db.Query("SELECT id, name FROM test_data ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("Failed to get column types: %v", err)
	}

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)
	pool := memory.NewGoAllocator()

	// Build batches with size 2 (should get 2 batches for 3 rows)
	record1, hasMore1, err := buildRecordBatch(rows, schema, pool, 2, columnTypes)
	if err != nil {
		t.Fatalf("First batch failed: %v", err)
	}
	if record1 == nil {
		t.Fatal("Expected non-nil first record")
	}
	defer record1.Release()

	if record1.NumRows() != 2 {
		t.Errorf("First batch: expected 2 rows, got %d", record1.NumRows())
	}
	if !hasMore1 {
		t.Error("Expected hasMore to be true after first batch")
	}

	// Second batch
	record2, hasMore2, err := buildRecordBatch(rows, schema, pool, 2, columnTypes)
	if err != nil {
		t.Fatalf("Second batch failed: %v", err)
	}
	if record2 == nil {
		t.Fatal("Expected non-nil second record")
	}
	defer record2.Release()

	if record2.NumRows() != 1 {
		t.Errorf("Second batch: expected 1 row, got %d", record2.NumRows())
	}
	if hasMore2 {
		t.Error("Expected hasMore to be false after second batch")
	}
}

func TestWriteArrowIPC_NullValues(t *testing.T) {
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
	err = WriteArrowIPC(rec, rows)
	if err != nil {
		t.Fatalf("WriteArrowIPC failed: %v", err)
	}

	// Verify the stream is valid
	reader, err := ipc.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create Arrow IPC reader: %v", err)
	}
	defer reader.Release()

	// Read the record and verify null handling
	if !reader.Next() {
		t.Fatal("Expected at least one record batch")
	}
	record := reader.Record()
	if record.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", record.NumRows())
	}
}

// Benchmark Arrow IPC writing
func BenchmarkWriteArrowIPC(b *testing.B) {
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
		WriteArrowIPC(rec, rows)
		rows.Close()
	}
}

// Test sqlTypeToArrowType with mock column type
type mockColumnType struct {
	name         string
	databaseType string
	nullable     bool
	hasNullable  bool
	scanType     interface{}
}

func (m *mockColumnType) Name() string                      { return m.name }
func (m *mockColumnType) DatabaseTypeName() string          { return m.databaseType }
func (m *mockColumnType) Nullable() (nullable, ok bool)     { return m.nullable, m.hasNullable }
func (m *mockColumnType) ScanType() interface{}             { return m.scanType }
func (m *mockColumnType) Length() (length int64, ok bool)   { return 0, false }
func (m *mockColumnType) DecimalSize() (int64, int64, bool) { return 0, 0, false }

// Ensure mockColumnType satisfies the required interface
var _ interface {
	Name() string
	DatabaseTypeName() string
	Nullable() (nullable, ok bool)
} = (*mockColumnType)(nil)

func TestSqlTypeToArrowType_AllTypes(t *testing.T) {
	// Test by creating actual tables with specific types and checking the mapping
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	tests := []struct {
		sqlType    string
		createSQL  string
		expectedID arrow.Type
	}{
		{"BOOLEAN", "CREATE TABLE t1 (c BOOLEAN)", arrow.BOOL},
		{"INTEGER", "CREATE TABLE t2 (c INTEGER)", arrow.INT32},
		{"BIGINT", "CREATE TABLE t3 (c BIGINT)", arrow.INT64},
		{"DOUBLE", "CREATE TABLE t4 (c DOUBLE)", arrow.FLOAT64},
		{"VARCHAR", "CREATE TABLE t5 (c VARCHAR)", arrow.STRING},
	}

	for _, tt := range tests {
		t.Run(tt.sqlType, func(t *testing.T) {
			_, err := db.Exec(tt.createSQL)
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			// Get table name from create SQL
			tableName := ""
			for _, word := range []string{"t1", "t2", "t3", "t4", "t5"} {
				if containsWord(tt.createSQL, word) {
					tableName = word
					break
				}
			}

			rows, err := db.Query("SELECT c FROM " + tableName)
			if err != nil {
				t.Fatalf("Failed to query: %v", err)
			}
			defer rows.Close()

			columnTypes, err := rows.ColumnTypes()
			if err != nil {
				t.Fatalf("Failed to get column types: %v", err)
			}

			if len(columnTypes) != 1 {
				t.Fatalf("Expected 1 column type, got %d", len(columnTypes))
			}

			arrowType, _ := sqlTypeToArrowType(columnTypes[0])
			if arrowType.ID() != tt.expectedID {
				t.Errorf("Expected Arrow type %v, got %v", tt.expectedID, arrowType.ID())
			}
		})
	}
}

func containsWord(s, word string) bool {
	return len(s) >= len(word) && (s == word ||
		(len(s) > len(word) && (s[:len(word)+1] == word+" " ||
			s[len(s)-len(word)-1:] == " "+word ||
			containsSubstring(s, " "+word+" "))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Verify sql.ColumnType interface requirements
var _ interface {
	Name() string
	DatabaseTypeName() string
	Nullable() (nullable, ok bool)
} = (*sql.ColumnType)(nil)
