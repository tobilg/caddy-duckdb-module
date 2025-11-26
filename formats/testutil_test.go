package formats

import (
	"database/sql"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// createTestDB creates an in-memory DuckDB database for testing
func createTestDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, err
	}
	return db, nil
}

// createTestTable creates a test table with various data types
func createTestTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE test_data (
			id INTEGER,
			name VARCHAR,
			age INTEGER,
			score DOUBLE,
			active BOOLEAN,
			created_at TIMESTAMP
		)
	`)
	return err
}

// insertTestData inserts sample test data
func insertTestData(db *sql.DB) error {
	_, err := db.Exec(`
		INSERT INTO test_data VALUES
			(1, 'Alice', 30, 95.5, true, '2024-01-15 10:30:00'),
			(2, 'Bob', 25, 87.3, false, '2024-01-16 14:45:00'),
			(3, 'Charlie', 35, 92.1, true, '2024-01-17 09:00:00')
	`)
	return err
}

// insertNullData inserts data with NULL values
func insertNullData(db *sql.DB) error {
	_, err := db.Exec(`
		INSERT INTO test_data VALUES
			(4, NULL, NULL, NULL, NULL, NULL)
	`)
	return err
}

// createAllTypesTable creates a table with many DuckDB data types for comprehensive testing
func createAllTypesTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE all_types (
			bool_col BOOLEAN,
			tinyint_col TINYINT,
			smallint_col SMALLINT,
			int_col INTEGER,
			bigint_col BIGINT,
			float_col FLOAT,
			double_col DOUBLE,
			varchar_col VARCHAR,
			date_col DATE,
			timestamp_col TIMESTAMP,
			blob_col BLOB
		)
	`)
	return err
}

// insertAllTypesData inserts test data for all types table
func insertAllTypesData(db *sql.DB) error {
	stmt, err := db.Prepare(`
		INSERT INTO all_types VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		true,                       // bool
		int8(127),                  // tinyint
		int16(32000),               // smallint
		int32(2147483647),          // integer
		int64(9223372036854775807), // bigint
		float32(3.14),              // float
		float64(3.141592653589793), // double
		"Hello, World!",            // varchar
		time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),   // date
		time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), // timestamp
		[]byte{0x48, 0x65, 0x6c, 0x6c, 0x6f},           // blob
	)
	return err
}

// getTestRows queries the test table and returns sql.Rows
func getTestRows(db *sql.DB) (*sql.Rows, error) {
	return db.Query("SELECT * FROM test_data ORDER BY id")
}

// getEmptyRows returns rows from an empty query result
func getEmptyRows(db *sql.DB) (*sql.Rows, error) {
	return db.Query("SELECT * FROM test_data WHERE 1=0")
}

// getAllTypesRows returns rows from all_types table
func getAllTypesRows(db *sql.DB) (*sql.Rows, error) {
	return db.Query("SELECT * FROM all_types")
}
