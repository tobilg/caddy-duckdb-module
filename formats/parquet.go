package formats

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/compress"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

// WriteParquet writes query results as Parquet format.
// This function converts SQL rows to Arrow Table and then writes to Parquet format.
func WriteParquet(w http.ResponseWriter, rows *sql.Rows) error {
	// Get column types and names
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %w", err)
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %w", err)
	}

	// Build Arrow schema (reusing the same logic from arrow.go)
	fields := make([]arrow.Field, len(columnNames))
	for i, colType := range columnTypes {
		arrowType, nullable := sqlTypeToArrowType(colType)
		fields[i] = arrow.Field{
			Name:     columnNames[i],
			Type:     arrowType,
			Nullable: nullable,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// Create memory allocator
	pool := memory.NewGoAllocator()

	// Collect all rows into record batches
	const batchSize = 10000 // Larger batch size for Parquet
	var recordBatches []arrow.Record

	for {
		record, hasMore, err := buildRecordBatch(rows, schema, pool, batchSize, columnTypes)
		if err != nil {
			// Clean up any previously created records
			for _, r := range recordBatches {
				r.Release()
			}
			return fmt.Errorf("failed to build record batch: %w", err)
		}

		if record == nil {
			break
		}

		recordBatches = append(recordBatches, record)

		if !hasMore {
			break
		}
	}

	// If no data, create an empty record with proper column arrays
	if len(recordBatches) == 0 {
		// Create empty arrays for each column in the schema
		emptyArrays := make([]arrow.Array, len(schema.Fields()))
		for i, field := range schema.Fields() {
			builder := array.NewBuilder(pool, field.Type)
			emptyArrays[i] = builder.NewArray()
			builder.Release()
		}
		emptyBatch := array.NewRecord(schema, emptyArrays, 0)
		// Release the empty arrays (record holds references)
		for _, arr := range emptyArrays {
			arr.Release()
		}
		recordBatches = append(recordBatches, emptyBatch)
	}

	// Create Arrow Table from record batches
	table := array.NewTableFromRecords(schema, recordBatches)
	defer table.Release()

	// Release record batches (table holds references)
	for _, record := range recordBatches {
		record.Release()
	}

	// Configure Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy), // Use Snappy compression
		parquet.WithDictionaryDefault(true),             // Enable dictionary encoding
	)

	arrowWriterProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(), // Store Arrow schema in metadata
	)

	// Set content type for Parquet
	w.Header().Set("Content-Type", "application/parquet")
	w.Header().Set("Content-Disposition", "attachment; filename=\"query_result.parquet\"")
	w.WriteHeader(http.StatusOK)

	// Write table to Parquet format directly to HTTP response
	err = pqarrow.WriteTable(table, w, table.NumRows(), writerProps, arrowWriterProps)
	if err != nil {
		return fmt.Errorf("failed to write parquet: %w", err)
	}

	return nil
}
