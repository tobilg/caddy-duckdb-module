package formats

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/decimal128"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// WriteArrowIPC writes query results as Apache Arrow IPC stream format.
// This format is ideal for HTTP streaming and zero-copy data transfer.
func WriteArrowIPC(w http.ResponseWriter, rows *sql.Rows) error {
	// Get column types and names
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %w", err)
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %w", err)
	}

	// Build Arrow schema
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

	// Set content type for Arrow IPC stream
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.WriteHeader(http.StatusOK)

	// Create IPC writer that writes directly to HTTP response
	writer := ipc.NewWriter(w, ipc.WithSchema(schema), ipc.WithAllocator(pool))
	defer writer.Close()

	// Process rows in batches for memory efficiency
	const batchSize = 1024

	for {
		// Build record batch
		record, hasMore, err := buildRecordBatch(rows, schema, pool, batchSize, columnTypes)
		if err != nil {
			return fmt.Errorf("failed to build record batch: %w", err)
		}

		if record == nil {
			break
		}

		// Write record batch to stream
		if err := writer.Write(record); err != nil {
			record.Release()
			return fmt.Errorf("failed to write record batch: %w", err)
		}

		record.Release()

		if !hasMore {
			break
		}
	}

	return nil
}

// buildRecordBatch builds a single Arrow record batch from sql.Rows
func buildRecordBatch(rows *sql.Rows, schema *arrow.Schema, pool memory.Allocator, batchSize int, columnTypes []*sql.ColumnType) (arrow.Record, bool, error) {
	// Create builders for each column
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(pool, field.Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	// Scan rows into builders
	rowCount := 0
	for rowCount < batchSize && rows.Next() {
		// Create scan targets
		values := make([]interface{}, len(columnTypes))
		valuePtrs := make([]interface{}, len(columnTypes))
		for i := range columnTypes {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, false, fmt.Errorf("failed to scan row: %w", err)
		}

		// Append values to builders
		for i, val := range values {
			if err := appendValueToBuilder(builders[i], val, columnTypes[i]); err != nil {
				return nil, false, fmt.Errorf("failed to append value to builder at column %d: %w", i, err)
			}
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("error iterating rows: %w", err)
	}

	if rowCount == 0 {
		return nil, false, nil
	}

	// Build arrays from builders
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}

	// Create record
	record := array.NewRecord(schema, arrays, int64(rowCount))

	// Release arrays (record holds references)
	for _, arr := range arrays {
		arr.Release()
	}

	// Check if there are more rows
	hasMore := rowCount == batchSize

	return record, hasMore, nil
}

// sqlTypeToArrowType maps SQL column types to Arrow types
func sqlTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, bool) {
	nullable, ok := colType.Nullable()
	if !ok {
		nullable = true // Default to nullable if unknown
	}

	dbType := colType.DatabaseTypeName()

	// Map based on database type name (DuckDB-specific)
	switch dbType {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, nullable
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, nullable
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nullable
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32, nullable
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nullable
	case "UTINYINT":
		return arrow.PrimitiveTypes.Uint8, nullable
	case "USMALLINT":
		return arrow.PrimitiveTypes.Uint16, nullable
	case "UINTEGER":
		return arrow.PrimitiveTypes.Uint32, nullable
	case "UBIGINT":
		return arrow.PrimitiveTypes.Uint64, nullable
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, nullable
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nullable
	case "DECIMAL":
		// Use default decimal(38, 9) - could be made more precise
		return &arrow.Decimal128Type{Precision: 38, Scale: 9}, nullable
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nullable
	case "TIMESTAMP":
		return arrow.FixedWidthTypes.Timestamp_us, nullable
	case "TIME":
		return arrow.FixedWidthTypes.Time64us, nullable
	case "INTERVAL":
		return arrow.FixedWidthTypes.Duration_us, nullable
	case "VARCHAR", "TEXT", "STRING":
		return arrow.BinaryTypes.String, nullable
	case "BLOB", "BYTEA":
		return arrow.BinaryTypes.Binary, nullable
	case "UUID":
		return arrow.BinaryTypes.String, nullable // UUIDs as strings
	case "JSON":
		return arrow.BinaryTypes.String, nullable // JSON as strings
	default:
		// Fallback based on Go scan type
		scanType := colType.ScanType()
		if scanType != nil {
			switch scanType.Kind() {
			case 1: // bool
				return arrow.FixedWidthTypes.Boolean, nullable
			case 2, 3, 4, 5, 6: // int variants
				return arrow.PrimitiveTypes.Int64, nullable
			case 7, 8, 9, 10, 11: // uint variants
				return arrow.PrimitiveTypes.Uint64, nullable
			case 13, 14: // float32, float64
				return arrow.PrimitiveTypes.Float64, nullable
			case 24: // string
				return arrow.BinaryTypes.String, nullable
			}
		}
		// Ultimate fallback
		return arrow.BinaryTypes.String, nullable
	}
}

// appendValueToBuilder appends a value to the appropriate Arrow builder
func appendValueToBuilder(builder array.Builder, val interface{}, colType *sql.ColumnType) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	// Convert byte arrays to strings first for string types
	if b, ok := val.([]byte); ok {
		if _, isStringBuilder := builder.(*array.StringBuilder); isStringBuilder {
			val = string(b)
		}
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("expected bool, got %T", val)
		}
	case *array.Int8Builder:
		if v, ok := val.(int8); ok {
			b.Append(v)
		} else if v, ok := val.(int64); ok {
			b.Append(int8(v))
		} else {
			return fmt.Errorf("expected int8, got %T", val)
		}
	case *array.Int16Builder:
		if v, ok := val.(int16); ok {
			b.Append(v)
		} else if v, ok := val.(int64); ok {
			b.Append(int16(v))
		} else {
			return fmt.Errorf("expected int16, got %T", val)
		}
	case *array.Int32Builder:
		if v, ok := val.(int32); ok {
			b.Append(v)
		} else if v, ok := val.(int64); ok {
			b.Append(int32(v))
		} else {
			return fmt.Errorf("expected int32, got %T", val)
		}
	case *array.Int64Builder:
		if v, ok := val.(int64); ok {
			b.Append(v)
		} else if v, ok := val.(int); ok {
			b.Append(int64(v))
		} else if v, ok := val.(int32); ok {
			b.Append(int64(v))
		} else {
			return fmt.Errorf("expected int64, got %T", val)
		}
	case *array.Uint8Builder:
		if v, ok := val.(uint8); ok {
			b.Append(v)
		} else if v, ok := val.(uint64); ok {
			b.Append(uint8(v))
		} else {
			return fmt.Errorf("expected uint8, got %T", val)
		}
	case *array.Uint16Builder:
		if v, ok := val.(uint16); ok {
			b.Append(v)
		} else if v, ok := val.(uint64); ok {
			b.Append(uint16(v))
		} else {
			return fmt.Errorf("expected uint16, got %T", val)
		}
	case *array.Uint32Builder:
		if v, ok := val.(uint32); ok {
			b.Append(v)
		} else if v, ok := val.(uint64); ok {
			b.Append(uint32(v))
		} else {
			return fmt.Errorf("expected uint32, got %T", val)
		}
	case *array.Uint64Builder:
		if v, ok := val.(uint64); ok {
			b.Append(v)
		} else if v, ok := val.(uint); ok {
			b.Append(uint64(v))
		} else {
			return fmt.Errorf("expected uint64, got %T", val)
		}
	case *array.Float32Builder:
		if v, ok := val.(float32); ok {
			b.Append(v)
		} else if v, ok := val.(float64); ok {
			b.Append(float32(v))
		} else {
			return fmt.Errorf("expected float32, got %T", val)
		}
	case *array.Float64Builder:
		if v, ok := val.(float64); ok {
			b.Append(v)
		} else if v, ok := val.(float32); ok {
			b.Append(float64(v))
		} else {
			return fmt.Errorf("expected float64, got %T", val)
		}
	case *array.StringBuilder:
		if v, ok := val.(string); ok {
			b.Append(v)
		} else {
			// Fallback: convert to string
			b.Append(fmt.Sprintf("%v", val))
		}
	case *array.BinaryBuilder:
		if v, ok := val.([]byte); ok {
			b.Append(v)
		} else if v, ok := val.(string); ok {
			b.Append([]byte(v))
		} else {
			return fmt.Errorf("expected []byte or string, got %T", val)
		}
	case *array.Date32Builder:
		if v, ok := val.(time.Time); ok {
			// Convert to days since epoch
			days := arrow.Date32FromTime(v)
			b.Append(days)
		} else {
			return fmt.Errorf("expected time.Time for date, got %T", val)
		}
	case *array.TimestampBuilder:
		if v, ok := val.(time.Time); ok {
			b.Append(arrow.Timestamp(v.UnixMicro()))
		} else {
			return fmt.Errorf("expected time.Time for timestamp, got %T", val)
		}
	case *array.Time64Builder:
		if v, ok := val.(time.Time); ok {
			// Time of day in microseconds
			midnight := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, v.Location())
			microseconds := v.Sub(midnight).Microseconds()
			b.Append(arrow.Time64(microseconds))
		} else {
			return fmt.Errorf("expected time.Time for time, got %T", val)
		}
	case *array.Decimal128Builder:
		// For decimals, we'll need to handle them as strings and convert
		// This is a simplified approach - production code would need more precision
		if v, ok := val.(float64); ok {
			// Convert float to decimal - this is approximate
			dec, err := decimal128.FromFloat64(v, 38, 9)
			if err != nil {
				b.AppendNull()
			} else {
				b.Append(dec)
			}
		} else if v, ok := val.(string); ok {
			// Try to parse as float first
			var f float64
			if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
				dec, err := decimal128.FromFloat64(f, 38, 9)
				if err != nil {
					b.AppendNull()
				} else {
					b.Append(dec)
				}
			} else {
				b.AppendNull()
			}
		} else {
			return fmt.Errorf("expected numeric type for decimal, got %T", val)
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

// WriteArrow is an alias for WriteArrowIPC for backward compatibility
func WriteArrow(w http.ResponseWriter, rows *sql.Rows) error {
	return WriteArrowIPC(w, rows)
}
