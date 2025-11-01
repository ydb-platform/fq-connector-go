package paging

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
)

// estimateArrowRecordSize estimates the size of an Arrow Record without serializing it.
// This is an approximation based on the data types and number of rows.
func estimateArrowRecordSize(record arrow.Record) (uint64, error) {
	if record == nil {
		return 0, nil
	}

	numRows := record.NumRows()
	if numRows == 0 {
		return 0, nil
	}

	// Start with a base size for the record structure itself
	// This includes metadata, schema, and other overhead
	size := uint64(64) // Base overhead for record structure

	// Add size for each column
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)

		colSize, err := estimateArrowArraySize(col)
		if err != nil {
			return 0, fmt.Errorf("estimate column %d size: %w", i, err)
		}

		size += colSize
	}

	return size, nil
}

// estimateArrowArraySize estimates the size of an Arrow Array without serializing it.
// nolint:gocyclo,funlen
func estimateArrowArraySize(arr arrow.Array) (uint64, error) {
	if arr == nil {
		return 0, nil
	}

	// Base size for array structure
	size := uint64(32) // Base overhead for array structure

	// Add size for validity bitmap (null values)
	// This is approximately 1 bit per value, rounded up to bytes
	validityBitmapSize := uint64((arr.Len() + 7) / 8)

	size += validityBitmapSize

	// Get the number of non-null values
	nonNullCount := arr.Len() - arr.NullN()

	// Add size based on data type and length
	switch arr := arr.(type) {
	case *array.Boolean:
		// For boolean arrays, we need about 1 bit per value (rounded up to bytes)
		size += uint64((nonNullCount + 7) / 8)
	case *array.Int8:
		size += uint64(nonNullCount) // 1 byte per value
	case *array.Int16:
		size += uint64(nonNullCount * 2) // 2 bytes per value
	case *array.Int32:
		size += uint64(nonNullCount * 4) // 4 bytes per value
	case *array.Int64:
		size += uint64(nonNullCount * 8) // 8 bytes per value
	case *array.Uint8:
		size += uint64(nonNullCount) // 1 byte per value
	case *array.Uint16:
		size += uint64(nonNullCount * 2) // 2 bytes per value
	case *array.Uint32:
		size += uint64(nonNullCount * 4) // 4 bytes per value
	case *array.Uint64:
		size += uint64(nonNullCount * 8) // 8 bytes per value
	case *array.Float32:
		size += uint64(nonNullCount * 4) // 4 bytes per value
	case *array.Float64:
		size += uint64(nonNullCount * 8) // 8 bytes per value
	case *array.String:
		// For string arrays, we need to account for the string data and offsets
		// Offsets are int32, so 4 bytes per value plus 1
		size += uint64((arr.Len() + 1) * 4) // Offsets are needed for all positions, even nulls

		// Estimate the string data size
		// This is an approximation; we iterate through values to get actual sizes
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				size += uint64(len(arr.Value(i)))
			}
		}
	case *array.Binary:
		// For binary arrays, similar to string arrays
		size += uint64((arr.Len() + 1) * 4) // Offsets are needed for all positions, even nulls

		// Estimate the binary data size
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				size += uint64(len(arr.Value(i)))
			}
		}
	case *array.Timestamp:
		size += uint64(nonNullCount * 8) // 8 bytes per timestamp
	case *array.Date32:
		size += uint64(nonNullCount * 4) // 4 bytes per date
	case *array.Date64:
		size += uint64(nonNullCount * 8) // 8 bytes per date
	case *array.Time32:
		size += uint64(nonNullCount * 4) // 4 bytes per time
	case *array.Time64:
		size += uint64(nonNullCount * 8) // 8 bytes per time
	case *array.Decimal128:
		size += uint64(nonNullCount * 16) // 16 bytes per decimal
	case *array.Decimal256:
		size += uint64(nonNullCount * 32) // 32 bytes per decimal
	case *array.Struct:
		// For struct arrays, we need to account for each field
		for i := 0; i < arr.NumField(); i++ {
			fieldArr := arr.Field(i)

			fieldSize, err := estimateArrowArraySize(fieldArr)
			if err != nil {
				return 0, fmt.Errorf("estimate struct field %d size: %w", i, err)
			}

			size += fieldSize
		}
	case array.ListLike:
		// This case handles all list-like arrays, including Map, List, LargeList, etc.
		// For list arrays, we need to account for the offsets and the values
		size += uint64((arr.Len() + 1) * 4) // Offsets (int32) are needed for all positions, even nulls

		// For Map arrays, we need to handle keys and items separately
		if mapArr, ok := arr.(*array.Map); ok {
			// Estimate the key-value pairs size
			keyArr := mapArr.Keys()

			keySize, err := estimateArrowArraySize(keyArr)
			if err != nil {
				return 0, fmt.Errorf("estimate map keys size: %w", err)
			}

			size += keySize

			itemArr := mapArr.Items()

			itemSize, err := estimateArrowArraySize(itemArr)
			if err != nil {
				return 0, fmt.Errorf("estimate map items size: %w", err)
			}

			size += itemSize
		} else {
			// For regular list arrays, estimate the values size
			valueArr := arr.ListValues()

			valueSize, err := estimateArrowArraySize(valueArr)
			if err != nil {
				return 0, fmt.Errorf("estimate list values size: %w", err)
			}

			size += valueSize
		}
	default:
		// For other types, return an error
		return 0, fmt.Errorf("unsupported arrow array type: %T", arr)
	}

	return size, nil
}
