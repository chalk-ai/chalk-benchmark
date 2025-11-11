package parse

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// ProcessJSONInputs reads a JSON file containing a list of lists of objects (batches of queries)
// and converts each batch into an Arrow record with multiple rows, then encodes it to bytes
// Format: [[{row1}, {row2}], [{row3}, {row4}, {row5}], ...]
func ProcessJSONInputs(jsonFilePath string) [][]byte {
	// Read the JSON file
	fileContent, err := os.ReadFile(jsonFilePath)
	if err != nil {
		fmt.Printf("Failed to read JSON file '%s': %v\n", jsonFilePath, err)
		os.Exit(1)
	}

	// First, try to parse as list of lists (batched format)
	var batchedInputList [][]map[string]interface{}
	if err := json.Unmarshal(fileContent, &batchedInputList); err == nil {
		// Successfully parsed as list of lists
		if len(batchedInputList) == 0 {
			fmt.Println("JSON file contains no input batches")
			os.Exit(1)
		}

		slog.Debug(fmt.Sprintf("Loaded %d batches from JSON file", len(batchedInputList)))

		// Convert each batch to Arrow record bytes
		var bytesList [][]byte
		for batchIdx, batch := range batchedInputList {
			if len(batch) == 0 {
				fmt.Printf("Batch %d is empty\n", batchIdx)
				os.Exit(1)
			}

			record, err := jsonBatchToRecord(batch)
			if err != nil {
				fmt.Printf("Failed to convert batch %d to Arrow record: %v\n", batchIdx, err)
				os.Exit(1)
			}
			defer record.Release()

			inputBytes, err := recordToBytes(record)
			if err != nil {
				fmt.Printf("Failed to encode batch %d to bytes: %v\n", batchIdx, err)
				os.Exit(1)
			}

			bytesList = append(bytesList, inputBytes)
			slog.Debug(fmt.Sprintf("Encoded batch %d with %d rows (%d bytes)", batchIdx, len(batch), len(inputBytes)))
		}

		return bytesList
	}

	// Fallback: try to parse as flat list of objects (each object is a single-row batch)
	var flatInputList []map[string]interface{}
	if err := json.Unmarshal(fileContent, &flatInputList); err != nil {
		fmt.Printf("Failed to parse JSON file '%s': %v\n", jsonFilePath, err)
		fmt.Println("Expected format: [[{row1}, {row2}], [{row3}]] or [{row1}, {row2}]")
		os.Exit(1)
	}

	if len(flatInputList) == 0 {
		fmt.Println("JSON file contains no input records")
		os.Exit(1)
	}

	slog.Debug(fmt.Sprintf("Loaded %d input records from JSON file (flat format)", len(flatInputList)))

	// Convert each input object to Arrow record bytes (single row per batch)
	var bytesList [][]byte
	for i, inputObj := range flatInputList {
		record, err := jsonBatchToRecord([]map[string]interface{}{inputObj})
		if err != nil {
			fmt.Printf("Failed to convert input %d to Arrow record: %v\n", i, err)
			os.Exit(1)
		}
		defer record.Release()

		inputBytes, err := recordToBytes(record)
		if err != nil {
			fmt.Printf("Failed to encode record %d to bytes: %v\n", i, err)
			os.Exit(1)
		}

		bytesList = append(bytesList, inputBytes)
		slog.Debug(fmt.Sprintf("Encoded input %d (%d bytes)", i, len(inputBytes)))
	}

	return bytesList
}

// jsonBatchToRecord converts a batch of JSON objects into a single Arrow record with multiple rows
func jsonBatchToRecord(batch []map[string]interface{}) (arrow.Record, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	// Use the first object to determine the schema
	firstObj := batch[0]
	if len(firstObj) == 0 {
		return nil, fmt.Errorf("empty first object in batch")
	}

	// Build schema from first object
	var fieldNames []string
	schemaFields := make([]arrow.Field, 0, len(firstObj))
	for key := range firstObj {
		fieldNames = append(fieldNames, key)
	}
	// Sort field names for consistent ordering
	// (Go map iteration order is non-deterministic)
	for i := 0; i < len(fieldNames)-1; i++ {
		for j := i + 1; j < len(fieldNames); j++ {
			if fieldNames[i] > fieldNames[j] {
				fieldNames[i], fieldNames[j] = fieldNames[j], fieldNames[i]
			}
		}
	}

	// Create builders for each field
	builders := make(map[string]array.Builder)
	for _, fieldName := range fieldNames {
		value := firstObj[fieldName]
		field, builder := createFieldAndBuilder(fieldName, value)
		schemaFields = append(schemaFields, field)
		builders[fieldName] = builder
	}

	// Append values from all rows to the builders
	for rowIdx, obj := range batch {
		for _, fieldName := range fieldNames {
			value, exists := obj[fieldName]
			if !exists {
				return nil, fmt.Errorf("field '%s' missing in row %d", fieldName, rowIdx)
			}
			if err := appendValueToBuilder(builders[fieldName], value); err != nil {
				return nil, fmt.Errorf("failed to append value for field '%s' in row %d: %v", fieldName, rowIdx, err)
			}
		}
	}

	// Build arrays from builders
	arrays := make([]arrow.Array, len(fieldNames))
	for i, fieldName := range fieldNames {
		arrays[i] = builders[fieldName].NewArray()
		builders[fieldName].Release()
	}

	return array.NewRecord(arrow.NewSchema(schemaFields, nil), arrays, int64(len(batch))), nil
}

// jsonObjectToRecord converts a JSON object (map[string]interface{}) to an Arrow record
func jsonObjectToRecord(inputObj map[string]interface{}) (arrow.Record, error) {
	if len(inputObj) == 0 {
		return nil, fmt.Errorf("empty input object")
	}

	// Create schema and arrays for each field
	schema := make([]arrow.Field, 0, len(inputObj))
	arrays := make([]arrow.Array, 0, len(inputObj))

	// Process each field in the input object
	for key, value := range inputObj {
		field, arr, err := valueToArrowFieldAndArray(key, value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field '%s': %v", key, err)
		}
		schema = append(schema, field)
		arrays = append(arrays, arr)
	}

	return array.NewRecord(arrow.NewSchema(schema, nil), arrays, 1), nil
}

// createFieldAndBuilder creates an Arrow field and builder for a given value type
func createFieldAndBuilder(fieldName string, value interface{}) (arrow.Field, array.Builder) {
	switch v := value.(type) {
	case float64:
		if v == float64(int64(v)) {
			field := arrow.Field{Name: fieldName, Type: arrow.PrimitiveTypes.Int64}
			builder := array.NewInt64Builder(memory.DefaultAllocator)
			return field, builder
		}
		field := arrow.Field{Name: fieldName, Type: arrow.PrimitiveTypes.Float64}
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		return field, builder
	case string:
		field := arrow.Field{Name: fieldName, Type: arrow.BinaryTypes.LargeString}
		builder := array.NewLargeStringBuilder(memory.DefaultAllocator)
		return field, builder
	case bool:
		field := arrow.Field{Name: fieldName, Type: arrow.FixedWidthTypes.Boolean}
		builder := array.NewBooleanBuilder(memory.DefaultAllocator)
		return field, builder
	case []interface{}:
		// For arrays, determine element type from first element
		if len(v) > 0 {
			switch v[0].(type) {
			case float64:
				allIntegers := true
				for _, elem := range v {
					if num, ok := elem.(float64); ok {
						if num != float64(int64(num)) {
							allIntegers = false
							break
						}
					}
				}
				if allIntegers {
					field := arrow.Field{Name: fieldName, Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}
					builder := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
					return field, builder
				}
				field := arrow.Field{Name: fieldName, Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)}
				builder := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Float64)
				return field, builder
			case string:
				field := arrow.Field{Name: fieldName, Type: arrow.ListOf(arrow.BinaryTypes.LargeString)}
				builder := array.NewListBuilder(memory.DefaultAllocator, arrow.BinaryTypes.LargeString)
				return field, builder
			case bool:
				field := arrow.Field{Name: fieldName, Type: arrow.ListOf(arrow.FixedWidthTypes.Boolean)}
				builder := array.NewListBuilder(memory.DefaultAllocator, arrow.FixedWidthTypes.Boolean)
				return field, builder
			}
		}
	}
	// Default to string
	field := arrow.Field{Name: fieldName, Type: arrow.BinaryTypes.LargeString}
	builder := array.NewLargeStringBuilder(memory.DefaultAllocator)
	return field, builder
}

// appendValueToBuilder appends a value to an Arrow builder
func appendValueToBuilder(builder array.Builder, value interface{}) error {
	switch b := builder.(type) {
	case *array.Int64Builder:
		if v, ok := value.(float64); ok {
			b.Append(int64(v))
			return nil
		}
		return fmt.Errorf("expected number for int64 field")
	case *array.Float64Builder:
		if v, ok := value.(float64); ok {
			b.Append(v)
			return nil
		}
		return fmt.Errorf("expected number for float64 field")
	case *array.LargeStringBuilder:
		if v, ok := value.(string); ok {
			b.Append(v)
			return nil
		}
		return fmt.Errorf("expected string for string field")
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
			return nil
		}
		return fmt.Errorf("expected boolean for boolean field")
	case *array.ListBuilder:
		if v, ok := value.([]interface{}); ok {
			b.Append(true)
			vb := b.ValueBuilder()
			switch valueBuilder := vb.(type) {
			case *array.Int64Builder:
				for _, elem := range v {
					if num, ok := elem.(float64); ok {
						valueBuilder.Append(int64(num))
					} else {
						return fmt.Errorf("expected number in int array")
					}
				}
			case *array.Float64Builder:
				for _, elem := range v {
					if num, ok := elem.(float64); ok {
						valueBuilder.Append(num)
					} else {
						return fmt.Errorf("expected number in float array")
					}
				}
			case *array.LargeStringBuilder:
				for _, elem := range v {
					if str, ok := elem.(string); ok {
						valueBuilder.Append(str)
					} else {
						return fmt.Errorf("expected string in string array")
					}
				}
			case *array.BooleanBuilder:
				for _, elem := range v {
					if boolVal, ok := elem.(bool); ok {
						valueBuilder.Append(boolVal)
					} else {
						return fmt.Errorf("expected boolean in boolean array")
					}
				}
			}
			return nil
		}
		return fmt.Errorf("expected array for list field")
	default:
		return fmt.Errorf("unsupported builder type")
	}
}

// valueToArrowFieldAndArray converts a JSON value to an Arrow field and array
func valueToArrowFieldAndArray(key string, value interface{}) (arrow.Field, arrow.Array, error) {
	switch v := value.(type) {
	case float64:
		// JSON numbers are always float64
		// Try to determine if it's an integer
		if v == float64(int64(v)) {
			field := arrow.Field{Name: key, Type: arrow.PrimitiveTypes.Int64}
			b := array.NewInt64Builder(memory.DefaultAllocator)
			defer b.Release()
			b.Append(int64(v))
			return field, b.NewInt64Array(), nil
		} else {
			field := arrow.Field{Name: key, Type: arrow.PrimitiveTypes.Float64}
			b := array.NewFloat64Builder(memory.DefaultAllocator)
			defer b.Release()
			b.Append(v)
			return field, b.NewFloat64Array(), nil
		}

	case string:
		field := arrow.Field{Name: key, Type: arrow.BinaryTypes.LargeString}
		b := array.NewLargeStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(v)
		return field, b.NewLargeStringArray(), nil

	case bool:
		field := arrow.Field{Name: key, Type: arrow.FixedWidthTypes.Boolean}
		b := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(v)
		return field, b.NewBooleanArray(), nil

	case []interface{}:
		// Handle arrays - need to determine the element type
		if len(v) == 0 {
			return arrow.Field{}, nil, fmt.Errorf("empty arrays are not supported")
		}

		// Check the type of the first element to determine array type
		switch v[0].(type) {
		case float64:
			// Check if all elements are integers
			allIntegers := true
			for _, elem := range v {
				if num, ok := elem.(float64); ok {
					if num != float64(int64(num)) {
						allIntegers = false
						break
					}
				} else {
					return arrow.Field{}, nil, fmt.Errorf("mixed types in array for field '%s'", key)
				}
			}

			if allIntegers {
				field := arrow.Field{Name: key, Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}
				b := array.NewInt64Builder(memory.DefaultAllocator)
				defer b.Release()
				for _, elem := range v {
					b.Append(int64(elem.(float64)))
				}
				lb := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
				defer lb.Release()
				vb := lb.ValueBuilder().(*array.Int64Builder)
				lb.Append(true)
				for _, elem := range v {
					vb.Append(int64(elem.(float64)))
				}
				return field, lb.NewListArray(), nil
			} else {
				field := arrow.Field{Name: key, Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)}
				lb := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Float64)
				defer lb.Release()
				vb := lb.ValueBuilder().(*array.Float64Builder)
				lb.Append(true)
				for _, elem := range v {
					vb.Append(elem.(float64))
				}
				return field, lb.NewListArray(), nil
			}

		case string:
			field := arrow.Field{Name: key, Type: arrow.ListOf(arrow.BinaryTypes.LargeString)}
			lb := array.NewListBuilder(memory.DefaultAllocator, arrow.BinaryTypes.LargeString)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.LargeStringBuilder)
			lb.Append(true)
			for _, elem := range v {
				if str, ok := elem.(string); ok {
					vb.Append(str)
				} else {
					return arrow.Field{}, nil, fmt.Errorf("mixed types in string array for field '%s'", key)
				}
			}
			return field, lb.NewListArray(), nil

		case bool:
			field := arrow.Field{Name: key, Type: arrow.ListOf(arrow.FixedWidthTypes.Boolean)}
			lb := array.NewListBuilder(memory.DefaultAllocator, arrow.FixedWidthTypes.Boolean)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.BooleanBuilder)
			lb.Append(true)
			for _, elem := range v {
				if b, ok := elem.(bool); ok {
					vb.Append(b)
				} else {
					return arrow.Field{}, nil, fmt.Errorf("mixed types in boolean array for field '%s'", key)
				}
			}
			return field, lb.NewListArray(), nil

		default:
			return arrow.Field{}, nil, fmt.Errorf("unsupported array element type for field '%s'", key)
		}

	case nil:
		return arrow.Field{}, nil, fmt.Errorf("null values are not supported for field '%s'", key)

	default:
		return arrow.Field{}, nil, fmt.Errorf("unsupported type %T for field '%s'", value, key)
	}
}

// Alternative: ProcessJSONInputsFromString allows parsing from a string (useful for testing)
func ProcessJSONInputsFromString(jsonStr string, inputNum map[string]int64, inputStr map[string]string) [][]byte {
	var inputList []map[string]string
	if err := json.Unmarshal([]byte(jsonStr), &inputList); err != nil {
		fmt.Printf("Failed to parse JSON string: %v\n", err)
		os.Exit(1)
	}

	var bytesList [][]byte
	for _, inputObj := range inputList {
		// Merge with inputNum and inputStr if provided
		fullInput := make(map[string]string)
		for k, v := range inputObj {
			fullInput[k] = v
		}
		for k, v := range inputNum {
			fullInput[k] = strconv.FormatInt(v, 10)
		}
		for k, v := range inputStr {
			fullInput[k] = v
		}

		record := parseInputsToRecord(fullInput, nil, nil, 1)
		defer record.Release()

		inputBytes, err := recordToBytes(record)
		if err != nil {
			fmt.Printf("Failed to encode record: %v\n", err)
			os.Exit(1)
		}
		bytesList = append(bytesList, inputBytes)
	}

	return bytesList
}
