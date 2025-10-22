package parse

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// parseInputsToRecord creates an arrow record from input maps
func parseInputsToRecord(rawInputs map[string]string, inputNum map[string]int64, inputStr map[string]string, chunkSize int64) arrow.Record {
	// Iterate over the original map and copy the key-value pairs
	totalNumInputs := len(rawInputs) + len(inputNum) + len(inputStr)
	schema := make([]arrow.Field, totalNumInputs)
	arrays := make([]arrow.Array, totalNumInputs)
	for j := 0; j < int(chunkSize); j++ {
		i := 0
		for key, value := range rawInputs {
			if _, err := strconv.Atoi(value); err == nil {
				schema[i] = arrow.Field{Name: key, Type: arrow.PrimitiveTypes.Int64}
				b := array.NewInt64Builder(memory.DefaultAllocator)
				defer b.Release()
				_ = b.AppendValueFromString(value) // cannot be nil
				arrays[i] = b.NewInt64Array()
			} else if _, err := strconv.ParseBool(value); err == nil {
				schema[i] = arrow.Field{Name: key, Type: arrow.FixedWidthTypes.Boolean}
				b := array.NewBooleanBuilder(memory.DefaultAllocator)
				defer b.Release()
				_ = b.AppendValueFromString(value) // cannot be nil
				arrays[i] = b.NewBooleanArray()
			} else if _, err := strconv.ParseFloat(value, 64); err == nil {
				schema[i] = arrow.Field{Name: key, Type: arrow.PrimitiveTypes.Float64}
				b := array.NewFloat64Builder(memory.DefaultAllocator)
				defer b.Release()
				_ = b.AppendValueFromString(value) // cannot be nil
				arrays[i] = b.NewFloat64Array()
			} else {
				schema[i] = arrow.Field{Name: key, Type: arrow.BinaryTypes.LargeString}
				b := array.NewLargeStringBuilder(memory.DefaultAllocator)
				defer b.Release()
				b.AppendString(value) // cannot be nil
				arrays[i] = b.NewLargeStringArray()
			}
			i += 1
		}
		for key, value := range inputNum {
			schema[i] = arrow.Field{Name: key, Type: arrow.PrimitiveTypes.Int64}
			b := array.NewInt64Builder(memory.DefaultAllocator)
			defer b.Release()
			b.Append(value) // cannot be nil
			arrays[i] = b.NewInt64Array()
			i += 1
		}
		for key, value := range inputStr {
			schema[i] = arrow.Field{Name: key, Type: arrow.BinaryTypes.LargeString}
			b := array.NewLargeStringBuilder(memory.DefaultAllocator)
			defer b.Release()
			b.Append(value) // cannot be nil
			arrays[i] = b.NewLargeStringArray()
			i += 1
		}
	}

	return array.NewRecord(arrow.NewSchema(schema, nil), arrays, 1)
}

// parsePkeyToRecord creates an arrow record from a single pkey in key=value format
func parsePkeyToRecord(pkey string) (arrow.Record, error) {
	// Split pkey into key and value parts
	parts := strings.SplitN(pkey, "=", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid pkey format: %s. Expected key=value format", pkey)
	}

	key := parts[0]
	value := parts[1]

	// Create a schema with a single field
	var field arrow.Field
	var builder array.Builder

	// Try to determine the type of the value
	if _, err := strconv.Atoi(value); err == nil {
		field = arrow.Field{Name: key, Type: arrow.PrimitiveTypes.Int64}
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		_ = b.AppendValueFromString(value)
		builder = b
	} else if _, err := strconv.ParseBool(value); err == nil {
		field = arrow.Field{Name: key, Type: arrow.FixedWidthTypes.Boolean}
		b := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer b.Release()
		_ = b.AppendValueFromString(value)
		builder = b
	} else if _, err := strconv.ParseFloat(value, 64); err == nil {
		field = arrow.Field{Name: key, Type: arrow.PrimitiveTypes.Float64}
		b := array.NewFloat64Builder(memory.DefaultAllocator)
		defer b.Release()
		_ = b.AppendValueFromString(value)
		builder = b
	} else {
		field = arrow.Field{Name: key, Type: arrow.BinaryTypes.LargeString}
		b := array.NewLargeStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendString(value)
		builder = b
	}

	schema := arrow.NewSchema([]arrow.Field{field}, nil)

	// Create the record with a single row
	record := array.NewRecord(schema, []arrow.Array{builder.NewArray()}, 1)

	return record, nil
}

// parseInputArrays detects and parses array notation in input values
// It returns a map of field names to array values
func parseInputArrays(rawInputs map[string]string) map[string][]string {
	arrays := make(map[string][]string)

	for key, value := range rawInputs {
		// Check if value starts with [ and ends with ]
		if len(value) >= 2 && value[0] == '[' && value[len(value)-1] == ']' {
			// Extract content between brackets
			content := value[1 : len(value)-1]
			// Split by comma, handle whitespace
			elements := strings.Split(content, ",")
			for i, elem := range elements {
				elements[i] = strings.TrimSpace(elem)
			}
			arrays[key] = elements
		}
	}

	return arrays
}

// parseNumberArrays detects and parses array notation in numeric input values
func parseNumberArrays(inputNum map[string]int64) map[string][]int64 {
	// Not directly applicable as inputNum is already parsed as int64,
	// but we could detect repeated keys in the original string representation
	// For simplicity, we'll handle this through a different mechanism
	return make(map[string][]int64)
}

// ProcessInputs handles standard inputs and now supports array notation
func ProcessInputs(inputStr map[string]string, inputNum map[string]int64, input map[string]string, chunkSize int64) [][]byte {
	if inputStr == nil && inputNum == nil && input == nil {
		fmt.Println("No inputs provided, please provide inputs with either `--in`, `--in_num`, `--in_str`, or `--in_file` flags")
		os.Exit(1)
	}

	// Check for array notation in inputs
	arrayInputs := parseInputArrays(input)

	// If no array inputs, just return the single record as before
	if len(arrayInputs) == 0 {
		arrowRecord := parseInputsToRecord(input, inputNum, inputStr, chunkSize)
		defer arrowRecord.Release()
		inputBytes, err := recordToBytes(arrowRecord)
		if err != nil {
			fmt.Println("Failed to convert inputs to Arrow record with error: ", err)
			os.Exit(1)
		}
		slog.Debug(fmt.Sprintf("Input\n: %x", inputBytes))
		return [][]byte{inputBytes}
	}

	// Handle array inputs by creating multiple records
	var bytesList [][]byte

	// Get an arbitrary key from the arrayInputs to determine how many records to create
	var arrayLength int
	for _, values := range arrayInputs {
		arrayLength = len(values)
		break
	}

	// Validate all arrays have the same length
	for key, values := range arrayInputs {
		if len(values) != arrayLength {
			fmt.Printf("Array length mismatch for key '%s'. Expected %d elements, got %d\n", key, arrayLength, len(values))
			os.Exit(1)
		}
	}

	// Create a record for each element in the arrays
	for i := 0; i < arrayLength; i++ {
		// Create a copy of the original inputs
		inputCopy := make(map[string]string)
		for k, v := range input {
			// Skip array inputs, we'll handle them separately
			if _, isArray := arrayInputs[k]; !isArray {
				inputCopy[k] = v
			}
		}

		// Add the i-th element from each array input
		for k, values := range arrayInputs {
			inputCopy[k] = values[i]
		}

		// Create the record from the modified inputs
		arrowRecord := parseInputsToRecord(inputCopy, inputNum, inputStr, chunkSize)
		defer arrowRecord.Release()

		inputBytes, err := recordToBytes(arrowRecord)
		if err != nil {
			fmt.Printf("Failed to convert record %d to Arrow bytes: %v\n", i, err)
			os.Exit(1)
		}
		slog.Debug(fmt.Sprintf("Input\n: %x", inputBytes))

		bytesList = append(bytesList, inputBytes)
	}

	return bytesList
}

func ProcessOutputs(output []string) []*commonv1.OutputExpr {
	outputsProcessed := make([]*commonv1.OutputExpr, len(output))
	for i := 0; i < len(outputsProcessed); i++ {
		outputsProcessed[i] = &commonv1.OutputExpr{
			Expr: &commonv1.OutputExpr_FeatureFqn{
				FeatureFqn: output[i],
			},
		}
	}
	return outputsProcessed
}

func ProcessOnlineQueryContext(useNativeSql bool, staticUnderscoreExprs bool, queryName string, queryNameVersion string, tags []string, storePlanStages bool) *commonv1.OnlineQueryContext {
	onlineQueryContext := commonv1.OnlineQueryContext{Options: map[string]*structpb.Value{}}
	if useNativeSql {
		onlineQueryContext.Options["use_native_sql_operators"] = structpb.NewBoolValue(useNativeSql)
	}
	if staticUnderscoreExprs {
		onlineQueryContext.Options["static_underscore_expressions"] = structpb.NewBoolValue(staticUnderscoreExprs)
	}
	if queryName != "" {
		onlineQueryContext.QueryName = &queryName
	}
	if queryNameVersion != "" {
		onlineQueryContext.QueryNameVersion = &queryNameVersion
	}
	if tags != nil {
		onlineQueryContext.Tags = tags
	}
	if storePlanStages {
		onlineQueryContext.Options["store_plan_stages"] = structpb.NewBoolValue(storePlanStages)
	}
	slog.Debug("OnlineQueryContext", "context", onlineQueryContext)
	return &onlineQueryContext
}
