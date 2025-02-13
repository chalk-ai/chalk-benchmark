package parse

import (
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"os"
	"strconv"
)

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

func ProcessInputs(inputStr map[string]string, inputNum map[string]int64, input map[string]string, chunkSize int64) []byte {
	if inputStr == nil && inputNum == nil && input == nil {
		fmt.Println("No inputs provided, please provide inputs with either `--in`, `--in_num`, `--in_str`, or `--in_file` flags")
		os.Exit(1)
	}
	arrowRecord := parseInputsToRecord(input, inputNum, inputStr, chunkSize)
	defer arrowRecord.Release()
	inputBytes, err := recordToBytes(arrowRecord)
	if err != nil {
		fmt.Println("Failed to convert inputs to Arrow record with error: ", err)
		os.Exit(1)
	}
	return inputBytes
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

func ProcessOnlineQueryContext(useNativeSql bool, staticUnderscoreExprs bool, queryName string, queryNameVersion string, tags []string) *commonv1.OnlineQueryContext {
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
	return &onlineQueryContext
}
