package parse

import (
	"fmt"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"os"
	"strconv"
)

func parseInputsToMap(rawInputs map[string]string, processedMap map[string]*structpb.Value) {
	// Iterate over the original map and copy the key-value pairs
	for key, value := range rawInputs {
		if _, err := strconv.Atoi(value); err == nil {
			intValue, _ := strconv.ParseInt(value, 10, 64)
			processedMap[key] = structpb.NewNumberValue(float64(intValue))
		} else if _, err := strconv.ParseBool(value); err == nil {
			boolValue, _ := strconv.ParseBool(value)
			processedMap[key] = structpb.NewBoolValue(boolValue)
		} else if _, err := strconv.ParseFloat(value, 64); err == nil {
			floatValue, _ := strconv.ParseFloat(value, 64)
			processedMap[key] = structpb.NewNumberValue(floatValue)
		} else {
			processedMap[key] = structpb.NewStringValue(value)
		}
	}
}

func ParseInputs(inputStr map[string]string, inputNum map[string]int64, input map[string]string) map[string]*structpb.Value {
	if inputStr == nil && inputNum == nil && input == nil {
		fmt.Println("No inputs provided, please provide inputs with either `--in`, `--in_num`, `--in_str`, or `--in_file` flags")
		os.Exit(1)
	}
	inputsProcessed := make(map[string]*structpb.Value)
	if input != nil {
		parseInputsToMap(input, inputsProcessed)
	}

	for k, v := range inputNum {
		inputsProcessed[k] = structpb.NewNumberValue(float64(v))
	}

	for k, v := range inputStr {
		inputsProcessed[k] = structpb.NewStringValue(v)
	}

	return inputsProcessed
}

func ParseOutputs(output []string) []*commonv1.OutputExpr {
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

func ParseOnlineQueryContext(useNativeSql bool, staticUnderscoreExprs bool, queryName string, tags []string) *commonv1.OnlineQueryContext {
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
	if tags != nil {
		onlineQueryContext.Tags = tags
	}
	return &onlineQueryContext
}
