package cmd

import (
	"fmt"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"os"
)

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
