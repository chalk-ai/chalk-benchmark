package main

import (
	"github.com/chalk-ai/chalk-go"
	"os"
	"strconv"
	"strings"

	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"
	_ "github.com/goccy/go-json"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const InstallPath = "/update/this/path"
const RPS = 10
const DurationSeconds = 120
const Host = "insert-your-host-here.chalk.ai:443"

var STRING_INPUTS = []string{"featureset.id"}
var STRING_OUTPUTS = []string{"featureset.feature_name_2"}

func runBenchmark(host string, rps uint, duration_seconds uint, inputs map[string]*structpb.Value, outputs []*commonv1.OutputExpr) {
	client, _ := chalk.NewClient(&chalk.ClientConfig{
		UseGrpc: true,
	})
	tokenResult, err := client.GetToken()
	ExitIfError(err, "Failed to get token")

	total_requests := uint(rps * duration_seconds)
	report, err := runner.Run(
		strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
		host,
		runner.WithRPS(rps),
		runner.WithTotalRequests(total_requests),
		runner.WithAsync(true),
		runner.WithConnections(16),
		runner.WithMetadata(map[string]string{
			"authorization":           tokenResult.AccessToken,
			"x-chalk-env-id":          tokenResult.PrimaryEnvironment,
			"x-chalk-deployment-type": "engine-grpc",
		}),
		runner.WithProtoFile("./chalk/engine/v1/query_server.proto", []string{InstallPath + "chalk-benchmark/benchmark/protos/"}),
		runner.WithSkipTLSVerify(true),
		runner.WithConcurrency(16),
		runner.WithBinaryData(OrFatal(proto.Marshal(&commonv1.OnlineQueryRequest{
			Inputs:  inputs,
			Outputs: outputs,
			Context: &commonv1.OnlineQueryContext{Options: map[string]*structpb.Value{
				"use_native_sql_operators": structpb.NewBoolValue(true),
			}},
		}))("failed to marshal request")),
	)
	ExitIfError(err, "Failed to run query")

	p := printer.ReportPrinter{
		Out:    os.Stdout,
		Report: report,
	}

	ExitIfError(p.Print("summary"), "failed to print report")

	outputFile, err := os.OpenFile("report.html", os.O_RDWR, 0666)
	ExitIfError(err, "failed to open report")
	htmlSaver := printer.ReportPrinter{
		Out:    outputFile,
		Report: report,
	}
	ExitIfError(htmlSaver.Print("html"), "failed to save report")

}

func main() {
	inputs := make(map[string]*structpb.Value)
	for i := 0; i < len(STRING_INPUTS); i++ {
		inputs[STRING_INPUTS[i]] = structpb.NewStringValue(strconv.Itoa(i))
	}

	outputs := make([]*commonv1.OutputExpr, len(STRING_OUTPUTS))
	for i := 0; i < len(outputs); i++ {
		outputs[i] = &commonv1.OutputExpr{
			Expr: &commonv1.OutputExpr_FeatureFqn{
				FeatureFqn: STRING_OUTPUTS[i],
			},
		}
	}

	runBenchmark(Host, RPS, DurationSeconds, inputs, outputs)
}
