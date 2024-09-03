package cmd

import (
	"fmt"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	enginev1 "github.com/chalk-ai/chalk-go/gen/chalk/engine/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"
	"github.com/chalk-ai/ghz/runner"
	"github.com/jhump/protoreflect/desc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"io"
	"os"
	"slices"
	"strings"
	"time"
)

type BenchmarkFunction func() (*runner.Report, error)

func BenchmarkPing(grpcHost string, authHeaders []runner.Option) BenchmarkFunction {
	pingRequest, err := proto.Marshal(&enginev1.PingRequest{Num: 10})
	if err != nil {
		fmt.Printf("Failed to marshal ping request with err: %s\n", err)
		os.Exit(1)
	}
	return func() (*runner.Report, error) {
		return runner.Run(
			strings.TrimPrefix(enginev1connect.QueryServicePingProcedure, "/"),
			grpcHost,
			slices.Concat(
				authHeaders,
				[]runner.Option{
					runner.WithRPS(1),
					runner.WithTotalRequests(1),
					runner.WithBinaryData(pingRequest),
				},
			)...,
		)
	}
}

func BenchmarkQuery(
	grpcHost string,
	globalHeaders []runner.Option,
	queryInputs map[string]*structpb.Value,
	queryOutputs []*commonv1.OutputExpr,
	onlineQueryContext *commonv1.OnlineQueryContext,
	rps uint,
	benchmarkDuration time.Duration,
	rampDuration time.Duration,
) BenchmarkFunction {
	// total requests calculated from duration and RPS
	queryOptions := QueryRateOptions(rps, benchmarkDuration, rampDuration, 0)

	oqr := commonv1.OnlineQueryRequest{
		Inputs:  queryInputs,
		Outputs: queryOutputs,
		Context: onlineQueryContext,
	}

	binaryData, err := proto.Marshal(&oqr)

	if err != nil {
		fmt.Printf("Failed to marshal online query request with inputs: '%v', outputs: '%v', and context '%v'\n", queryInputs, queryOutputs, onlineQueryContext)
		os.Exit(1)
	}

	return func() (*runner.Report, error) {
		return runner.Run(
			strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
			grpcHost,
			slices.Concat(
				globalHeaders,
				queryOptions,
				[]runner.Option{
					runner.WithBinaryData(binaryData),
				},
			)...,
		)
	}
}

func BenchmarkQueryFromFile(
	grpcHost string,
	globalHeaders []runner.Option,
	records []Record,
	outputs []*commonv1.OutputExpr,
	onlineQueryContext *commonv1.OnlineQueryContext,
	rps uint,
	benchmarkDuration time.Duration,
	rampDuration time.Duration,
) BenchmarkFunction {
	// total requests calculated from duration and RPS
	queryOptions := QueryRateOptions(rps, benchmarkDuration, rampDuration, uint(len(records)))

	// binaryData, err := proto.Marshal(&onlineQueryContext)
	binaryDataFunc := func(mtd *desc.MethodDescriptor, cd *runner.CallData) []byte {
		request := records[cd.RequestNumber%int64(len(records))]
		oqr := commonv1.OnlineQueryRequest{
			Inputs:  request,
			Outputs: outputs,
			Context: onlineQueryContext,
		}
		value, err := proto.Marshal(
			&oqr,
		)
		if err != nil {
			fmt.Printf("Failed to marshal online query request with inputs: '%v', outputs: '%v', and context '%v'\n", request, outputs, onlineQueryContext)
			os.Exit(1)
		}
		return value
	}

	return func() (*runner.Report, error) {
		return runner.Run(
			strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
			grpcHost,
			slices.Concat(
				globalHeaders,
				queryOptions,
				[]runner.Option{
					runner.WithBinaryDataFunc(binaryDataFunc),
				},
			)...,
		)
	}
}

func BenchmarkUploadFeatures(
	grpcHost string,
	globalHeaders []runner.Option,
	uploadFeaturesFile string,
	rps uint,
	benchmarkDuration time.Duration,
	rampDuration time.Duration,
) BenchmarkFunction {
	file, err := os.Open(uploadFeaturesFile)
	queryOptions := QueryRateOptions(rps, benchmarkDuration, rampDuration, 0)
	if err != nil {
		fmt.Printf("Failed to open file with err: %s\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Read the entire file
	data, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("Failed to read file with err: %s\n", err)
		os.Exit(1)
	}
	request := commonv1.UploadFeaturesBulkRequest{
		InputsFeather: data,
		BodyType:      commonv1.FeatherBodyType_FEATHER_BODY_TYPE_RECORD_BATCHES,
	}
	binaryData, err := proto.Marshal(&request)
	if err != nil {
		fmt.Printf("Failed to marshal upload features request with err: %s\n", err)
		os.Exit(1)
	}
	return func() (*runner.Report, error) {
		return runner.Run(
			strings.TrimPrefix(enginev1connect.QueryServiceUploadFeaturesBulkProcedure, "/"),
			grpcHost,
			slices.Concat(
				globalHeaders,
				queryOptions,
				[]runner.Option{
					runner.WithBinaryData(binaryData),
				},
			)...,
		)
	}
}
