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
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type BenchmarkFunction struct {
	F        func() (*runner.Report, error)
	Duration time.Duration
}

func BenchmarkPing(grpcHost string, authHeaders []runner.Option) []BenchmarkFunction {
	pingRequest, err := proto.Marshal(&enginev1.PingRequest{Num: 10})
	if err != nil {
		fmt.Printf("Failed to marshal ping request with err: %s\n", err)
		os.Exit(1)
	}
	return []BenchmarkFunction{{
		F: func() (*runner.Report, error) {
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
		},
		Duration: time.Duration(0),
	}}
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
	scheduleFile string,
) []BenchmarkFunction {
	// total requests calculated from duration and RPS
	queryOptions := QueryRateOptions(rps, benchmarkDuration, rampDuration, 0, scheduleFile)

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
	var bfs []BenchmarkFunction
	for _, queryOption := range queryOptions {
		bfs = append(bfs, BenchmarkFunction{
			F: func() (*runner.Report, error) {
				return runner.Run(
					strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
					grpcHost,
					slices.Concat(
						globalHeaders,
						queryOption.Options,
						[]runner.Option{
							runner.WithBinaryData(binaryData),
						},
					)...,
				)
			},
			Duration: queryOption.Duration,
		})
	}
	return bfs
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
	scheduleFile string,
) []BenchmarkFunction {
	// total requests calculated from duration and RPS
	queryOptions := QueryRateOptions(rps, benchmarkDuration, rampDuration, uint(len(records)), scheduleFile)

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
	var bfs []BenchmarkFunction
	for _, queryOption := range queryOptions {
		bfs = append(bfs, BenchmarkFunction{
			F: func() (*runner.Report, error) {
				return runner.Run(
					strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
					grpcHost,
					slices.Concat(
						globalHeaders,
						queryOption.Options,
						[]runner.Option{
							runner.WithBinaryDataFunc(binaryDataFunc),
						},
					)...,
				)
			},
			Duration: benchmarkDuration,
		})
	}
	return bfs
}

func BenchmarkUploadFeatures(
	grpcHost string,
	globalHeaders []runner.Option,
	uploadFeaturesFile string,
	rps uint,
	benchmarkDuration time.Duration,
	rampDuration time.Duration,
	scheduleFile string,
) []BenchmarkFunction {
	file, err := os.Open(uploadFeaturesFile)
	queryOptions := QueryRateOptions(rps, benchmarkDuration, rampDuration, 0, scheduleFile)
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

	var bfs []BenchmarkFunction
	for _, queryOption := range queryOptions {
		bfs = append(bfs, BenchmarkFunction{
			F: func() (*runner.Report, error) {
				return runner.Run(
					strings.TrimPrefix(enginev1connect.QueryServiceUploadFeaturesBulkProcedure, "/"),
					grpcHost,
					slices.Concat(
						globalHeaders,
						queryOption.Options,
						[]runner.Option{
							runner.WithBinaryData(binaryData),
						},
					)...,
				)
			},
			Duration: queryOption.Duration,
		})
	}
	return bfs
}

func RunBenchmarks(bfs []BenchmarkFunction) *runner.Report {
	var reports []*runner.Report
	totalRunTime := time.Duration(0)
	for _, bf := range bfs {
		totalRunTime += bf.Duration
		var wg sync.WaitGroup

		if !noProgress && !test {
			wg.Add(1)
			go pbar(bf.Duration, rampDuration, &wg)
		}

		result, err := bf.F()

		if !noProgress && !test {
			wg.Wait()
		}
		if err != nil {
			fmt.Printf("Failed to run request with err: %s\n", err)
			os.Exit(1)
		}
		reports = append(reports, result)
	}
	return mergeReports(reports)
}

func mergeReports(reports []*runner.Report) *runner.Report {

	if len(reports) == 1 {
		return reports[0]
	}
	mergedReport := reports[0]

	for _, report := range reports[1:] {
		mergedReport.Details = slices.Concat(mergedReport.Details, report.Details)
		mergedReport.Count += report.Count
		mergedReport.Slowest = max(mergedReport.Slowest, report.Slowest)
		mergedReport.Fastest = min(mergedReport.Fastest, report.Fastest)
		maps.Copy(mergedReport.ErrorDist, report.ErrorDist)
		maps.Copy(mergedReport.StatusCodeDist, report.StatusCodeDist)
		mergedReport.Average = time.Duration(
			(float64(mergedReport.Average)*float64(mergedReport.Count) + float64(report.Average)*float64(report.Count)) /
				float64(mergedReport.Count+report.Count),
		)
	}
	okLats := make([]float64, 0)
	for _, d := range mergedReport.Details {
		okLats = append(okLats, d.Latency.Seconds())
	}
	mergedReport.LatencyDistribution = runner.Latencies(okLats)
	idx := slices.IndexFunc(mergedReport.LatencyDistribution, func(l runner.LatencyDistribution) bool {
		return l.Percentage == 99
	})
	var p99 float64
	if idx == -1 {
		p99 = mergedReport.Slowest.Seconds()
	} else {
		p99 = mergedReport.LatencyDistribution[idx].Latency.Seconds()
	}
	mergedReport.Histogram = runner.Histogram(okLats, mergedReport.Slowest.Seconds(), mergedReport.Fastest.Seconds(), p99)
	return mergedReport
}
