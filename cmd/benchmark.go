package cmd

import (
	"fmt"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	enginev1 "github.com/chalk-ai/chalk-go/gen/chalk/engine/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"
	"github.com/chalk-ai/ghz/runner"
	"github.com/jhump/protoreflect/desc"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"io"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

type BenchmarkFunction struct {
	F        func() (*runner.Report, error)
	Duration time.Duration
	Type     string
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
			Type:     queryOption.Type,
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
			Duration: queryOption.Duration,
			Type:     queryOption.Type,
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
	for i, bf := range bfs {
		totalRunTime += bf.Duration
		var wg sync.WaitGroup

		if !noProgress && !test {
			wg.Add(1)
			go pbar(bf.Duration, lo.Ternary(i == 0, rampDuration, time.Duration(0)), &wg, bf.Type)
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

	mergedReport := reports[0]

	if len(reports) > 1 {
		for _, report := range reports[1:] {
			tempDetails := slices.Concat(mergedReport.Details, report.Details)
			sort.Slice(tempDetails, func(i, j int) bool {
				return tempDetails[i].Latency < tempDetails[j].Latency
			})
			mergedReport.Details = tempDetails
			mergedReport.Count += report.Count
			mergedReport.Slowest = max(mergedReport.Slowest, report.Slowest)
			mergedReport.Fastest = min(mergedReport.Fastest, report.Fastest)
			mergedReport.Total += report.Total
			for k, v := range mergedReport.ErrorDist {
				mergedReport.ErrorDist[k] = report.ErrorDist[k] + v
			}
			for k, v := range mergedReport.StatusCodeDist {
				mergedReport.StatusCodeDist[k] = report.StatusCodeDist[k] + v
			}
			mergedReport.Average = time.Duration(
				(float64(mergedReport.Average)*float64(mergedReport.Count) + float64(report.Average)*float64(report.Count)) /
					float64(mergedReport.Count+report.Count),
			)
		}
	}
	okLats := make([]float64, 0)
	for _, d := range mergedReport.Details {
		okLats = append(okLats, d.Latency.Seconds())
	}
	mergedReport.LatencyDistribution = runner.Latencies(okLats)
	idx := slices.IndexFunc(mergedReport.LatencyDistribution, func(l runner.LatencyDistribution) bool {
		return l.Percentage == 99
	})
	var p99Average float64
	if idx == -1 {
		p99Average = mergedReport.Slowest.Seconds()
	} else {
		p99Average = mergedReport.LatencyDistribution[idx].Latency.Seconds()
	}
	mergedReport.Histogram = runner.Histogram(okLats, mergedReport.Slowest.Seconds(), mergedReport.Fastest.Seconds(), p99Average)
	timeSortedLatencies := mergedReport.Details
	slices.SortFunc(timeSortedLatencies, func(a, b runner.ResultDetail) int {
		return int(a.Timestamp.Sub(b.Timestamp))
	})
	GroupedLatenciesRPS := GroupLatencies(timeSortedLatencies, time.Duration(1*float64(time.Second)))
	var RPS []runner.DataPointRPS
	for k, v := range GroupedLatenciesRPS {
		RPS = append(RPS, runner.DataPointRPS{X: float64(k), Y: float64(len(v))})
	}
	slices.SortFunc(RPS, func(a, b runner.DataPointRPS) int {
		return int(a.X - b.X)
	})
	mergedReport.RPS = RPS

	GroupedLatencies := GroupLatencies(timeSortedLatencies, percentileWindow)

	Aggs := CalculatePercentiles(GroupedLatencies, p50, p95, p99)
	slices.SortFunc(Aggs, func(a, b runner.DataPoint) int {
		return int(a.X - b.X)
	})
	mergedReport.Aggs = Aggs
	mergedReport.P99 = p99
	mergedReport.P95 = p95
	mergedReport.P50 = p50
	return mergedReport
}

func GroupLatencies(details []runner.ResultDetail, window time.Duration) map[time.Duration][]float64 {
	groupedLatencies := make(map[time.Duration][]float64)
	initialTimestamp := details[0].Timestamp
	durationTarget := window

	i := 0
	for i < len(details) {
		var latencies []float64
		for i < len(details) && details[i].Timestamp.Sub(initialTimestamp) < durationTarget {
			latencies = append(latencies, float64(details[i].Latency))
			i += 1
		}
		slices.Sort(latencies)
		groupedLatencies[durationTarget-window] = latencies
		durationTarget += window
	}
	return groupedLatencies
}

func CalculatePercentiles(details map[time.Duration][]float64, p50 bool, p95 bool, p99 bool) []runner.DataPoint {
	var dps []runner.DataPoint
	for k, d := range details {
		dps = append(dps, runner.DataPoint{X: float64(k), Y: CalculatePercentile(d, p50, p95, p99)})
	}
	return dps
}

func CalculatePercentile(details []float64, p50 bool, p95 bool, p99 bool) runner.DataPointAgg {
	var dp runner.DataPointAgg
	if p50 {
		p50index := uint(math.Ceil(float64(len(details)) * .5))
		dp.P50 = details[min(p50index, uint(len(details)-1))]
	}
	if p95 {
		p95index := uint(math.Ceil(float64(len(details)) * .95))
		dp.P95 = details[min(p95index, uint(len(details)-1))]
	}
	if p99 {
		p99index := uint(math.Ceil(float64(len(details)) * .99))
		dp.P99 = details[min(p99index, uint(len(details)-1))]
	}
	return dp
}
