package cmd

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chalk-ai/chalk-benchmark/parse"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	enginev1 "github.com/chalk-ai/chalk-go/gen/chalk/engine/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"
	"github.com/chalk-ai/ghz/runner"
	"github.com/google/uuid"
	"github.com/jhump/protoreflect/desc"
	"github.com/samber/lo"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type BenchmarkFunction struct {
	F        *runner.Requester
	Duration time.Duration
	Type     string
	Cleanup  func() error // Optional cleanup function
}

func BenchmarkPing(grpcHost string, authHeaders []runner.Option) []BenchmarkFunction {
	pingRequest, err := proto.Marshal(&enginev1.PingRequest{Num: 10})
	if err != nil {
		fmt.Printf("Failed to marshal ping request with err: %s\n", err)
		os.Exit(1)
	}
	runConfig, err := runner.NewConfig(
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
	if err != nil {
		fmt.Printf("Failed to create run config with err: %s\n", err)
		os.Exit(1)
	}
	req, err := runner.NewRequester(
		runConfig,
	)

	return []BenchmarkFunction{{
		F:        req,
		Duration: time.Duration(0),
	}}
}

func BenchmarkQuery(
	grpcHost string,
	globalHeaders []runner.Option,
	inputSource parse.InputSource,
	rps string,
	benchmarkDuration time.Duration,
	rampDuration time.Duration,
	traceSampleRate float64,
	authHeaders map[string]string,
) []BenchmarkFunction {
	// total requests calculated from duration and RPS
	queryOptions := parse.QueryRateOptions(rps, benchmarkDuration, rampDuration)

	// Helper function to create trace metadata based on sample rate
	// Must include auth headers because WithMetadataProvider replaces all metadata
	createTraceMetadata := func(cd *runner.CallData) (*metadata.MD, error) {
		md := metadata.MD{}

		// Always include auth headers
		for key, value := range authHeaders {
			md.Set(key, value)
		}

		// Randomly decide whether to enable tracing based on sample rate
		if traceSampleRate > 0 && rand.Float64() < traceSampleRate {
			// Generate trace ID (32 hex chars) and span ID (16 hex chars)
			traceIdStr := strings.ReplaceAll(uuid.New().String(), "-", "")
			spanIdStr := strings.ReplaceAll(uuid.New().String(), "-", "")[:16]

			// Create traceparent header (W3C Trace Context format)
			// Format: version-trace_id-span_id-trace_flags
			// trace_flags = 01 means sampled
			traceparent := fmt.Sprintf("00-%s-%s-01", traceIdStr, spanIdStr)

			// Create X-Cloud-Trace-Context header (Google Cloud format)
			// Format: trace_id/1;o=1 (CLI uses "1" as hardcoded value)
			// o=1 means sampled
			cloudTraceContext := fmt.Sprintf("%s/1;o=1", traceIdStr)

			md.Set("traceparent", traceparent)
			md.Set("x-cloud-trace-context", cloudTraceContext)
		}

		return &md, nil
	}

	// Binary data function that calls inputSource.Next()
	// This is thread-safe and optimized for high throughput
	binaryDataFunc := func(mtd *desc.MethodDescriptor, cd *runner.CallData) []byte {
		data, err := inputSource.Next()
		if err != nil {
			fmt.Printf("Failed to get next input: %s\n", err)
			os.Exit(1)
		}
		return data
	}

	// Create BenchmarkFunctions for each query option
	var bfs []BenchmarkFunction
	for i, queryOption := range queryOptions {
		// Build runner options
		var runnerOptions []runner.Option

		if traceSampleRate > 0 {
			runnerOptions = slices.Concat(
				globalHeaders,
				queryOption.Options,
				[]runner.Option{
					runner.WithBinaryDataFunc(binaryDataFunc),
					runner.WithMetadataProvider(createTraceMetadata),
				},
			)
		} else {
			runnerOptions = slices.Concat(
				globalHeaders,
				queryOption.Options,
				[]runner.Option{
					runner.WithBinaryDataFunc(binaryDataFunc),
				},
			)
		}

		runConfig, err := runner.NewConfig(
			strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryBulkProcedure, "/"),
			grpcHost,
			runnerOptions...,
		)
		if err != nil {
			fmt.Printf("Failed to create run config with err: %s\n", err)
			os.Exit(1)
		}
		req, err := runner.NewRequester(runConfig)
		if err != nil {
			fmt.Printf("Failed to create requester with err: %s\n", err)
			os.Exit(1)
		}

		bf := BenchmarkFunction{
			F:        req,
			Duration: queryOption.Duration,
			Type:     queryOption.Type,
		}

		// Add cleanup function to first benchmark function
		if i == 0 {
			bf.Cleanup = inputSource.Close
		}

		bfs = append(bfs, bf)
	}
	return bfs
}


func BenchmarkUploadFeatures(
	grpcHost string,
	globalHeaders []runner.Option,
	uploadFeaturesFile string,
	rps string,
	benchmarkDuration time.Duration,
	rampDuration time.Duration,
) []BenchmarkFunction {
	file, err := os.Open(uploadFeaturesFile)
	queryOptions := parse.QueryRateOptions(rps, benchmarkDuration, rampDuration)
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
		runConfig, err := runner.NewConfig(
			strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryBulkProcedure, "/"),
			grpcHost,
			slices.Concat(
				globalHeaders,
				queryOption.Options,
				[]runner.Option{
					runner.WithBinaryData(binaryData),
				},
			)...,
		)
		if err != nil {
			fmt.Printf("Failed to create run config with err: %s\n", err)
			os.Exit(1)
		}
		req, err := runner.NewRequester(runConfig)
		if err != nil {
			fmt.Printf("Failed to create requester with err: %s\n", err)
			os.Exit(1)
		}

		bfs = append(bfs, BenchmarkFunction{
			F:        req,
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

		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func(req *runner.Requester) {
			<-c
			req.Stop(runner.ReasonCancel)
			os.Exit(1)
		}(bf.F)

		result, err := bf.F.Run()

		if err != nil {
			fmt.Printf("Failed to run request with err: %s\n", err)
			os.Exit(1)
		}

		if !noProgress && !test {
			wg.Wait()
		}
		if err != nil {
			fmt.Printf("Failed to run request with err: %s\n", err)
			os.Exit(1)
		}
		reports = append(reports, result)
	}

	// Run cleanup functions after all benchmarks complete
	for _, bf := range bfs {
		if bf.Cleanup != nil {
			if err := bf.Cleanup(); err != nil {
				fmt.Printf("Warning: cleanup error: %s\n", err)
			}
		}
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
	sort.Slice(okLats, func(i, j int) bool { return okLats[i] < okLats[j] })
	mergedReport.LatencyDistribution = runner.Latencies(okLats, p99_9)
	idx := slices.IndexFunc(mergedReport.LatencyDistribution, func(l runner.LatencyDistribution) bool {
		return l.Percentage == 99
	})
	var p99Average float64
	if idx == -1 {
		p99Average = mergedReport.Slowest.Seconds()
	} else {
		p99Average = mergedReport.LatencyDistribution[idx].Latency.Seconds()
	}
	idx99_9 := slices.IndexFunc(mergedReport.LatencyDistribution, func(l runner.LatencyDistribution) bool {
		return l.Percentage == 99.9
 	})
	var p99_9Average float64
	if idx99_9 == -1 {
		p99_9Average = mergedReport.Slowest.Seconds()
 	} else {
 	    p99_9Average = mergedReport.LatencyDistribution[idx99_9].Latency.Seconds()
	}
	mergedReport.Histogram = runner.Histogram(okLats, mergedReport.Slowest.Seconds(), mergedReport.Fastest.Seconds(), p99Average, p99_9Average)
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

	Aggs := CalculatePercentiles(GroupedLatencies, p50, p95, p99, p99_9)
	slices.SortFunc(Aggs, func(a, b runner.DataPoint) int {
		return int(a.X - b.X)
	})
	mergedReport.Aggs = Aggs
	mergedReport.P99 = p99
	mergedReport.P95 = p95
	mergedReport.P50 = p50
	mergedReport.P99_9 = p99_9
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

func CalculatePercentiles(details map[time.Duration][]float64, p50 bool, p95 bool, p99 bool, p99_9 bool) []runner.DataPoint {
	var dps []runner.DataPoint
	for k, d := range details {
		if len(d) == 0 {
			dps = append(dps, runner.DataPoint{X: float64(k), Y: runner.DataPointAgg{P50: 0, P95: 0, P99: 0, P99_9: 0}})
		}
		dps = append(dps, runner.DataPoint{X: float64(k), Y: CalculatePercentile(d, p50, p95, p99, p99_9)})
	}
	return dps
}

func CalculatePercentile(details []float64, p50 bool, p95 bool, p99 bool, p99_9 bool) runner.DataPointAgg {
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
	if p99_9 {
		p99_9index := uint(math.Ceil(float64(len(details)) * .999))
		dp.P99_9 = details[min(p99_9index, uint(len(details)-1))]
	}
	return dp
}
