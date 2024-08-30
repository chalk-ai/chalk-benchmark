package cmd

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/samber/lo"
	"github.com/spf13/pflag"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	parquetFile "github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"
	"github.com/chalk-ai/ghz/printer"
	"github.com/chalk-ai/ghz/runner"

	_ "github.com/goccy/go-json"
	progress "github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

const DefaultLoadRampStart = 2

type PlannerOptions struct {
	UseNativeSql          bool
	StaticUnderscoreExprs bool
}

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

func processReport(result *runner.Report) {
	// Correct Total Time & RPS calculations to exclude ramp up time
	result.Total = result.Total - rampDuration
	result.Rps = float64(result.Count) / result.Total.Seconds()
}

func pbar(t time.Duration, rampDuration time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	rampBar := progress.NewOptions(
		int(rampDuration.Seconds()),
		progress.OptionSetPredictTime(false),
		progress.OptionFullWidth(),
	)
	if float64(rampDuration) != 0 {
		fmt.Println("Ramping up load test...")
		for i := int64(0); i < int64(rampDuration.Seconds()); i++ {
			rampBar.Add(1)
			time.Sleep(1 * time.Second)
		}
	}
	bar := progress.NewOptions(
		int(t.Seconds()),
		progress.OptionSetPredictTime(false),
		progress.OptionFullWidth(),
	)
	fmt.Println("\nRunning load test...")
	for i := int64(0); i < int64(t.Seconds()); i++ {
		bar.Add(1)
		time.Sleep(1 * time.Second)
	}
	fmt.Println("\nDone sending requests, waiting for all responses...")
}

var rootCmd = &cobra.Command{
	Use:   "chalk-benchmark --client-id <client_id> --client-secret <client_secret> --rps <rps> --duration-seconds <duration_seconds> --in user.id=1 --out user.email",
	Short: "Run load test for chalk grpc",
	Long:  `This should be run on a node close to the client's sandbox`,
	Run: func(cmd *cobra.Command, args []string) {
		rampDurationSeconds := uint(math.Floor(float64(rampDuration / time.Second)))
		if rampDurationSeconds == 1 {
			fmt.Print("Ramp duration must either be 0 or greater than 1 second\n")
			os.Exit(1)
		}
		grpcHost, accessToken, targetEnvironment := AuthenticateUser(host, clientId, clientSecret, environment)

		// Writes embedded protos to tmp directory
		authHeaders := map[string]string{
			"authorization":           fmt.Sprintf("Bearer %s", accessToken),
			"x-chalk-env-id":          targetEnvironment,
			"x-chalk-deployment-type": "engine-grpc",
		}

		globalHeaders := []runner.Option{
			runner.WithSkipTLSVerify(true),
			runner.WithMetadata(authHeaders),
			runner.WithReflectionMetadata(authHeaders),
			runner.WithAsync(true),
			runner.WithConnections(numConnections),
			runner.WithTimeout(timeout),
			runner.WithAsync(true),
			runner.WithConcurrency(concurrency),
			runner.WithInsecure(insecureQueryHost),
			runner.WithSkipTLSVerify(insecureQueryHost),
		}

		var result *runner.Report
		var err error
		var wg sync.WaitGroup
		switch lo.Ternary(test, "test", lo.Ternary(uploadFeatures, "upload", lo.Ternary(inputFile != "", "query_file", "query"))) {
		case "test":
			result, err = Ping(grpcHost, globalHeaders)
		case "upload":
			result, err = BenchmarkUploadFeatures(
				grpcHost,
				globalHeaders,
				uploadFeaturesFile,
				rps,
				benchmarkDuration,
				rampDuration,
			)
		case "query_file":
			onlineQueryContext := ParseOnlineQueryContext(useNativeSql, staticUnderscoreExprs, queryName, tags)
			result, err = BenchmarkQueryFromFile(
				grpcHost,
				globalHeaders,
				inputFile,
				&onlineQueryContext,
				rps,
				benchmarkDuration,
				rampDuration,
			)
		case "query":
			queryInputs, queryOutputs := ParseInputsAndOutputs(inputStr, inputNum, input, output)
			onlineQueryContext := ParseOnlineQueryContext(useNativeSql, staticUnderscoreExprs, queryName, tags)
			result, err = BenchmarkQuery(
				grpcHost,
				globalHeaders,
				queryInputs,
				queryOutputs,
				&onlineQueryContext,
				rps,
				benchmarkDuration,
				rampDuration,
			)
		}
		if err != nil {
			fmt.Printf("Failed to run request with err: %s\n", err)
			os.Exit(1)
		}

		if uploadFeatures {
			file, err := os.Open(uploadFeaturesFile)
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
			result, err = runner.Run(
				strings.TrimPrefix(enginev1connect.QueryServiceUploadFeaturesBulkProcedure, "/"),
				grpcHost,
				slices.Concat(
					authHeadersConf,
					queryHeaders,
					[]runner.Option{
						runner.WithBinaryData(binaryData),
						runner.WithTotalRequests(1000),
					},
				)...,
			)
			if err != nil {
				fmt.Printf("Failed to run request with err: %s\n", err)
				os.Exit(1)
			}
			fmt.Printf("Successfully uploaded features to Chalk")

			p := printer.ReportPrinter{
				Out:    os.Stdout,
				Report: result,
			}
			if err := p.Print("summary"); err != nil {
				fmt.Printf("Failed to print report with error: %s\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		} else {
			if inputStr == nil && inputNum == nil && input == nil {
				fmt.Println("No inputs provided, please provide inputs with either the `--in_num` or the `--in_str` flags")
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

			outputsProcessed := make([]*commonv1.OutputExpr, len(output))
			for i := 0; i < len(outputsProcessed); i++ {
				outputsProcessed[i] = &commonv1.OutputExpr{
					Expr: &commonv1.OutputExpr_FeatureFqn{
						FeatureFqn: output[i],
					},
				}
			}
			var queryContext commonv1.OnlineQueryContext

			if queryName != "" {
				queryContext = commonv1.OnlineQueryContext{
					QueryName: &queryName,
					Tags:      tags,
				}
			}
			oqr := commonv1.OnlineQueryRequest{
				Inputs:  inputsProcessed,
				Outputs: outputsProcessed,
				Context: &queryContext,
			}
			if verbose {
				fmt.Printf("Inputs: %v\n", inputsProcessed)
				fmt.Printf("Outputs: %v\n", outputsProcessed)
			}
			oqr.Context = &commonv1.OnlineQueryContext{Options: map[string]*structpb.Value{
				"use_native_sql_operators":      structpb.NewBoolValue(useNativeSql),
				"static_underscore_expressions": structpb.NewBoolValue(staticUnderscoreExprs),
			}}
			binaryData, err := proto.Marshal(&oqr)
			if err != nil {
				fmt.Printf("Failed to marshal online query request with inputs: '%s' and outputs: '%s'\n", inputsProcessed, outputsProcessed)
				os.Exit(1)
			}

			totalRequests := uint(float64(rps) * float64(benchmarkDuration/time.Second))
			runnerOptions := []runner.Option{
				runner.WithRPS(rps),
				runner.WithAsync(true),
				runner.WithTotalRequests(totalRequests),
				runner.WithConnections(numConnections),
				runner.WithMetadata(map[string]string{
					"authorization":           fmt.Sprintf("Bearer %s", accessToken),
					"x-chalk-env-id":          targetEnvironment,
					"x-chalk-deployment-type": "engine-grpc",
				}),
				runner.WithReflectionMetadata(map[string]string{
					"authorization":           fmt.Sprintf("Bearer %s", accessToken),
					"x-chalk-env-id":          targetEnvironment,
					"x-chalk-deployment-type": "engine-grpc",
				}),
				runner.WithSkipTLSVerify(insecureQueryHost),
				runner.WithInsecure(insecureQueryHost),
				runner.WithConcurrency(16),
				runner.WithBinaryData(binaryData),
			}
			additionalArgs := []runner.Option{runner.WithTimeout(timeout)}
			runnerOptions = append(runnerOptions, additionalArgs...)

			step := uint(math.Floor(float64(rps) / float64(rampDurationSeconds)))

			loadEnd := step * rampDurationSeconds

			numWarmUpQueries := (rampDurationSeconds / 2) * (2*DefaultLoadRampStart + (rampDurationSeconds-1)*step)
			fmt.Printf("Ramping up to %d RPS over %d seconds with %d warm-up queries\n", rps, rampDurationSeconds, numWarmUpQueries)

			if step >= 0 {
				runnerOptions = []runner.Option{
					runner.WithTotalRequests(totalRequests + numWarmUpQueries),
					runner.WithLoadSchedule("line"),
					runner.WithLoadStart(DefaultLoadRampStart),
					runner.WithLoadEnd(loadEnd),
					runner.WithLoadStep(int(step)),
					runner.WithSkipFirst(numWarmUpQueries),
					runner.WithRPS(rps),
					runner.WithAsync(true),
					runner.WithConnections(numConnections),
					runner.WithMetadata(map[string]string{
						"authorization":           fmt.Sprintf("Bearer %s", accessToken),
						"x-chalk-env-id":          targetEnvironment,
						"x-chalk-deployment-type": "engine-grpc",
					}),
					runner.WithReflectionMetadata(map[string]string{
						"authorization":           fmt.Sprintf("Bearer %s", accessToken),
						"x-chalk-env-id":          targetEnvironment,
						"x-chalk-deployment-type": "engine-grpc",
					}),
					runner.WithSkipTLSVerify(true),
					runner.WithConcurrency(concurrency),
					runner.WithBinaryData(binaryData),
					runner.WithTimeout(timeout),
				}
			}
			// add extra queries total request
			totalRequests += numWarmUpQueries

			wg.Add(1)
			go pbar(benchmarkDuration, rampDuration, &wg)

			result, err = runner.Run(
				strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
				lo.Ternary(queryHost == "", grpcHost, queryHost),
				runnerOptions...,
			)
			if err != nil {
				fmt.Printf("Failed to run online query with err: %s\n", err)
				os.Exit(1)
			}
		}
		wg.Wait()

		PrintReport(outputFilename, result, includeRequestMetadata)
	}
}

func getParquetFileRecordReader(featuresFile string) (pqarrow.RecordReader, error) {
	file, err := parquetFile.OpenParquetFile(featuresFile, false)

	if err != nil {
		return nil, err
	}
	reader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	return reader.GetRecordReader(context.Background(), nil, nil)
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func normalizeFlagNames(f *pflag.FlagSet, name string) pflag.NormalizedName {
	switch name {
	case "host":
		name = "api-host"
		break
	default:
		name = strings.Replace(name, "_", "-", -1)
		break
	}
	return pflag.NormalizedName(name)
}

// benchmark parameters
var test bool
var rps uint
var benchmarkDuration time.Duration
var rampDuration time.Duration
var numConnections uint
var concurrency uint
var timeout time.Duration
var outputFilename string

// environment & client parameters
var host string
var insecureQueryHost bool
var queryHost string
var environment string
var clientId string
var clientSecret string

// input & output parameters
var input map[string]string
var inputStr map[string]string
var inputNum map[string]int64
var inputFile string
var output []string
var tags []string
var uploadFeatures bool
var uploadFeaturesFile string
var queryName string
var token string

// planner options
var useNativeSql bool
var staticUnderscoreExprs bool

// other parameters
var includeRequestMetadata bool
var verbose bool
var noProgress bool

func init() {
	viper.AutomaticEnv()
	flags := rootCmd.Flags()
	flags.SetNormalizeFunc(normalizeFlagNames)

	// benchmark parameters
	flags.BoolVarP(&test, "test", "t", false, "Ping the GRPC engine to make sure the benchmarking tool can reach the engine.")
	flags.UintVarP(&rps, "rps", "r", 1, "Number of requests to execute per second against the engine.")
	flags.DurationVarP(&benchmarkDuration, "duration", "d", time.Duration(60.0*float64(time.Second)), "Amount of time to run the ramp up for the benchmark (only applies if the RPS>20)")
	flags.DurationVar(&rampDuration, "ramp_duration", time.Duration(10.0*float64(time.Second)), "Amount of time to run the benchmark (for example, '60s').")
	flags.UintVar(&numConnections, "num_connections", 16, "The number of gRPC connections used by the tool.")
	flags.UintVar(&concurrency, "concurrency", 16, "Number of workers (concurrency) for requests.")
	flags.DurationVar(&timeout, "timeout", 20*time.Second, "Timeout for requests.")
	flags.StringVar(&outputFilename, "output_file", "result.html", "Output filename for the saved report.")
	flags.StringVar(&token, "token", os.Getenv("CHALK_BENCHMARK_TOKEN"), "jwt to use for the request—if this is provided the client_id and client_secret will be ignored.")

	// environment & client parameters
	flags.StringVar(&host, "host", "https://api.chalk.ai", "API server url—in host cases, this default will work.")
	flags.StringVar(&queryHost, "query_host", "", "query server url—in host cases, this default will work.")
	flags.BoolVar(&insecureQueryHost, "insecure_query_host", false, "whether to run the client without TLS—can be useful when making requests directly to the engine.")
	flags.StringVar(&environment, "environment", "", "Environment for the client.")
	flags.StringVarP(&clientId, "client_id", "c", os.Getenv("CHALK_CLIENT_ID"), "client_id for your environment.")
	flags.StringVarP(&clientSecret, "client_secret", "s", os.Getenv("CHALK_CLIENT_SECRET"), "client_secret for your environment.")

	// input & output parameters
	flags.StringToStringVar(&input, "in", nil, "input features to the online query, for instance: 'user.id=xwdw,user.name'John'. This flag will try to convert inputs to the right type. If you need to explicitly pass in a number or string, use the `in-num` or `in-str` flag.")
	flags.StringToStringVar(&inputStr, "in_str", nil, "string input features to the online query, for instance: 'user.id=xwdw,user.name'John'.")
	flags.StringToInt64Var(&inputNum, "in_num", nil, "numeric input features to the online query, for instance 'user.id=1,user.age=28'")
	flags.StringArrayVar(&output, "out", nil, "target output features for the online query, for instance: 'user.is_fraud'.")
	flags.StringArrayVar(&tags, "tag", nil, "Tags to add to the online query: e.g. '--tag test'.")
	flags.BoolVar(&uploadFeatures, "upload_features", false, "Whether to upload features to Chalk.")
	flags.StringVar(&uploadFeaturesFile, "upload_features_file", "", "File containing features to upload to Chalk.")
	flags.StringVar(&queryName, "query_name", "", "Query name for the benchmark query.")

	// planner options
	flags.BoolVar(&useNativeSql, "native_sql", false, "Whether to use the `use_native_sql_operators` planner option—defaults to whatever is set for environment.")
	flags.BoolVar(&staticUnderscoreExprs, "static_underscore", true, "Whether to use the `static_underscore_expressions` planner option—defaults to whatever is set for environment.")

	// other parameters
	flags.BoolVar(&includeRequestMetadata, "include_request_md", false, "Whether to include request metadata in the report: this defaults to false since a true value includes the auth token.")
	flags.BoolVar(&verbose, "verbose", false, "Whether to print verbose output.")
	flags.BoolVar(&noProgress, "no-progress", false, "Whether to print verbose output.")
}
