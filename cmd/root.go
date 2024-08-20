package cmd

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/spf13/pflag"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	parquetFile "github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	"github.com/chalk-ai/chalk-go"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	enginev1 "github.com/chalk-ai/chalk-go/gen/chalk/engine/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"

	_ "github.com/goccy/go-json"
	progress "github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

const MinRPSForRamp = 20
const DefaultLoadRampStart = 2

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
}

var rootCmd = &cobra.Command{
	Use:   "chalk-benchmark --client_id <client_id> --client_secret <client_secret> --rps <rps> --duration_seconds <duration_seconds>",
	Short: "Run load test for chalk grpc",
	Long:  `This should be run on a node close to the client's sandbox`,
	Run: func(cmd *cobra.Command, args []string) {
		client, err := chalk.NewClient(&chalk.ClientConfig{
			ApiServer:    host,
			ClientId:     clientId,
			ClientSecret: clientSecret,
			// EnvironmentId: environment,
			UseGrpc: true,
		})
		if err != nil {
			fmt.Printf("Failed to create client with error: %s\n", err)
			os.Exit(1)
		}

		tmpd := WriteEmbeddedDirToTmp()
		cd := CurDir()

		tokenResult, err := client.GetToken()
		grpcHost := strings.TrimPrefix(strings.TrimPrefix(tokenResult.Engines[tokenResult.PrimaryEnvironment], "https://"), "http://")

		var result *runner.Report
		var wg sync.WaitGroup

		if test {
			pingRequest, err := proto.Marshal(&enginev1.PingRequest{Num: 10})
			if err != nil {
				fmt.Printf("Failed to marshal ping request with err: %s\n", err)
				os.Exit(1)
			}
			result, err = runner.Run(
				strings.TrimPrefix(enginev1connect.QueryServicePingProcedure, "/"),
				grpcHost,
				runner.WithRPS(1),
				runner.WithTotalRequests(1),
				runner.WithMetadata(map[string]string{
					"authorization":           fmt.Sprintf("Bearer %s", tokenResult.AccessToken),
					"x-chalk-env-id":          tokenResult.PrimaryEnvironment,
					"x-chalk-deployment-type": "engine-grpc",
				}),
				runner.WithProtoFile("./chalk/engine/v1/query_server.proto", []string{filepath.Join(tmpd, "protos")}),
				runner.WithSkipTLSVerify(true),
				runner.WithBinaryData(pingRequest),
			)
			if err != nil {
				fmt.Printf("Failed to run Ping request with err: %s\n", err)
				os.Exit(1)
			}
			fmt.Printf("Successfully pinged GRPC Engine")
			os.Exit(0)

		} else if uploadFeatures {
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
				runner.WithRPS(rps),
				runner.WithAsync(true),
				runner.WithTotalRequests(1000),
				runner.WithConnections(16),
				runner.WithMetadata(map[string]string{
					"authorization":           fmt.Sprintf("Bearer %s", tokenResult.AccessToken),
					"x-chalk-env-id":          tokenResult.PrimaryEnvironment,
					"x-chalk-deployment-type": "engine-grpc",
				}),
				runner.WithProtoFile("./chalk/engine/v1/query_server.proto", []string{filepath.Join(tmpd, "protos")}),
				runner.WithSkipTLSVerify(true),
				runner.WithConcurrency(16),
				runner.WithBinaryData(binaryData),
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

			if inputStr == nil && inputNum == nil {
				fmt.Println("No inputs provided, please provide inputs with either the `--in_num` or the `--in_str` flags")
				os.Exit(1)
			}
			inputsProcessed := make(map[string]*structpb.Value)
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
				queryContext = commonv1.OnlineQueryContext{QueryName: &queryName}
			}
			oqr := commonv1.OnlineQueryRequest{
				Inputs:  inputsProcessed,
				Outputs: outputsProcessed,
				Context: &queryContext,
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

			totalRequests := uint(float64(rps) * float64(durationFlag/time.Second))
			runnerOptions := []runner.Option{
				runner.WithRPS(rps),
				runner.WithAsync(true),
				runner.WithTotalRequests(totalRequests),
				runner.WithConnections(16),
				runner.WithMetadata(map[string]string{
					"authorization":           fmt.Sprintf("Bearer %s", tokenResult.AccessToken),
					"x-chalk-env-id":          tokenResult.PrimaryEnvironment,
					"x-chalk-deployment-type": "engine-grpc",
				}),
				runner.WithProtoFile("./chalk/engine/v1/query_server.proto", []string{filepath.Join(tmpd, "protos")}),
				runner.WithSkipTLSVerify(true),
				runner.WithConcurrency(16),
				runner.WithBinaryData(binaryData),
			}

			if rps > MinRPSForRamp {
				rampDurationSeconds := uint(math.Floor(float64(rampDuration / time.Second)))

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
							"authorization":           fmt.Sprintf("Bearer %s", tokenResult.AccessToken),
							"x-chalk-env-id":          tokenResult.PrimaryEnvironment,
							"x-chalk-deployment-type": "engine-grpc",
						}),
						runner.WithProtoFile("./chalk/engine/v1/query_server.proto", []string{filepath.Join(tmpd, "protos")}),
						runner.WithSkipTLSVerify(true),
						runner.WithConcurrency(concurrency),
						runner.WithBinaryData(binaryData),
					}
				}
				// add extra queries total request
				totalRequests += numWarmUpQueries

			} else {
				rampDuration = time.Duration(0)
			}
			wg.Add(1)
			go pbar(durationFlag, rampDuration, &wg)

			result, err = runner.Run(
				strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
				grpcHost,
				runnerOptions...,
			)
			if err != nil {
				fmt.Printf("Failed to run online query with err: %s\n", err)
				os.Exit(1)
			}
		}
		wg.Wait()
		p := printer.ReportPrinter{
			Out:    os.Stdout,
			Report: result,
		}

		// Correct Total Time & RPS calculations to exclude ramp up time
		result.Total = result.Total - rampDuration
		result.Rps = float64(result.Count) / result.Total.Seconds()

		err = p.Print("summary")
		if err != nil {
			fmt.Printf("Failed to print report with error: %s\n", err)
			os.Exit(1)
		}

		reportFile := filepath.Join(cd, fmt.Sprintf("%s.html", strings.TrimSuffix(outputFile, ".html")))
		outputFile, err := os.OpenFile(reportFile, os.O_RDWR|os.O_CREATE, 0660)
		if err != nil {
			fmt.Printf("Failed to open report file with error: %s\n", err)
			os.Exit(1)
		}

		// prevents the bearer token from being printed out as part of the report
		if !includeRequestMetadata {
			result.Options.Metadata = nil
		}

		htmlSaver := printer.ReportPrinter{
			Out:    outputFile,
			Report: result,
		}

		err = htmlSaver.Print("html")
		if err != nil {
			fmt.Printf("Failed to save report with error: %s\n", err)
			os.Exit(1)
		}
		fmt.Printf("Wrote report file to %s\n", reportFile)
	},
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
	case "client_id":
		name = "client-id"
		break
	case "client_secret":
		name = "client-secret"
		break
	case "in_str":
		name = "in-str"
		break
	case "in_num":
		name = "in-num"
		break
	case "output_file":
		name = "output-file"
		break
	case "native_sql":
		name = "native-sql"
		break
	case "include_request_md":
		name = "include-request-md"
		break
	case "ramp_duration":
		name = "ramp-duration"
		break
	case "query_name":
		name = "query-name"
		break
	case "num_connections":
		name = "num-connections"
		break
	}
	return pflag.NormalizedName(name)
}

var rps uint
var durationFlag time.Duration
var test bool
var host string
var clientId string
var clientSecret string
var inputStr map[string]string
var inputNum map[string]int64
var output []string
var outputFile string
var useNativeSql bool
var staticUnderscoreExprs bool
var includeRequestMetadata bool
var rampDuration time.Duration
var queryName string
var uploadFeatures bool
var uploadFeaturesFile string
var numConnections uint
var concurrency uint

func init() {
	viper.AutomaticEnv()
	flags := rootCmd.Flags()
	flags.SetNormalizeFunc(normalizeFlagNames)
	flags.BoolVarP(&test, "test", "t", false, "Ping the GRPC engine to make sure the benchmarking tool can reach the engine.")
	flags.UintVarP(&rps, "rps", "r", 1, "Number of concurrent requests.")
	flags.DurationVarP(&durationFlag, "duration", "d", time.Duration(60.0*float64(time.Second)), "Amount of time to run the ramp up for the benchmark (only applies if the RPS>20)")
	flags.DurationVar(&rampDuration, "ramp_duration", time.Duration(10.0*float64(time.Second)), "Amount of time to run the benchmark (for example, '60s').")
	flags.StringVarP(&clientId, "client_id", "c", os.Getenv("CHALK_CLIENT_ID"), "client_id for your environment.")
	flags.StringVarP(&clientSecret, "client_secret", "s", os.Getenv("CHALK_CLIENT_SECRET"), "client_secret for your environment.")
	flags.StringToStringVar(&inputStr, "in_str", nil, "string input features to the online query, for instance: 'user.id=xwdw,user.name'John'.")
	flags.StringToInt64Var(&inputNum, "in_num", nil, "numeric input features to the online query, for instance 'user.id=1,user.age=28'")
	flags.StringVar(&queryName, "query_name", "", "Query name for the benchmark query.")
	flags.StringArrayVar(&output, "out", nil, "target output features for the online query, for instance: 'user.is_fraud'.")
	flags.StringVar(&outputFile, "output_file", "result.html", "Output filename for the saved report.")
	flags.StringVar(&host, "host", "https://api.chalk.ai", "API server url—in host cases, this default will work.")
	flags.BoolVar(&useNativeSql, "native_sql", false, "Whether to use the `use_native_sql_operators` planner option.")
	flags.BoolVar(&staticUnderscoreExprs, "static_underscore", true, "Whether to use the `static_underscore_expressions` planner option.")
	flags.BoolVar(&includeRequestMetadata, "include_request_md", false, "Whether to include request metadata in the report: this defaults to false since a true value includes the auth token.")
	flags.BoolVar(&uploadFeatures, "upload_features", false, "Whether to upload features to Chalk.")
	flags.StringVar(&uploadFeaturesFile, "upload_features_file", "", "File containing features to upload to Chalk.")
	flags.UintVar(&numConnections, "num_connections", 16, "Number of connections for requests.")
	flags.UintVar(&concurrency, "concurrency", 16, "Concurrency for requests.")
	flags.StringVar(&uploadFeaturesFile, "upload_features_file", "", "File containing features to upload to Chalk.")
}
