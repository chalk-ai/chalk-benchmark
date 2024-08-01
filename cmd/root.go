package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	"github.com/chalk-ai/chalk-go"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	enginev1 "github.com/chalk-ai/chalk-go/gen/chalk/engine/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"

	_ "github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

func curDir() string {
	_, filename, _, ok := runtime.Caller(0)

	if !ok {
		fmt.Println("Failed to find working directory")
		os.Exit(1)
	}
	return filepath.Dir(filename)
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "go run main.go --client_id <client_id> --client_secret <client_secret> --rps <rps> --duration_seconds <duration_seconds>",
	Short: "Run load test for chalk grpc",
	Long:  `This should be run on a node close to the client's sandbox`,
	Run: func(cmd *cobra.Command, args []string) {
		client := OrFatal(chalk.NewClient(&chalk.ClientConfig{
			ApiServer:     host,
			ClientId:      clientId,
			ClientSecret:  clientSecret,
			EnvironmentId: environment,
		}))("to create client")

		tokenResult, err := client.GetToken()
		grpcHost := strings.TrimPrefix(strings.TrimPrefix(tokenResult.Engines[tokenResult.PrimaryEnvironment], "https://"), "http://")

		cd := curDir()

		var result *runner.Report

		if test {
			pingRequest, err := proto.Marshal(&enginev1.PingRequest{Num: 10})
			if err != nil {
				fmt.Printf("Failed to marshal ping request with err: %s", err)
				os.Exit(1)
			}
			result, err = runner.Run(
				strings.TrimPrefix(enginev1connect.QueryServicePingProcedure, "/"),
				fmt.Sprintf("%s:443", grpcHost),
				runner.WithRPS(1),
				runner.WithTotalRequests(1),
				runner.WithMetadata(map[string]string{
					"authorization":           fmt.Sprintf("Bearer %s", tokenResult.AccessToken),
					"x-chalk-env-id":          tokenResult.PrimaryEnvironment,
					"x-chalk-deployment-type": "engine-grpc",
				}),
				runner.WithProtoFile("./chalk/engine/v1/query_server.proto", []string{filepath.Join(cd, "protos")}),
				runner.WithSkipTLSVerify(true),
				runner.WithBinaryData(pingRequest),
			)
			if err != nil {
				fmt.Printf("Failed to run Ping request with err: %s", err)
				os.Exit(1)
			}

		} else {
			if inputStr == nil && inputNum == nil {
				fmt.Println("No inputs provided, please provide inputs with either the `--in_num` or the `--in_str` flags")
				os.Exit(1)
			}
			inputsProcessed := make(map[string]*structpb.Value)
			for k, v := range inputNum {
				if inputsAreInts {
					inputsProcessed[k] = structpb.NewNumberValue(float64(v))
				}
			}

			for k, v := range inputStr {
				if inputsAreInts {
					inputsProcessed[k] = structpb.NewStringValue(v)
				}
			}

			outputsProcessed := make([]*commonv1.OutputExpr, len(output))
			for i := 0; i < len(outputsProcessed); i++ {
				outputsProcessed[i] = &commonv1.OutputExpr{
					Expr: &commonv1.OutputExpr_FeatureFqn{
						FeatureFqn: output[i],
					},
				}
			}

			oqr := commonv1.OnlineQueryRequest{
				Inputs:  inputsProcessed,
				Outputs: outputsProcessed,
			}
			if useNativeSql {
				oqr.Context = &commonv1.OnlineQueryContext{Options: map[string]*structpb.Value{
					"use_native_sql_operators": structpb.NewBoolValue(true),
				}}
			}
			binaryData, err := proto.Marshal(&oqr)
			if err != nil {
				fmt.Printf("Failed to marshal online query request with inputs: '%s' and outputs: '%s'", inputsProcessed, outputsProcessed)
				os.Exit(1)
			}

			total_requests := uint(float64(rps) * durationSeconds)
			result, err = runner.Run(
				strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
				fmt.Sprintf("%s:443", grpcHost),
				runner.WithRPS(rps),
				runner.WithTotalRequests(total_requests),
				runner.WithAsync(true),
				runner.WithConnections(16),
				runner.WithMetadata(map[string]string{
					"authorization":           fmt.Sprintf("Bearer %s", tokenResult.AccessToken),
					"x-chalk-env-id":          tokenResult.PrimaryEnvironment,
					"x-chalk-deployment-type": "engine-grpc",
				}),
				runner.WithProtoFile("./chalk/engine/v1/query_server.proto", []string{filepath.Join(cd, "protos")}),
				runner.WithSkipTLSVerify(true),
				runner.WithConcurrency(16),
				runner.WithBinaryData(binaryData),
			)
			if err != nil {
				fmt.Printf("Failed to run online query with err: %s", err)
				os.Exit(1)
			}
		}

		p := printer.ReportPrinter{
			Out:    os.Stdout,
			Report: result,
		}

		ExitIfError(p.Print("summary"), "failed to print report")

		outputFile, err := os.OpenFile(filepath.Join(filepath.Dir(cd), fmt.Sprintf("%s.html", strings.TrimSuffix(outputFile, ".html"))), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
		if err != nil {
			fmt.Printf("Failed to open report file with error: %s", err)
			os.Exit(1)
		}

		// prevents the bearer token from being printed out as part of the report
		if removeReportMetadata {
			result.Options.Metadata = nil
		}

		htmlSaver := printer.ReportPrinter{
			Out:    outputFile,
			Report: result,
		}
		err = htmlSaver.Print("html")
		if err != nil {
			fmt.Printf("Failed to save report with error: %s", err)
			os.Exit(1)
		}
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var rps uint
var durationSeconds float64
var test bool
var inputsAreInts bool
var host string
var clientId string
var clientSecret string
var inputStr map[string]string
var inputNum map[string]int64
var output []string
var environment string
var outputFile string
var useNativeSql bool
var removeReportMetadata bool

func init() {
	viper.AutomaticEnv()
	flags := rootCmd.Flags()
	flags.BoolVarP(&test, "test", "t", false, "Whether to run ping command or not")
	flags.UintVarP(&rps, "rps", "r", 1, "Number of concurrent requests")
	flags.Float64VarP(&durationSeconds, "duration_seconds", "d", 120, "Amount of time to run the benchmark, in seconds")
	flags.BoolVarP(&inputsAreInts, "inputs_are_ints", "i", false, "Go benchmark treats inputs as strings by default. If you want to use ints, set this flag to true")
	flags.StringVarP(&environment, "environment", "e", os.Getenv("CHALK_ENV_ID"), "Host to make requests to, with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringVarP(&clientId, "client_id", "c", os.Getenv("CHALK_CLIENT_ID"), "Host to make requests to, with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringVarP(&clientSecret, "client_secret", "s", os.Getenv("CHALK_CLIENT_SECRET"), "to make requests to, with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringToStringVar(&inputStr, "in_str", nil, ", with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringToInt64Var(&inputNum, "in_num", nil, ", with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringArrayVar(&output, "out", nil, "to make requests to, with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringVarP(&outputFile, "output_file", "o", "result.html", "File name for saved report.")
	flags.StringVar(&host, "host", "https://api.chalk.ai", "Default should not need to be updated.")
	flags.BoolVar(&useNativeSql, "native_sql", false, "Whether to use the `use_native_sql_operators` planner option.")
	flags.BoolVar(&removeReportMetadata, "remove_metadata", true, "Whether to use the `use_native_sql_operators` planner option.")
}
