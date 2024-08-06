package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	"github.com/chalk-ai/chalk-go"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	enginev1 "github.com/chalk-ai/chalk-go/gen/chalk/engine/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"

	_ "github.com/goccy/go-json"
	pb "github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

func pbar(t time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	secondsInt := int64(t.Seconds())
	bar := pb.NewOptions(
		int(secondsInt),
		pb.OptionSetPredictTime(false),
		pb.OptionFullWidth(),
	)
	for i := int64(0); i < secondsInt; i++ {
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
			ApiServer:     host,
			ClientId:      clientId,
			ClientSecret:  clientSecret,
			EnvironmentId: "fiskridk7pfd",
			UseGrpc:       true,
		})
		if err != nil {
			fmt.Printf("Failed to create client with error: %s\n", err)
			os.Exit(1)
		}

		tmpd := WriteEmbeddedDirToTmp()
		cd := CurDir()

		tokenResult, err := client.GetToken()
		grpcHost := strings.TrimPrefix(strings.TrimPrefix(tokenResult.Engines["fiskridk7pfd"], "https://"), "http://")

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

		} else {
			wg.Add(1)
			go pbar(durationFlag, &wg)

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
				fmt.Printf("Failed to marshal online query request with inputs: '%s' and outputs: '%s'\n", inputsProcessed, outputsProcessed)
				os.Exit(1)
			}

			totalRequests := uint(float64(rps) * float64(durationFlag/time.Second))
			result, err = runner.Run(
				strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
				grpcHost,
				runner.WithRPS(rps),
				runner.WithTotalRequests(totalRequests),
				runner.WithAsync(true),
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
				fmt.Printf("Failed to run online query with err: %s\n", err)
				os.Exit(1)
			}
		}
		wg.Wait()
		p := printer.ReportPrinter{
			Out:    os.Stdout,
			Report: result,
		}

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

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
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
var includeRequestMetadata bool

func init() {
	viper.AutomaticEnv()
	flags := rootCmd.Flags()
	flags.BoolVarP(&test, "test", "t", false, "Ping the GRPC engine to make sure the benchmarking tool can reach the engine.")
	flags.UintVarP(&rps, "rps", "r", 1, "Number of concurrent requests")
	flags.DurationVarP(&durationFlag, "duration", "d", time.Duration(60.0*float64(time.Second)), "Amount of time to run the benchmark (for example, '60s').")
	flags.StringVarP(&clientId, "client_id", "c", os.Getenv("CHALK_CLIENT_ID"), "client_id for your environment.")
	flags.StringVarP(&clientSecret, "client_secret", "s", os.Getenv("CHALK_CLIENT_SECRET"), "client_secret for your environment.")
	flags.StringToStringVar(&inputStr, "in_str", nil, "string input features to the online query, for instance: 'user.id=xwdw,user.name'John'")
	flags.StringToInt64Var(&inputNum, "in_num", nil, "numeric input features to the online query, for instance 'user.id=1,user.age=28'")
	flags.StringArrayVar(&output, "out", nil, "target output features for the online query, for instance: 'user.is_fraud'")
	flags.StringVar(&outputFile, "output_file", "result.html", "Output filename for the saved report.")
	flags.StringVar(&host, "host", "https://api.chalk.ai", "API server url—in host cases, this default will work.")
	flags.BoolVar(&useNativeSql, "native_sql", false, "Whether to use the `use_native_sql_operators` planner option.")
	flags.BoolVar(&includeRequestMetadata, "include_request_md", false, "Whether to include request metadata in the report: this defaults to false since a true value includes the auth token.")
}
