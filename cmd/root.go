package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	"github.com/chalk-ai/chalk-go"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"

	_ "github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

func curDir() string {
    ex, err := os.Executable()
    if err != nil {
        panic(err)
    }
    return filepath.Dir(ex) + "/benchmark"
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "go run main.go --client_id <client_id> --client_secret <client_secret> --rps <rps> --duration_seconds <duration_seconds>",
	Short: "Run load test for chalk grpc",
	Long:  `This should be run on a node close to the clients sandbox`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
    if inputs == nil {
      fmt.Println("No inputs provided, please provide inputs with the `--in` flag")
		  os.Exit(1)
    }
    inputsProcessed := make(map[string]*structpb.Value)
    for k, v := range inputs {
      inputsProcessed[k] = structpb.NewStringValue(v)
    }

    outputsProcessed := make([]*commonv1.OutputExpr, len(outputs))
    for i := 0; i < len(outputsProcessed); i++ {
      outputsProcessed[i] = &commonv1.OutputExpr{
        Expr: &commonv1.OutputExpr_FeatureFqn{
          FeatureFqn: outputs[i],
        },
      }
    }
    client := OrFatal(chalk.NewClient(&chalk.ClientConfig{
      ApiServer: host,
      ClientId: clientId,
      ClientSecret: clientSecret,
      EnvironmentId: environment,
    }))("to create client")

    tokenResult, err := client.GetToken()
    fmt.Println(tokenResult.AccessToken)
    os.Exit(1)

    cd := curDir()
    total_requests := uint(float64(rps) * durationSeconds)
    report, err := runner.Run(
      strings.TrimPrefix(enginev1connect.QueryServiceOnlineQueryProcedure, "/"),
      tokenResult.Engines[environment],
      runner.WithRPS(rps),
      runner.WithTotalRequests(total_requests),
      runner.WithAsync(true),
      runner.WithConnections(16),
      runner.WithMetadata(map[string]string{
        "authorization":           fmt.Sprintf("Bearer %s", tokenResult.AccessToken),
        "x-chalk-env-id":          tokenResult.PrimaryEnvironment,
        "x-chalk-deployment-type": "engine-grpc",
      }),
      runner.WithProtoFile("./chalk/engine/v1/query_server.proto", []string{cd}),
      runner.WithSkipTLSVerify(true),
      runner.WithConcurrency(16),
      runner.WithBinaryData(OrFatal(proto.Marshal(&commonv1.OnlineQueryRequest{
        Inputs:  inputsProcessed,
        Outputs: outputsProcessed,
        Context: &commonv1.OnlineQueryContext{Options: map[string]*structpb.Value{
          "use_native_sql_operators": structpb.NewBoolValue(true),
        }},
      }))("failed to marshal request")),
    )
    ExitIfError(err, "to run query")

    p := printer.ReportPrinter{
      Out:    os.Stdout,
      Report: report,
    }

    ExitIfError(p.Print("summary"), "to print report")

    outputFile, err := os.OpenFile("report.html", os.O_RDWR, 0666)
    ExitIfError(err, "to open report")
    htmlSaver := printer.ReportPrinter{
      Out:    outputFile,
      Report: report,
    }
    ExitIfError(htmlSaver.Print("html"), "to save report")
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
var host string
var clientId string
var clientSecret string
var inputs map[string]string
var outputs []string
var environment string

func init() {
	viper.AutomaticEnv()
	flags := rootCmd.Flags()
	flags.BoolVarP(&test, "test", "t", false, "Whether to run ping command or not")
	flags.UintVarP(&rps, "rps", "r", 1, "Number of concurrent requests")
	flags.Float64VarP(&durationSeconds, "duration_seconds", "d", 120, "Amount of time to run the benchmark, in seconds")
	flags.StringVarP(&clientId, "client_id", "c", os.Getenv("CHALK_CLIENT_ID"), "Host to make requests to, with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringVarP(&environment, "environment", "e", os.Getenv("CHALK_ENV_ID"), "Host to make requests to, with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringVarP(&clientSecret, "client_secret", "s", os.Getenv("CHALK_CLIENT_SECRET"), "to make requests to, with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringToStringVar(&inputs, "in", nil, ", with port, for instance 'insert-your-host-here.chalk.ai:443'")
	flags.StringArrayVar(&outputs, "out", nil, "to make requests to, with port, for instance 'insert-your-host-here.chalk.ai:443'")
  flags.StringVar(&host, "host", "https://api.chalk.ai", "Default should not need to be updated.")
}
