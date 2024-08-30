package cmd

import (
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"github.com/chalk-ai/ghz/runner"
	"google.golang.org/protobuf/types/known/structpb"
)

func BenchmarkQuery(
	grpcHost string,
	globalHeaders []runner.Option,
	queryInputs map[string]*structpb.Value,
	queryOutputs []*commonv1.OutputExpr,
	&onlineQueryContext *commonv1.OnlineQueryContext,
	rps int,
	benchmarkDuration time.Duration,
	rampDuration time.Duration,
) (*runner.Report, error) {
	return nil, nil
}

func BenchmarkQueryFromFile(
	grpcHost string,
	globalHeaders []runner.Option,
	inputFile string,
	&onlineQueryContext *commonv1.OnlineQueryContext,
	rps int,
	benchmarkDuration time.Duration,
	rampDuration time.Duration,
) (*runner.Report, error) {
	return nil, nil
}

func BenchmarkUploadFeatures(
	grpcHost,
	globalHeaders,
	uploadFeaturesFile string,
	&onlineQueryContext,
	rps,
	benchmarkDuration,
	rampDuration,
) (*runner.Report, error) {
	return nil, nil
}
