package cmd

import (
	"fmt"
	enginev1 "github.com/chalk-ai/chalk-go/gen/chalk/engine/v1"
	"github.com/chalk-ai/chalk-go/gen/chalk/engine/v1/enginev1connect"
	"github.com/chalk-ai/ghz/runner"
	"google.golang.org/protobuf/proto"
	"os"
	"slices"
	"strings"
)

func Ping(grpcHost string, authHeaders []runner.Option) (*runner.Report, error) {
	pingRequest, err := proto.Marshal(&enginev1.PingRequest{Num: 10})
	if err != nil {
		fmt.Printf("Failed to marshal ping request with err: %s\n", err)
		os.Exit(1)
	}
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
