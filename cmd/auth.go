package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/chalk-ai/chalk-go"
)

func AuthenticateUser(host string, clientId string, clientSecret string, environment string) (string, string, string) {
	var targetEnvironment string
	var grpcHost string
	var accessToken string

	if token != "" {
		slog.Debug("Authenticating with token, not authenticating through grpc server.")
		if environment == "" || queryHost == "" {
			fmt.Println("When authenticating directly with a token, the environment and query-host must be explicitly provided. Please provide an environment with the `--environment` flag and a query host with the `--query-host` flag.")
			os.Exit(1)
		}
		slog.Debug(fmt.Sprintf("Host: '%s' | QueryHost: '%s' | Environment: '%s'", host, queryHost, environment))
		targetEnvironment = environment
		grpcHost = strings.TrimPrefix(strings.TrimPrefix(queryHost, "https://"), "http://")
		accessToken = token
	} else {
		if environment == "" {
			slog.Debug(fmt.Sprintf("Authenticating to environment '%s' through API server: '%s'", environment, host))
		} else {
			slog.Debug(fmt.Sprintf("Authenticating through API server: '%s'", host))
		}
		client, err := chalk.NewClient(&chalk.ClientConfig{
			ApiServer:     host,
			ClientId:      clientId,
			ClientSecret:  clientSecret,
			UseGrpc:       true,
			EnvironmentId: environment,
		})
		if err != nil {
			fmt.Printf("Failed to create client with error: %s\n", err)
			os.Exit(1)
		}

		tokenResult, err := client.GetToken()

		if err != nil {
			fmt.Printf("Failed to get token with error: %s\n", err)
			os.Exit(1)
		}
		if tokenResult.PrimaryEnvironment == "" && environment == "" {
			fmt.Printf("Failed to find target environment for benchmark. If you are using your user token instead of a service token, pass the environment id in explicitly using the `--environment` flag\n")
			os.Exit(1)
		} else if environment != "" && tokenResult.PrimaryEnvironment == "" {
			targetEnvironment = environment
		} else if tokenResult.PrimaryEnvironment != "" && environment == "" {
			targetEnvironment = tokenResult.PrimaryEnvironment
		} else if environment == tokenResult.PrimaryEnvironment {
			targetEnvironment = environment
		} else {
			fmt.Printf("Service token environment '%s' does not match the provided environment '%s'\n", tokenResult.PrimaryEnvironment, environment)
			os.Exit(1)
		}
		if queryHost != "" {
			grpcHost = strings.TrimPrefix(strings.TrimPrefix(queryHost, "https://"), "http://")
		} else {
			grpcHost = strings.TrimPrefix(strings.TrimPrefix(tokenResult.Engines[targetEnvironment], "https://"), "http://")
		}
		accessToken = tokenResult.AccessToken
	}
	return grpcHost, accessToken, targetEnvironment
}
