package cmd

import (
	"fmt"
	"github.com/chalk-ai/chalk-go"
	"os"
	"strings"
)

func AuthenticateUser(host string, clientId string, clientSecret string, environment string) (string, string, string) {
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

	var targetEnvironment string
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
	grpcHost := strings.TrimPrefix(strings.TrimPrefix(tokenResult.Engines[targetEnvironment], "https://"), "http://")
	return grpcHost, tokenResult.AccessToken, targetEnvironment
}
