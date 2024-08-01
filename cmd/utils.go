package cmd

import (
	"fmt"
	_ "github.com/goccy/go-json"
	"os"
)

func ExitIfError(err error, msg string) {
	if err != nil {
		fmt.Printf("Exiting: %s, %s", msg, err)
		os.Exit(1)
	}
}
func OrFatal[T any](t T, err error) func(s string) T {
	return func(s string) T {
		ExitIfError(err, s)
		return t
	}
}
