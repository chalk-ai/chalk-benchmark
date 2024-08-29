package cmd

import (
	_ "github.com/goccy/go-json"
	"os"
)

func CurDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return cwd
}
