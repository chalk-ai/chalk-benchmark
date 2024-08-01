package cmd

import (
	"fmt"
	_ "github.com/goccy/go-json"
  "embed"
	"os"
  "path/filepath"
  "io/fs"

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

//go:embed protos
var protos embed.FS

func WriteEmbeddedDirToTmp() string {
  tmpDir := OrFatal(os.MkdirTemp("", "tmp"))("Failed to create temp dir")
  // should defer close here

  fs.WalkDir(protos, ".", func(path string, d fs.DirEntry, err error) error {
    if d.IsDir() {
      err := os.MkdirAll(filepath.Join(tmpDir, path), os.ModePerm)
      if err != nil {
        fmt.Printf("Failed to create dir: %s\n", err)
        os.Exit(1)
      }
      return nil
    }
    fileContent, err := protos.ReadFile(path)
    if err != nil {
      fmt.Printf("Failed to read embedded dir: %s\n", err)
      os.Exit(1)
    }
    if err := os.WriteFile(filepath.Join(tmpDir, path), fileContent, 0666); err != nil {
        fmt.Printf("error os.WriteFile error: %v", err)
        os.Exit(1)
    }
    return nil
  })
  return tmpDir
}

