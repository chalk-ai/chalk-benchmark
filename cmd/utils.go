package cmd

import (
	"embed"
	"fmt"
	_ "github.com/goccy/go-json"
	"io/fs"
	"os"
	"path/filepath"
)

func CurDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return cwd
}

//go:embed protos
var protos embed.FS

func WriteEmbeddedDirToTmp() string {
	tmpDir, err := os.MkdirTemp("", "tmp")
	// should probably call this in main and defer remove here
	if err != nil {
		fmt.Printf("Failed to create temp dir with error: %s\n", err)
		os.Exit(1)
	}

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
			fmt.Printf("error os.WriteFile error: %s\n", err)
			os.Exit(1)
		}
		return nil
	})
	return tmpDir
}
