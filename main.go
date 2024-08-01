package main

import (
	"path/filepath"
	"fmt"
	"main/cmd"
	"runtime"
)

func main() {
  _, b, _, _ := runtime.Caller(0)
  basepath := filepath.Dir(b)
  fmt.Println(basepath)
	cmd.Execute()
}
