package report

import (
	"fmt"
	"github.com/chalk-ai/ghz/printer"
	"github.com/chalk-ai/ghz/runner"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func CurDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return cwd
}

type ReportType string

var (
	ReportTypeHTML ReportType = "html"
	ReportTypeJSON ReportType = "json"
)

func processReport(result *runner.Report, rampDuration time.Duration) {
	// Correct Total Time & RPS calculations to exclude ramp up time
	result.Total = result.Total - rampDuration
	result.Rps = float64(result.Count) / result.Total.Seconds()
}

func PrintReport(result *runner.Report, rampDuration time.Duration) {
	fmt.Println("\nPrinting Report...")
	processReport(result, rampDuration)
	p := printer.ReportPrinter{
		Out:    os.Stdout,
		Report: result,
	}

	err := p.Print("summary")
	if err != nil {
		fmt.Printf("Failed to print report with error: %s\n", err)
		os.Exit(1)
	}
}

func SaveReport(outputFilename string, result *runner.Report, includeRequestMetadata bool, reportType ReportType) {
	filenameNoPrefix := strings.TrimSuffix(outputFilename, "."+string(reportType))
	reportFile := filepath.Join(
		CurDir(),
		fmt.Sprintf("%s.%s", filenameNoPrefix, reportType),
	)
	outputFile, err := os.OpenFile(reportFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		fmt.Printf("Failed to open report file with error: %s\n", err)
		os.Exit(1)
	}

	// prevents the bearer token from being printed out as part of the report
	if !includeRequestMetadata {
		result.Options.Metadata = nil
	}

	fileSaver := printer.ReportPrinter{
		Out:    outputFile,
		Report: result,
	}

	err = fileSaver.Print(string(reportType))
	if err != nil {
		fmt.Printf("Failed to save report with error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Wrote report file to %s\n", reportFile)
}
