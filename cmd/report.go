package cmd

import (
	"fmt"
	"github.com/chalk-ai/ghz/printer"
	"github.com/chalk-ai/ghz/runner"
	"os"
	"path/filepath"
	"strings"
)

func PrintReport(outputFilename string, result *runner.Report, includeRequestMetadata bool) {
	fmt.Println("\nPrinting Report...")
	processReport(result)
	p := printer.ReportPrinter{
		Out:    os.Stdout,
		Report: result,
	}

	err := p.Print("summary")
	if err != nil {
		fmt.Printf("Failed to print report with error: %s\n", err)
		os.Exit(1)
	}

	cd := CurDir()
	reportFile := filepath.Join(cd, fmt.Sprintf("%s.html", strings.TrimSuffix(outputFilename, ".html")))
	outputFile, err := os.OpenFile(reportFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		fmt.Printf("Failed to open report file with error: %s\n", err)
		os.Exit(1)
	}

	// prevents the bearer token from being printed out as part of the report
	if !includeRequestMetadata {
		result.Options.Metadata = nil
	}

	htmlSaver := printer.ReportPrinter{
		Out:    outputFile,
		Report: result,
	}

	err = htmlSaver.Print("html")
	if err != nil {
		fmt.Printf("Failed to save report with error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Wrote report file to %s\n", reportFile)
}
