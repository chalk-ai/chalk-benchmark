package parse

import (
	"testing"
	"time"
)

func TestJSONParser(t *testing.T) {
	duration, err := time.ParseDuration("2s")
	if err != nil {
		t.Fatalf("Failed to parse duration: %s", err)
	}
	ParseScheduleFile("../testdata/schedule.json", duration)
}
