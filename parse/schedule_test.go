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
	ParseScheduleFile("schedule.json", duration)
}
