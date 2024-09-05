package cmd

import (
	"testing"
)

func TestJSONParser(t *testing.T) {
	ParseScheduleFile("schedule.json")
}
