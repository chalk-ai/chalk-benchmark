package parse

import (
	"fmt"
	"github.com/chalk-ai/ghz/load"
	"github.com/chalk-ai/ghz/runner"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

const DefaultLoadRampStart = 2

// ParseLoadSegments parses a load pattern string like "1h@1000,30m@2000,2h@500"
// Format: <duration>@<rps>,<duration>@<rps>,...
// Returns nil if the string is not in the expected format
func ParseLoadSegments(s string) ([]load.LoadSegment, error) {
	// Check if it contains the @ symbol (our syntax marker)
	if !strings.Contains(s, "@") {
		return nil, nil
	}

	segments := []load.LoadSegment{}
	parts := strings.Split(s, ",")

	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split by @ to get duration and RPS
		atParts := strings.Split(part, "@")
		if len(atParts) != 2 {
			return nil, fmt.Errorf("segment %d: expected format '<duration>@<rps>', got '%s'", i+1, part)
		}

		durationStr := strings.TrimSpace(atParts[0])
		rpsStr := strings.TrimSpace(atParts[1])

		// Parse duration
		duration, err := time.ParseDuration(durationStr)
		if err != nil {
			return nil, fmt.Errorf("segment %d: failed to parse duration '%s': %w", i+1, durationStr, err)
		}

		if duration <= 0 {
			return nil, fmt.Errorf("segment %d: duration must be positive, got %s", i+1, duration)
		}

		// Parse RPS
		rpsValue, err := strconv.ParseFloat(rpsStr, 64)
		if err != nil {
			return nil, fmt.Errorf("segment %d: failed to parse RPS value '%s': %w", i+1, rpsStr, err)
		}

		if rpsValue < 0 {
			return nil, fmt.Errorf("segment %d: RPS must be non-negative, got %f", i+1, rpsValue)
		}

		segments = append(segments, load.LoadSegment{
			RPS:      rpsValue,
			Duration: duration,
		})
	}

	if len(segments) == 0 {
		return nil, fmt.Errorf("no valid segments found in '%s'", s)
	}

	return segments, nil
}

type QueryRun struct {
	Options  []runner.Option
	Duration time.Duration
	Type     string
}

func QueryRateOptions(rpsStr string, benchmarkDuration time.Duration, rampDuration time.Duration) []QueryRun {
	// Try to parse as load segments first (e.g., "1h@1000,30m@2000")
	segments, err := ParseLoadSegments(rpsStr)
	if err != nil {
		fmt.Printf("Failed to parse load segments: %v\n", err)
		os.Exit(1)
	}

	// If we found segments, use SegmentedPacer
	if segments != nil {
		pacer := &load.SegmentedPacer{
			Segments: segments,
		}

		// Calculate total duration and requests
		var totalDuration time.Duration
		var totalRequests uint
		for _, seg := range segments {
			totalDuration += seg.Duration
			totalRequests += uint(seg.RPS * seg.Duration.Seconds())
		}

		return []QueryRun{
			{
				Options: []runner.Option{
					runner.WithPacer(pacer),
					runner.WithTotalRequests(totalRequests),
				},
				Duration: totalDuration,
				Type:     fmt.Sprintf("Segmented load: %d segments over %s [%d total queries]", len(segments), totalDuration, totalRequests),
			},
		}
	}

	// Otherwise parse as a simple integer RPS
	rps, err := strconv.ParseUint(rpsStr, 10, 64)
	if err != nil {
		fmt.Printf("Failed to parse RPS value '%s': %v\n", rpsStr, err)
		os.Exit(1)
	}

	queryRunOptions := []runner.Option{
		runner.WithRPS(uint(rps)),
	}
	var durationSeconds uint
	durationSeconds = uint(math.Ceil(float64(benchmarkDuration / time.Second)))
	totalRequests := uint(float64(durationSeconds) * float64(rps))

	if rampDuration != time.Duration(0) {
		rampDurationSeconds := uint(math.Floor(float64(rampDuration / time.Second)))

		step := uint(math.Floor(float64(rps) / float64(rampDurationSeconds)))
		if step == 0 {
			return []QueryRun{
				{
					Options: slices.Concat(
						queryRunOptions,
						[]runner.Option{
							runner.WithTotalRequests(totalRequests),
							runner.WithRPS(uint(rps)),
						},
					),
					Duration: benchmarkDuration,
					Type:     fmt.Sprintf("Running for %s at %d requests/second [%d queries]", benchmarkDuration, rps, totalRequests),
				},
			}

		}
		loadEnd := step * rampDurationSeconds
		numWarmUpQueries := (rampDurationSeconds / 2) * (2*DefaultLoadRampStart + (rampDurationSeconds-1)*step)
		return []QueryRun{
			{
				Options: slices.Concat(
					queryRunOptions,
					[]runner.Option{
						runner.WithLoadSchedule("line"),
						runner.WithLoadStart(DefaultLoadRampStart),
						runner.WithLoadEnd(loadEnd),
						runner.WithLoadStep(int(step)),
						runner.WithSkipFirst(numWarmUpQueries),
						runner.WithTotalRequests(totalRequests + numWarmUpQueries),
					},
				),
				Duration: benchmarkDuration,
				Type:     fmt.Sprintf("Running for %s at %d requests/second [%d queries]", benchmarkDuration, rps, totalRequests),
			},
		}
	}
	return []QueryRun{
		{
			Options: append(
				queryRunOptions,
				runner.WithTotalRequests(totalRequests),
			),
			Duration: benchmarkDuration,
			Type:     fmt.Sprintf("Running for %s at %d requests/second [%d queries]", benchmarkDuration, rps, totalRequests),
		},
	}
}
