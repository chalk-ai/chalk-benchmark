package cmd

import (
	"github.com/chalk-ai/ghz/runner"
	_ "github.com/goccy/go-json"
	"math"
	"os"
	"slices"
	"time"
)

func CurDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return cwd
}

func QueryRateOptions(rps uint, benchmarkDuration time.Duration, rampDuration time.Duration, totalRequests uint) []runner.Option {
	queryRunOptions := []runner.Option{
		runner.WithRPS(rps),
	}
	var durationSeconds uint
	if totalRequests == 0 {
		durationSeconds = uint(math.Ceil(float64(benchmarkDuration / time.Second)))
		totalRequests = uint(float64(durationSeconds * rps))
	} else {
		durationSeconds = uint(math.Ceil(float64(totalRequests) / float64(rps)))
	}
	if rampDuration != time.Duration(0) {
		rampDurationSeconds := uint(math.Floor(float64(rampDuration / time.Second)))

		step := uint(math.Floor(float64(rps) / float64(rampDurationSeconds)))
		loadEnd := step * rampDurationSeconds
		numWarmUpQueries := (rampDurationSeconds / 2) * (2*DefaultLoadRampStart + (rampDurationSeconds-1)*step)
		return slices.Concat(
			queryRunOptions,
			[]runner.Option{
				runner.WithLoadSchedule("line"),
				runner.WithLoadStart(DefaultLoadRampStart),
				runner.WithLoadEnd(loadEnd),
				runner.WithLoadStep(int(step)),
				runner.WithSkipFirst(numWarmUpQueries),
				runner.WithTotalRequests(totalRequests + numWarmUpQueries),
			},
		)
	}
	return append(
		queryRunOptions,
		runner.WithTotalRequests(totalRequests),
	)
}
