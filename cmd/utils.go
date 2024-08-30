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

type RampUp struct {
	rampStep int
	ramp
	NumWarmUpQueries int
}

func QueryRateOptions(rps int, duration time.Duration, benchmarkDuration time.Duration, rampDuration time.Duration, totalRequests int) []runner.Option {
	queryRunOptions := []runner.Option{
		runner.WithRPS(uint(rps)),
	}
	var totalRequests uint
	if benchmarkDuration != time.Duration(0) {
		durationSeconds := float64(benchmarkDuration / time.Second)
		totalRequests = uint(durationSeconds * float64(rps))
	} else {
		rps = totalRequests / int(duration.Seconds())
	}
	if rampDuration != time.Duration(0) {
		step := uint(math.Floor(float64(rps) / float64(rampDurationSeconds)))
		loadEnd := step * rampDurationSeconds

		numWarmUpQueries := (rampDurationSeconds / 2) * (2*DefaultLoadRampStart + (rampDurationSeconds-1)*step)
		totalRequests := uint(float64(rps) * float64(benchmarkDuration/time.Second))
		return slices.Concat(
			queryRunOptions,
			[]runner.Option{
				runner.WithLoadSchedule("line"),
				runner.WithLoadStart(DefaultLoadRampStart),
				runner.WithLoadEnd(loadEnd),
				runner.WithLoadStep(int(step)),
				runner.WithSkipFirst(numWarmUpQueries),
			},
		)
	}
	return queryRunOptions
}
