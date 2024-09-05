package cmd

import (
	"fmt"
	"github.com/chalk-ai/ghz/runner"
	"github.com/goccy/go-json"
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

type StepPacer struct {
	StepSize     int64  `json:"step_size"`
	StepStart    uint64 `json:"step_start"`
	Duration     string `json:"duration"`
	StepDuration string `json:"step_duration"`
}

type ConstPacer struct {
	RPS      uint64 `json:"rps"`
	Duration string `json:"duration"`
}

type RawPipelineStep struct {
	Type   string           `json:"type"`
	Params *json.RawMessage `json:"params"`
}

func ReadJSONFile(filePath string) []byte {
	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("Failed to open schedule file with err: %v", err)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("Failed to read schedule file with err: %v", err)
		os.Exit(1)
	}
	return data
}

type QueryRun struct {
	Options  []runner.Option
	Duration time.Duration
}

func ParseScheduleFile(scheduleFile string) []QueryRun {
	var queryRuns []QueryRun
	jsonFileData := ReadJSONFile(scheduleFile)

	var pipelineSteps []RawPipelineStep
	// defer the closing of our jsonFile so that we can parse it later on

	if err := json.Unmarshal(jsonFileData, &pipelineSteps); err != nil {
		fmt.Println("Failed to unmarshal schedule file with err: ", err)
		os.Exit(1)
	}
	for i, step := range pipelineSteps {
		switch step.Type {
		case "step":
			var sp StepPacer
			if err := json.Unmarshal(*step.Params, &sp); err != nil {
				fmt.Printf("Failed to unmarshal step pacer: %v, with err: %s", step.Params, err)
				os.Exit(1)
			}
			stepTime, err := time.ParseDuration(sp.Duration)
			if err != nil {
				fmt.Printf("Failed to parse duration of %v: %s", step.Params, sp.Duration)
				os.Exit(1)
			}
			singleStepTime, err := time.ParseDuration(sp.StepDuration)
			if err != nil {
				fmt.Printf("Failed to parse step duration of %v: %s", step.Params, sp.StepDuration)
				os.Exit(1)
			}
			numSteps := math.Ceil(float64(stepTime) / float64(singleStepTime))
			numRequests := uint(numSteps * (2*float64(sp.StepStart) + float64(sp.StepSize-1)) * float64(sp.StepSize))
			queryRuns = append(
				queryRuns,
				QueryRun{
					Options: []runner.Option{
						runner.WithTotalRequests(numRequests),
						runner.WithLoadStep(int(sp.StepSize)),
						runner.WithLoadStepDuration(singleStepTime),
						runner.WithLoadStart(uint(sp.StepStart)),
					},
					Duration: stepTime,
				},
			)
		case "const":
			var cp ConstPacer
			if err := json.Unmarshal(*step.Params, &cp); err != nil {
				fmt.Printf("Failed to unmarshal step pacer: %v, with err: %s", step.Params, err)
				os.Exit(1)
			}
			stepTime, err := time.ParseDuration(cp.Duration)
			if err != nil {
				fmt.Printf("Failed to parse duration of %v: %s", step.Params, cp.Duration)
				os.Exit(1)
			}
			if i == 0 && rampDuration != time.Duration(0) {
				queryRuns = QueryRateOptions(uint(cp.RPS), stepTime, rampDuration, 0, "")
			}
			queryRuns = append(
				queryRuns,
				QueryRun{
					Options: []runner.Option{
						runner.WithTotalRequests(uint(float64(cp.RPS) * stepTime.Seconds())),
						runner.WithRPS(uint(cp.RPS)),
					},
					Duration: stepTime,
				},
			)
		default:
			fmt.Printf("Got unknown pipeline step type: %q", step.Type)
			os.Exit(1)
		}
	}
	return queryRuns
}

func QueryRateOptions(rps uint, benchmarkDuration time.Duration, rampDuration time.Duration, totalRequests uint, scheduleFile string) []QueryRun {
	if scheduleFile != "" {
		return ParseScheduleFile(scheduleFile)
	}
	queryRunOptions := []runner.Option{
		runner.WithRPS(rps),
	}
	var durationSeconds uint
	if totalRequests == 0 {
		durationSeconds = uint(math.Ceil(float64(benchmarkDuration / time.Second)))
		totalRequests = uint(float64(durationSeconds * rps))
	} else {
		durationSeconds = uint(math.Ceil(float64(totalRequests) / float64(rps)))
		benchmarkDuration = time.Duration(durationSeconds) * time.Second
	}
	if rampDuration != time.Duration(0) {
		rampDurationSeconds := uint(math.Floor(float64(rampDuration / time.Second)))

		step := uint(math.Floor(float64(rps) / float64(rampDurationSeconds)))
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
		},
	}
}
