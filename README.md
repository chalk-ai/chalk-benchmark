# chalk-benchmark

This repository contains tooling for benchmarking your Chalk queries. In order to benchmark
your Chalk query, you must have

* a deployed Chalk environment provisioned with a GRPC Query Server
* permissions to run queries within the Chalk environment
* a Chalk query that you would like to benchmark

We use [ghz](https://github.com/bojand/ghz) to benchmark Chalk GRPC queries. Ghz enables us to
adjust common benchmarking parameters such as requests per second and total requests.

## Instructions

1. Clone this repository
2. Update the global variables in `benchmark/benchmark.go` based on the query you would like to benchmark.
3. Run `go run benchmark/benchmark.go` to start the benchmarking process.
4. Review the benchmark results. The script is set to print a report in your terminal and output an HTML rendering of
   the benchmark report. 
