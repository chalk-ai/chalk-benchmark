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
2. Run `go build`
3. To benchmark, run:
  `chalk-benchmark --client_id <client_id> --client_secret <client_secret> --in_num 'user.id=1' --out 'user.name' --rps=1 --duration=5`
4. Review the benchmark results. The script is set to print a report in your terminal and output an HTML rendering of
   the benchmark report. 
