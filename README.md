# chalk-benchmark

This repository contains tooling for benchmarking your Chalk queries. To benchmark
your Chalk query, you must have:

- a deployed Chalk environment provisioned with a GRPC Query Server,
- permission to run queries and create tokens within the Chalk environment,
- a Chalk query that you would like to benchmark.

We use [ghz](https://github.com/bojand/ghz) to benchmark Chalk GRPC queries. Ghz enables us to
adjust common benchmarking parameters such as requests per second and total requests.

## Downloading 

To run Chalk's GRPC benchmarking tool, download the relevant release for your machine from the [releases](https://github.com/chalk-ai/chalk-benchmark/releases) page of the repository. Alternatively, you can clone the repository and [build the binary from source](#building-from-source).

## Instructions

First, download the binary above (or clone this repo and build from source as mentioned below)

The binary can be run as follows:

```sh
chalk-benchmark \
  --client_id <client_id> \
  --client_secret <client_secret> \
  --in_num 'user.id=1' \
  --out 'user.name' \
  --rps=1 --duration=10s
```

For more information on the options available, run `chalk-benchmark --help`.

The benchmarking tool outputs an HTML file with the benchmark result. The file will be saved in the current directory. 

## Building From Source

1. Clone this repository: `git clone git@github.com:chalk-ai/chalk-benchmark.git`
2. Run `go build`
