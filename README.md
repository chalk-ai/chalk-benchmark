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
2. 
