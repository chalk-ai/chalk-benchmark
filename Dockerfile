FROM --platform=linux/amd64 golang:1.23-alpine AS builder

RUN mkdir -p /app/chalk-benchmark/
WORKDIR /app/chalk-benchmark
COPY go.mod go.sum ./
# This requies buildkit
RUN go env -w GOCACHE=/go-cache
RUN go env -w GOMODCACHE=/gomod-cache
RUN --mount=type=cache,target=/gomod-cache go mod download

COPY . .

RUN --mount=type=cache,target=/gomod-cache \
    --mount=type=cache,target=/go-cache \
    CGO_ENABLED=0 GOOS=linux \
    go build \
    -ldflags="-X 'github.com/chalk-ai/chalk-private/chalk-benchmark/info.Sha=$COMMIT_SHA'"

# Run stage
FROM --platform=linux/amd64 alpine

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the built binary from the builder stage
COPY --from=builder /app/chalk-benchmark /usr/local/bin/chalk-benchmark

# Set the working directory
WORKDIR /app

CMD ["/usr/local/bin/chalk-benchmark", "grpc", "--log-json"]
