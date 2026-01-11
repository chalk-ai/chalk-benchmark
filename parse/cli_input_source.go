package parse

import (
	"fmt"
	"sync/atomic"

	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"google.golang.org/protobuf/proto"
)

// CLIInputSource provides input from CLI arguments.
// All batches are pre-materialized at creation time.
type CLIInputSource struct {
	marshaledRequests [][]byte // All pre-marshalled OnlineQueryBulkRequest bytes
	requestIndex      atomic.Int64
}

// NewCLIInputSource creates a new CLI input source.
// The CLI inputs are parsed and all batches are converted to marshalled OnlineQueryBulkRequest bytes.
func NewCLIInputSource(
	inputStr map[string]string,
	inputNum map[string]int64,
	input map[string]string,
	chunkSize int64,
	outputs []*commonv1.OutputExpr,
	context *commonv1.OnlineQueryContext,
) (*CLIInputSource, error) {
	fmt.Printf("Loading CLI inputs\n")

	// Use existing ProcessInputs to get IPC bytes for each batch
	batchIPCBytes := ProcessInputs(inputStr, inputNum, input, chunkSize)

	if len(batchIPCBytes) == 0 {
		return nil, fmt.Errorf("no batches created from CLI inputs")
	}

	// Pre-marshal all OnlineQueryBulkRequest messages
	marshaledRequests := make([][]byte, len(batchIPCBytes))
	for i, ipcBytes := range batchIPCBytes {
		oqr := commonv1.OnlineQueryBulkRequest{
			InputsFeather: ipcBytes,
			Outputs:       outputs,
			Context:       context,
		}

		marshaledBytes, err := proto.Marshal(&oqr)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request %d: %w", i, err)
		}

		marshaledRequests[i] = marshaledBytes
	}

	fmt.Printf("Pre-materialized %d batches from CLI inputs\n", len(marshaledRequests))

	source := &CLIInputSource{
		marshaledRequests: marshaledRequests,
	}
	source.requestIndex.Store(0)

	return source, nil
}

// Next returns the next marshalled OnlineQueryBulk request.
// This is thread-safe and cycles through available batches.
func (s *CLIInputSource) Next() ([]byte, error) {
	idx := int(s.requestIndex.Add(1)-1) % len(s.marshaledRequests)
	return s.marshaledRequests[idx], nil
}

// Close releases all resources (nothing to do for CLI)
func (s *CLIInputSource) Close() error {
	return nil
}

// NewCLIInputSourceFromBytes creates a new CLI input source from pre-marshalled IPC bytes.
// This is useful when you already have IPC bytes (e.g., from JSON inputs) and just need
// to wrap them with outputs and context into OnlineQueryBulkRequest messages.
func NewCLIInputSourceFromBytes(
	batchIPCBytes [][]byte,
	outputs []*commonv1.OutputExpr,
	context *commonv1.OnlineQueryContext,
) (*CLIInputSource, error) {
	if len(batchIPCBytes) == 0 {
		return nil, fmt.Errorf("no batches provided")
	}

	fmt.Printf("Creating input source from %d pre-encoded batches\n", len(batchIPCBytes))

	// Pre-marshal all OnlineQueryBulkRequest messages
	marshaledRequests := make([][]byte, len(batchIPCBytes))
	for i, ipcBytes := range batchIPCBytes {
		oqr := commonv1.OnlineQueryBulkRequest{
			InputsFeather: ipcBytes,
			Outputs:       outputs,
			Context:       context,
		}

		marshaledBytes, err := proto.Marshal(&oqr)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request %d: %w", i, err)
		}

		marshaledRequests[i] = marshaledBytes
	}

	source := &CLIInputSource{
		marshaledRequests: marshaledRequests,
	}
	source.requestIndex.Store(0)

	return source, nil
}
