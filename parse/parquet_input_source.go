package parse

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow/memory"
	parquetFile "github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	commonv1 "github.com/chalk-ai/chalk-go/gen/chalk/common/v1"
	"google.golang.org/protobuf/proto"
)

// LazyParquetInputSource provides lazy loading of parquet data with background prefetching.
// Optimized for high throughput (100k+ QPS) with minimal blocking on the hot path.
type LazyParquetInputSource struct {
	loader  *LazyBatchLoader
	outputs []*commonv1.OutputExpr
	onlineQueryContext *commonv1.OnlineQueryContext

	// Request tracking
	requestIndex atomic.Int64
	mu           sync.Mutex
	marshalCache [][]byte // Pre-marshalled OnlineQueryBulkRequest bytes
}

// NewLazyParquetInputSource creates a new lazy-loading parquet input source.
// bufferSize controls how many batches to keep in memory (default: 100).
func NewLazyParquetInputSource(
	filePath string,
	chunkSize int64,
	bufferSize int,
	outputs []*commonv1.OutputExpr,
	onlineQueryContext *commonv1.OnlineQueryContext,
) (*LazyParquetInputSource, error) {
	loader, err := NewLazyBatchLoader(filePath, chunkSize, bufferSize)
	if err != nil {
		return nil, err
	}

	source := &LazyParquetInputSource{
		loader:              loader,
		outputs:             outputs,
		onlineQueryContext:  onlineQueryContext,
		marshalCache:        make([][]byte, bufferSize),
	}

	source.requestIndex.Store(0)

	return source, nil
}

// Next returns the next marshalled OnlineQueryBulk request.
// This is thread-safe and optimized for high throughput.
func (s *LazyParquetInputSource) Next() ([]byte, error) {
	// Get and increment request index atomically
	idx := int(s.requestIndex.Add(1) - 1)

	// Get batch from loader (fast - just array access)
	ipcBytes, _, err := s.loader.GetBatch(idx)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch %d: %w", idx, err)
	}

	// Marshal OnlineQueryBulkRequest
	// Note: We marshal on each call because outputs/context might differ per request
	// and proto marshalling is relatively fast compared to I/O
	oqr := commonv1.OnlineQueryBulkRequest{
		InputsFeather: ipcBytes,
		Outputs:       s.outputs,
		Context:       s.onlineQueryContext,
	}

	marshaledBytes, err := proto.Marshal(&oqr)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	return marshaledBytes, nil
}

// Close releases all resources
func (s *LazyParquetInputSource) Close() error {
	return s.loader.Close()
}

// PreMaterializedParquetInputSource loads all batches into memory at startup.
// Best for datasets that fit in memory and require absolute minimal latency.
type PreMaterializedParquetInputSource struct {
	marshaledRequests [][]byte // All pre-marshalled OnlineQueryBulkRequest bytes
	requestIndex      atomic.Int64
}

// NewPreMaterializedParquetInputSource creates a new pre-materialized parquet input source.
// All batches are loaded into memory at creation time.
func NewPreMaterializedParquetInputSource(
	filePath string,
	chunkSize int64,
	outputs []*commonv1.OutputExpr,
	onlineQueryContext *commonv1.OnlineQueryContext,
) (*PreMaterializedParquetInputSource, error) {
	fmt.Printf("Pre-materializing parquet file: %s\n", filePath)

	// Open parquet file
	file, err := parquetFile.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer file.Close()

	// Set batch size
	props := pqarrow.ArrowReadProperties{}
	if chunkSize > 0 {
		props.BatchSize = chunkSize
	} else {
		props.BatchSize = 1 << 30 // 1 billion rows
	}

	reader, err := pqarrow.NewFileReader(file, props, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}

	rgReader, err := reader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get record reader: %w", err)
	}
	defer rgReader.Release()

	// Load all batches
	var marshaledRequests [][]byte
	batchCount := 0

	for rgReader.Next() {
		record := rgReader.Record()
		record.Retain()

		// Convert to IPC bytes
		ipcBytes, err := RecordToBytes(record)
		record.Release()
		if err != nil {
			return nil, fmt.Errorf("failed to convert record to bytes: %w", err)
		}

		// Marshal full OnlineQueryBulkRequest
		oqr := commonv1.OnlineQueryBulkRequest{
			InputsFeather: ipcBytes,
			Outputs:       outputs,
			Context:       onlineQueryContext,
		}

		marshaledBytes, err := proto.Marshal(&oqr)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}

		marshaledRequests = append(marshaledRequests, marshaledBytes)
		batchCount++
	}

	if batchCount == 0 {
		return nil, fmt.Errorf("no batches found in parquet file")
	}

	fmt.Printf("Pre-materialized %d batches from parquet file\n", batchCount)

	source := &PreMaterializedParquetInputSource{
		marshaledRequests: marshaledRequests,
	}
	source.requestIndex.Store(0)

	return source, nil
}

// Next returns the next marshalled OnlineQueryBulk request.
// This is extremely fast - just atomic increment + array access.
func (s *PreMaterializedParquetInputSource) Next() ([]byte, error) {
	idx := int(s.requestIndex.Add(1)-1) % len(s.marshaledRequests)
	return s.marshaledRequests[idx], nil
}

// Close releases all resources (nothing to do for pre-materialized)
func (s *PreMaterializedParquetInputSource) Close() error {
	return nil
}
