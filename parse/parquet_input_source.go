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

// QueuedLazyParquetInputSource uses a buffered channel as a queue,
// with a background producer goroutine that keeps it filled with pre-marshaled requests.
// This gives the memory efficiency of lazy loading with near-zero latency on Next() calls.
type QueuedLazyParquetInputSource struct {
	loader             *LazyBatchLoader
	outputs            []*commonv1.OutputExpr
	onlineQueryContext *commonv1.OnlineQueryContext

	// Queue of pre-marshaled requests
	queue chan []byte

	// Control channels
	stopChan chan struct{}
	errChan  chan error
	wg       sync.WaitGroup
}

// NewQueuedLazyParquetInputSource creates a new queued lazy-loading parquet input source.
// queueSize controls how many pre-marshaled requests to buffer (recommended: 10000-20000 for 10k+ QPS).
// bufferSize controls how many parquet batches the loader keeps in memory.
func NewQueuedLazyParquetInputSource(
	filePath string,
	chunkSize int64,
	bufferSize int,
	queueSize int,
	outputs []*commonv1.OutputExpr,
	onlineQueryContext *commonv1.OnlineQueryContext,
) (*QueuedLazyParquetInputSource, error) {
	loader, err := NewLazyBatchLoader(filePath, chunkSize, bufferSize)
	if err != nil {
		return nil, err
	}

	if queueSize <= 0 {
		queueSize = 15000 // Default: buffer ~1.5s at 10k QPS
	}

	source := &QueuedLazyParquetInputSource{
		loader:             loader,
		outputs:            outputs,
		onlineQueryContext: onlineQueryContext,
		queue:              make(chan []byte, queueSize),
		stopChan:           make(chan struct{}),
		errChan:            make(chan error, 10),
	}

	// Start producer goroutine
	source.wg.Add(1)
	go source.producer()

	return source, nil
}

// producer continuously loads batches, marshals them, and feeds the queue.
// Runs in a background goroutine to keep the queue full.
func (s *QueuedLazyParquetInputSource) producer() {
	defer s.wg.Done()

	batchIndex := 0
	totalBatches := s.loader.GetBatchCount()

	for {
		select {
		case <-s.stopChan:
			return
		default:
			// Get batch from loader (fast - just array access)
			ipcBytes, _, err := s.loader.GetBatch(batchIndex)
			if err != nil {
				// Batch not ready yet or error - back off briefly
				select {
				case s.errChan <- fmt.Errorf("producer failed to get batch %d: %w", batchIndex, err):
				default:
				}
				continue
			}

			// Marshal the request (this is the expensive part we're doing off the hot path)
			oqr := commonv1.OnlineQueryBulkRequest{
				InputsFeather: ipcBytes,
				Outputs:       s.outputs,
				Context:       s.onlineQueryContext,
			}

			marshaledBytes, err := proto.Marshal(&oqr)
			if err != nil {
				select {
				case s.errChan <- fmt.Errorf("producer failed to marshal request: %w", err):
				default:
				}
				continue
			}

			// Send to queue (blocks if queue is full - this is the backpressure mechanism)
			select {
			case s.queue <- marshaledBytes:
				// Successfully queued, advance to next batch
				batchIndex++
				if totalBatches > 0 && batchIndex >= totalBatches {
					// Wrap around to beginning for infinite cycling
					batchIndex = 0
				}
			case <-s.stopChan:
				return
			}
		}
	}
}

// Next returns the next marshalled OnlineQueryBulk request.
// This is extremely fast - just a channel receive, no marshaling or computation.
// Thread-safe and lock-free.
func (s *QueuedLazyParquetInputSource) Next() ([]byte, error) {
	select {
	case msg := <-s.queue:
		return msg, nil
	case <-s.stopChan:
		return nil, fmt.Errorf("input source closed")
	}
}

// Close releases all resources
func (s *QueuedLazyParquetInputSource) Close() error {
	close(s.stopChan)
	s.wg.Wait()
	close(s.queue)
	return s.loader.Close()
}
