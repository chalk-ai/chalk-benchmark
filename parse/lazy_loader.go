package parse

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	parquetFile "github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

// LazyBatchLoader provides lazy loading of parquet batches with a circular buffer
// and background loading to prevent blocking on the hot path.
type LazyBatchLoader struct {
	filePath   string
	chunkSize  int64
	bufferSize int

	// State
	mu            sync.RWMutex
	batches       []arrow.Record // Circular buffer of records (fixed size)
	batchIpcBytes [][]byte       // IPC bytes for each batch (fixed size)
	bufferStart   int            // Global index of first batch in buffer
	nextLoadIndex int            // Next global batch index to load
	totalBatches  int            // Total number of batches (-1 = unknown)

	// Parquet reader state
	reader      *pqarrow.FileReader
	rgReader    pqarrow.RecordReader
	parquetFile *parquetFile.Reader

	// Background loading
	stopChan chan struct{}
	errChan  chan error
}

// NewLazyBatchLoader creates a new lazy batch loader with a circular buffer.
// bufferSize determines how many batches to keep in memory.
func NewLazyBatchLoader(filePath string, chunkSize int64, bufferSize int) (*LazyBatchLoader, error) {
	file, err := parquetFile.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Set batch size
	props := pqarrow.ArrowReadProperties{}
	if chunkSize > 0 {
		props.BatchSize = chunkSize
	} else {
		props.BatchSize = 1 << 30 // 1 billion rows
	}

	reader, err := pqarrow.NewFileReader(file, props, memory.DefaultAllocator)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}

	rgReader, err := reader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get record reader: %w", err)
	}

	// Use a reasonable default buffer size if not specified
	if bufferSize <= 0 {
		bufferSize = 100 // Default to 100 batches
	}

	loader := &LazyBatchLoader{
		filePath:      filePath,
		chunkSize:     chunkSize,
		bufferSize:    bufferSize,
		batches:       make([]arrow.Record, bufferSize), // Pre-allocate fixed size
		batchIpcBytes: make([][]byte, bufferSize),       // Pre-allocate fixed size
		bufferStart:   0,
		nextLoadIndex: 0,
		totalBatches:  -1, // Unknown until we finish first pass
		reader:        reader,
		rgReader:      rgReader,
		parquetFile:   file,
		stopChan:      make(chan struct{}),
		errChan:       make(chan error, 10),
	}

	// Pre-load initial batches synchronously
	if err := loader.loadInitialBatches(); err != nil {
		loader.Close()
		return nil, fmt.Errorf("failed to load initial batches: %w", err)
	}

	// Start background loading
	go loader.backgroundLoader()

	return loader, nil
}

// loadInitialBatches loads the first buffer of batches synchronously
func (l *LazyBatchLoader) loadInitialBatches() error {
	batchesLoaded := 0
	for batchesLoaded < l.bufferSize && l.rgReader.Next() {
		record := l.rgReader.Record()
		record.Retain()

		ipcBytes, err := RecordToBytes(record)
		if err != nil {
			record.Release()
			return fmt.Errorf("failed to convert record to bytes: %w", err)
		}

		// Store in circular buffer
		bufferPos := batchesLoaded % l.bufferSize
		l.batches[bufferPos] = record
		l.batchIpcBytes[bufferPos] = ipcBytes

		batchesLoaded++
		l.nextLoadIndex++
	}

	// Check if we've loaded everything
	if !l.rgReader.Next() {
		l.totalBatches = batchesLoaded
		fmt.Printf("Loaded all %d batches from parquet file (fits in buffer)\n", l.totalBatches)
	} else {
		fmt.Printf("Loaded initial %d batches into circular buffer\n", batchesLoaded)
	}

	if batchesLoaded == 0 {
		return fmt.Errorf("no batches found in parquet file")
	}

	return nil
}

// backgroundLoader continuously loads batches ahead of the current position
func (l *LazyBatchLoader) backgroundLoader() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopChan:
			return
		case <-ticker.C:
			// Try to keep the buffer full
			if err := l.loadNextBatch(); err != nil {
				select {
				case l.errChan <- err:
				default:
					// Error channel full, drop error
				}
			}
		}
	}
}

// loadNextBatch loads the next batch from the file
func (l *LazyBatchLoader) loadNextBatch() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If we know total batches and we're caught up, we're done
	if l.totalBatches > 0 && l.nextLoadIndex >= l.totalBatches+l.bufferSize {
		// We've loaded a full cycle, just idle
		return nil
	}

	// Try to read next batch
	if !l.rgReader.Next() {
		// Hit EOF
		if l.totalBatches < 0 {
			// First time hitting EOF, now we know the total
			l.totalBatches = l.nextLoadIndex
			fmt.Printf("Discovered %d total batches in parquet file\n", l.totalBatches)
		}

		// Reset reader to loop back
		l.rgReader.Release()
		rgReader, err := l.reader.GetRecordReader(context.Background(), nil, nil)
		if err != nil {
			return fmt.Errorf("failed to reset record reader: %w", err)
		}
		l.rgReader = rgReader

		// Try reading first batch of new cycle
		if !l.rgReader.Next() {
			return fmt.Errorf("failed to read batch after reset")
		}
	}

	record := l.rgReader.Record()
	record.Retain()

	ipcBytes, err := RecordToBytes(record)
	if err != nil {
		record.Release()
		return fmt.Errorf("failed to convert record to bytes: %w", err)
	}

	// Calculate buffer position
	bufferPos := l.nextLoadIndex % l.bufferSize

	// Release old record if it exists
	if l.batches[bufferPos] != nil {
		l.batches[bufferPos].Release()
	}

	// Store new batch
	l.batches[bufferPos] = record
	l.batchIpcBytes[bufferPos] = ipcBytes

	// Update indices
	l.nextLoadIndex++
	if l.nextLoadIndex-l.bufferSize > l.bufferStart {
		l.bufferStart = l.nextLoadIndex - l.bufferSize
	}

	return nil
}

// GetBatch returns the batch at the specified global index.
// This should be fast - just array access, no blocking I/O.
func (l *LazyBatchLoader) GetBatch(globalIndex int) ([]byte, arrow.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// If we know total batches, normalize the index
	actualIndex := globalIndex
	if l.totalBatches > 0 {
		actualIndex = globalIndex % l.totalBatches
	}

	// Check if batch is in buffer
	if actualIndex >= l.bufferStart && actualIndex < l.nextLoadIndex {
		bufferPos := actualIndex % l.bufferSize
		return l.batchIpcBytes[bufferPos], l.batches[bufferPos], nil
	}

	// Batch not in buffer - this shouldn't happen if background loader is keeping up
	return nil, nil, fmt.Errorf("batch %d (actual: %d) not in buffer [%d, %d)",
		globalIndex, actualIndex, l.bufferStart, l.nextLoadIndex)
}

// GetBatchCount returns the total number of batches
func (l *LazyBatchLoader) GetBatchCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.totalBatches > 0 {
		return l.totalBatches
	}
	return l.nextLoadIndex // Return what we have so far
}

// Close releases all resources
func (l *LazyBatchLoader) Close() error {
	// Stop background loader
	close(l.stopChan)

	l.mu.Lock()
	defer l.mu.Unlock()

	// Release all records in the buffer
	for i := 0; i < l.bufferSize; i++ {
		if l.batches[i] != nil {
			l.batches[i].Release()
			l.batches[i] = nil
		}
	}
	l.batchIpcBytes = nil

	// Close readers
	if l.rgReader != nil {
		l.rgReader.Release()
		l.rgReader = nil
	}

	if l.parquetFile != nil {
		err := l.parquetFile.Close()
		l.parquetFile = nil
		return err
	}

	return nil
}
