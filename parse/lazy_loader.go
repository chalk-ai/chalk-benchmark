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
	mu               sync.RWMutex
	batches          []arrow.Record // Circular buffer of records (fixed size)
	batchIpcBytes    [][]byte       // IPC bytes for each batch (fixed size)
	bufferStart      int            // Global index of first batch in buffer
	nextLoadIndex    int            // Next global batch index to load
	totalBatches     int            // Total number of batches (-1 = unknown)
	lastConsumedIndex int           // Last batch index that was consumed via GetBatch

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
		filePath:          filePath,
		chunkSize:         chunkSize,
		bufferSize:        bufferSize,
		batches:           make([]arrow.Record, bufferSize), // Pre-allocate fixed size
		batchIpcBytes:     make([][]byte, bufferSize),       // Pre-allocate fixed size
		bufferStart:       0,
		nextLoadIndex:     0,
		totalBatches:      -1, // Unknown until we finish first pass
		lastConsumedIndex: -1, // No batches consumed yet
		reader:            reader,
		rgReader:          rgReader,
		parquetFile:       file,
		stopChan:          make(chan struct{}),
		errChan:           make(chan error, 10),
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
// and discovers the total number of batches
func (l *LazyBatchLoader) loadInitialBatches() error {
	fmt.Println("Starting warmup phase...")
	batchesLoaded := 0

	// First, load up to bufferSize batches into the buffer
	for batchesLoaded < l.bufferSize {
		if !l.rgReader.Next() {
			// Hit EOF before filling buffer - we have everything
			l.totalBatches = batchesLoaded
			fmt.Printf("Loaded all %d batches from parquet file (fits in buffer)\n", l.totalBatches)

			if batchesLoaded == 0 {
				return fmt.Errorf("no batches found in parquet file")
			}

			fmt.Println("warmup completed")
			return nil
		}

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

	fmt.Printf("Loaded initial %d batches into circular buffer\n", batchesLoaded)

	// Calculate totalBatches from parquet metadata (fast!)
	fmt.Println("Calculating total batch count from metadata...")
	metadata := l.parquetFile.MetaData()
	totalRows := metadata.NumRows
	batchSize := l.chunkSize
	if batchSize <= 0 {
		batchSize = 1 << 30 // 1 billion rows
	}

	// Calculate number of batches (ceiling division)
	l.totalBatches = int((totalRows + batchSize - 1) / batchSize)
	fmt.Printf("Calculated %d total batches (%d rows, batch size %d)\n", l.totalBatches, totalRows, batchSize)

	// Reset reader for background loading to continue from where buffer left off
	l.rgReader.Release()
	rgReader, err := l.reader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return fmt.Errorf("failed to reset record reader: %w", err)
	}
	l.rgReader = rgReader

	// Skip to where we left off (the batches already loaded into buffer)
	for i := 0; i < batchesLoaded; i++ {
		if !l.rgReader.Next() {
			return fmt.Errorf("failed to skip to buffer position %d", i)
		}
	}

	fmt.Println("warmup completed")
	return nil
}

// backgroundLoader continuously loads batches ahead of the current position
// Runs as fast as possible to keep buffer full, yielding only when buffer is full
func (l *LazyBatchLoader) backgroundLoader() {
	consecutiveIdle := 0

	for {
		select {
		case <-l.stopChan:
			return
		default:
			// Try to load next batch
			err := l.loadNextBatch()

			if err != nil {
				select {
				case l.errChan <- err:
				default:
					// Error channel full, drop error
				}
			}

			// Check if we're idle (buffer full or caught up)
			l.mu.Lock()
			isIdle := (l.totalBatches > 0 && l.nextLoadIndex >= l.totalBatches+l.bufferSize) ||
				(l.nextLoadIndex-l.bufferStart >= l.bufferSize)
			l.mu.Unlock()

			if isIdle {
				consecutiveIdle++
				// Back off exponentially when idle, up to 10ms
				if consecutiveIdle > 10 {
					time.Sleep(10 * time.Millisecond)
				} else {
					time.Sleep(time.Duration(consecutiveIdle) * time.Millisecond)
				}
			} else {
				consecutiveIdle = 0
				// Yield briefly to let GetBatch() calls through
				time.Sleep(10 * time.Microsecond)
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

	// Don't load if the buffer is full and we can't advance bufferStart yet
	// This prevents overwriting unconsumed batches
	if l.nextLoadIndex-l.bufferStart >= l.bufferSize {
		// Buffer is full, wait for consumption to catch up
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

	// Only advance bufferStart if we have consumed batches beyond the current window
	// This prevents evicting unconsumed batches
	minRequiredStart := l.nextLoadIndex - l.bufferSize
	if minRequiredStart > l.bufferStart {
		// Only advance if we've consumed at least up to the new buffer start position
		// This ensures we don't evict batches that haven't been requested yet
		if l.lastConsumedIndex >= minRequiredStart - 1 {
			l.bufferStart = minRequiredStart
		}
	}

	return nil
}

// GetBatch returns the batch at the specified global index.
// This should be fast - just array access, no blocking I/O.
func (l *LazyBatchLoader) GetBatch(globalIndex int) ([]byte, arrow.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If we know total batches, normalize the index
	actualIndex := globalIndex
	if l.totalBatches > 0 {
		actualIndex = globalIndex % l.totalBatches
	}

	// Check if batch is in buffer
	if actualIndex >= l.bufferStart && actualIndex < l.nextLoadIndex {
		bufferPos := actualIndex % l.bufferSize

		// Track consumption to prevent premature eviction
		if actualIndex > l.lastConsumedIndex {
			l.lastConsumedIndex = actualIndex
		}

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
