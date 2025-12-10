package parse

import (
	"context"
	"github.com/apache/arrow/go/v17/arrow/memory"
	parquetFile "github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"testing"
)

func TestParquetInputFile(t *testing.T) {
	_, err := ReadParquetFile("../testdata/test.parquet", 1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParquetChunkSize(t *testing.T) {
	// First, get the total number of rows in the test file
	file, err := parquetFile.OpenParquetFile("../testdata/test.parquet", false)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	reader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}

	table, err := reader.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer table.Release()

	totalRows := table.NumRows()
	if totalRows == 0 {
		t.Skip("Test file has no rows, skipping chunk size test")
	}

	// Test different chunk sizes
	tests := []struct {
		name            string
		chunkSize       int64
		expectedChunks  int
		skipChunkCount  bool // Skip chunk count validation (for negative/zero chunk sizes)
	}{
		{
			name:           "chunk size 1",
			chunkSize:      1,
			expectedChunks: int(totalRows),
		},
		{
			name:           "chunk size 2",
			chunkSize:      2,
			expectedChunks: int((totalRows + 1) / 2), // ceil division
		},
		{
			name:           "chunk size equals total rows",
			chunkSize:      totalRows,
			expectedChunks: 1,
		},
		{
			name:           "chunk size larger than total rows",
			chunkSize:      totalRows * 2,
			expectedChunks: 1,
		},
		{
			name:           "negative chunk size (uses default batch size)",
			chunkSize:      -1,
			skipChunkCount: true, // Default batch size may vary
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks, err := ReadParquetFile("../testdata/test.parquet", tt.chunkSize)
			if err != nil {
				t.Fatal(err)
			}

			if !tt.skipChunkCount && len(chunks) != tt.expectedChunks {
				t.Errorf("expected %d chunks, got %d", tt.expectedChunks, len(chunks))
			}

			// Verify that chunks are not empty
			if len(chunks) == 0 {
				t.Error("expected at least one chunk, got zero")
			}

			// Verify that each chunk contains data
			for i, chunk := range chunks {
				if len(chunk) == 0 {
					t.Errorf("chunk %d is empty", i)
				}
			}
		})
	}
}

func TestIterateParquetFileChunking(t *testing.T) {
	// Get total rows first
	file, err := parquetFile.OpenParquetFile("../testdata/test.parquet", false)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	reader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}

	table, err := reader.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer table.Release()

	totalRows := table.NumRows()
	if totalRows == 0 {
		t.Skip("Test file has no rows, skipping chunk size test")
	}

	// Test with chunk size 2
	chunkSize := int64(2)
	expectedChunks := int((totalRows + 1) / 2)

	chunkCount := 0
	for chunk, err := range IterateParquetFile("../testdata/test.parquet", chunkSize) {
		if err != nil {
			t.Fatal(err)
		}
		if len(chunk) == 0 {
			t.Errorf("chunk %d is empty", chunkCount)
		}
		chunkCount++
	}

	if chunkCount != expectedChunks {
		t.Errorf("expected %d chunks, got %d", expectedChunks, chunkCount)
	}
}
