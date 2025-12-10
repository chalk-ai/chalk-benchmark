package parse

import (
	"context"
	"github.com/apache/arrow/go/v17/arrow/memory"
	parquetFile "github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"iter"
)

func ReadParquetFile(featuresFile string, chunkSize int64) ([][]byte, error) {
	file, err := parquetFile.OpenParquetFile(featuresFile, false)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Set batch size - if <= 0, use a large value to read all rows at once
	props := pqarrow.ArrowReadProperties{}
	if chunkSize > 0 {
		props.BatchSize = chunkSize
	} else {
		// Use a large batch size to read all rows in one batch
		props.BatchSize = 1 << 30 // 1 billion rows
	}

	reader, err := pqarrow.NewFileReader(file, props, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}

	schema, err := reader.Schema()
	if err != nil {
		return nil, err
	}
	output := make([][]byte, 0, schema.Metadata().Len())

	// Use GetRecordReader to read all row groups with the specified batch size
	rgReader, err := reader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return nil, err
	}
	defer rgReader.Release()

	for rgReader.Next() {
		record := rgReader.Record()
		record.Retain()

		bytes, err := recordToBytes(record)
		record.Release()
		if err != nil {
			return nil, err
		}
		output = append(output, bytes)
	}

	return output, nil
}

func IterateParquetFile(featuresFile string, chunkSize int64) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		file, err := parquetFile.OpenParquetFile(featuresFile, false)
		if err != nil {
			yield(nil, err)
			return
		}
		defer file.Close()

		// Set batch size - if <= 0, use a large value to read all rows at once
		props := pqarrow.ArrowReadProperties{}
		if chunkSize > 0 {
			props.BatchSize = chunkSize
		} else {
			// Use a large batch size to read all rows in one batch
			props.BatchSize = 1 << 30 // 1 billion rows
		}

		reader, err := pqarrow.NewFileReader(file, props, memory.DefaultAllocator)
		if err != nil {
			yield(nil, err)
			return
		}

		rgReader, err := reader.GetRecordReader(context.Background(), nil, nil)
		if err != nil {
			yield(nil, err)
			return
		}
		defer rgReader.Release()

		for rgReader.Next() {
			record := rgReader.Record()
			record.Retain()

			bytes, err := recordToBytes(record)
			record.Release()
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(bytes, nil) {
				return
			}
		}
		return
	}
}
