package parse

import (
	"context"
	"github.com/apache/arrow/go/v17/arrow/array"
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
	reader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	schema, err := reader.Schema()
	if err != nil {
		return nil, err
	}
	output := make([][]byte, 0, schema.Metadata().Len())
	table, err := reader.ReadTable(context.Background())
	if err != nil {
		return nil, err
	}
	tr := array.NewTableReader(table, chunkSize)
	for tr.Next() {
		record := tr.Record()
		bytes, err := recordToBytes(record)
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
			if !yield(nil, err) {
				return
			}
		}
		defer file.Close()
		reader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if err != nil {
			if !yield(nil, err) {
				return
			}
		}
		////schema, err := reader.Schema()
		//if err != nil {
		//	if !yield(nil, err) {
		//		return
		//	}
		//}
		table, err := reader.ReadTable(context.Background())
		if err != nil {
			if !yield(nil, err) {
				return
			}
		}
		tr := array.NewTableReader(table, chunkSize)
		for tr.Next() {
			record := tr.Record()
			bytes, err := recordToBytes(record)
			if err != nil {
				if !yield(nil, err) {
					return
				}
			}
			if !yield(bytes, nil) {
				return
			}
		}
		return
	}
}
