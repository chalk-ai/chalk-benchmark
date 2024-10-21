package parse

import (
	"context"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	parquetFile "github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

func ReadParquetFile(featuresFile string, chunkSize int64) ([][]byte, error) {
	file, err := parquetFile.OpenParquetFile(featuresFile, false)
	defer file.Close()
	if err != nil {
		return nil, err
	}
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
