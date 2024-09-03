package cmd

import "testing"

func TestReadParquetFile(t *testing.T) {
	parq, err := ReadParquetFile("../test.parquet")
	if err != nil {
		t.Errorf("Failed to read parquet file")
	}
	if parq == nil {
		t.Errorf("Failed to read parquet file")
	}
}
