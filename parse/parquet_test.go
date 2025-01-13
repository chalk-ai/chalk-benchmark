package parse

import (
	"testing"
)

func TestParquetInputFile(t *testing.T) {
	_, err := ReadParquetFile("../testdata/test.parquet", 1)
	if err != nil {
		t.Fatal(err)
	}
}
