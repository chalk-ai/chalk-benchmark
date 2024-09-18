package parse

import (
	"testing"
)

func TestParquetInputFile(t *testing.T) {
	records, err := ReadParquetFile("../testdata/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if uint(records[0]["user.id"].GetNumberValue()) != 1 {
		t.Fatalf("record should be equal")
	}
	if records[0]["user.name"].GetStringValue() != "Samuel" {
		t.Fatalf("record should be equal")
	}
	if records[0]["user.is_conventional_name"].GetBoolValue() != true {
		t.Fatalf("record should be equal")
	}
}
