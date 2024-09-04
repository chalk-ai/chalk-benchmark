package cmd

import (
	"context"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	parquetFile "github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
	"slices"
)

type Record = map[string]*structpb.Value

func convertValuesToRecords(values []*structpb.Value, columnNames []string) ([]Record, error) {
	numRows := len(values) / len(columnNames)
	rows := make([]Record, 0, numRows)
	for i := 0; i < numRows; i++ {
		record := make(Record)
		for j := 0; j < len(columnNames); j++ {
			record[columnNames[j]] = values[j*numRows+i]
		}
		rows = append(rows, record)
	}
	return rows, nil
}

func ReadParquetFile(featuresFile string) ([]Record, error) {
	file, err := parquetFile.OpenParquetFile(featuresFile, false)
	defer file.Close()
	if err != nil {
		return nil, err
	}
	reader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	schema, err := reader.Schema()
	columnNames := make([]string, 0, schema.NumFields())

	if err != nil {
		return nil, err
	}
	table, err := reader.ReadTable(context.Background())
	if err != nil {
		return nil, err
	}

	var values []*structpb.Value
	for i := int64(0); i < table.NumCols(); i++ {
		col := table.Column(int(i))
		if err != nil {
			return nil, err
		}
		for _, chunk := range col.Data().Chunks() {
			switch col.DataType().ID() {
			case arrow.INT32:
				intArray := chunk.(*array.Int32).Int32Values()
				values = slices.Concat(
					values,
					lo.Map(
						intArray,
						func(x int32, _ int) *structpb.Value {
							return structpb.NewNumberValue(float64(x))
						},
					),
				)
			case arrow.INT64:
				intArray := chunk.(*array.Int64).Int64Values()
				values = slices.Concat(
					values,
					lo.Map(
						intArray,
						func(x int64, _ int) *structpb.Value {
							return structpb.NewNumberValue(float64(x))
						},
					),
				)
			case arrow.FLOAT64:
				floatArray := chunk.(*array.Float64).Float64Values()
				values = slices.Concat(
					values,
					lo.Map(
						floatArray,
						func(x float64, _ int) *structpb.Value {
							return structpb.NewNumberValue(x)
						},
					),
				)
			case arrow.BOOL:
				for i := 0; i < chunk.Len(); i++ {
					values = append(
						values,
						structpb.NewBoolValue(chunk.(*array.Boolean).Value(i)),
					)
				}

			case arrow.STRING:
				for i := 0; i < chunk.Len(); i++ {
					values = append(
						values,
						structpb.NewStringValue(chunk.ValueStr(i)),
					)
				}
			}
		}
		columnNames = append(columnNames, col.Name())
	}
	if err != nil {
		return nil, err
	}
	return convertValuesToRecords(values, columnNames)
}
