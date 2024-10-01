package parse

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCliInputsBasic(t *testing.T) {
	record := parseInputsToRecord(
		map[string]string{
			"id":     "a492e7d0-1dc2-429d-97a0-63897764e883",
			"int":    "2",
			"bool":   "true",
			"float":  "2.0",
			"float2": "210.50",
		},
		map[string]int64{},
		map[string]string{},
	)
	schema := record.Schema()

	for i, v := range record.Columns() {
		columnField := schema.Field(i)
		switch columnField.Name {
		case "id":
			assert.Equal(t, v.DataType(), arrow.BinaryTypes.LargeString, "expected string value for field %s", columnField.Name)
		case "int":
			assert.Equal(t, v.DataType(), arrow.PrimitiveTypes.Int64, "expected int64 value for field %s", columnField.Name)
		case "bool":
			assert.Equal(t, v.DataType(), arrow.FixedWidthTypes.Boolean, "expected bool value for field %s", columnField.Name)
		case "float":
			assert.Equal(t, v.DataType(), arrow.PrimitiveTypes.Float64, "expected float64 value for field %s", columnField.Name)
		case "float2":
			assert.Equal(t, v.DataType(), arrow.PrimitiveTypes.Float64, "expected float64 value for field %s", columnField.Name)
		}
	}
}

func TestCliInputsOverride(t *testing.T) {
	record := parseInputsToRecord(
		map[string]string{
			"id": "a492e7d0-1dc2-429d-97a0-63897764e883",
		},
		map[string]int64{
			"intExplicit": 2.0,
		},
		map[string]string{
			"strExplicit": "2",
		},
	)
	schema := record.Schema()

	for i, v := range record.Columns() {
		columnField := schema.Field(i)
		switch columnField.Name {
		case "id":
			assert.Equal(t, v.DataType(), arrow.BinaryTypes.LargeString, "expected string value for field %s", columnField.Name)
		case "intExplicit":
			assert.Equal(t, v.DataType(), arrow.PrimitiveTypes.Int64, "expected int64 value for field %s", columnField.Name)
		case "strExplicit":
			assert.Equal(t, v.DataType(), arrow.BinaryTypes.LargeString, "expected string value for field %s", columnField.Name)
		}
	}
}
