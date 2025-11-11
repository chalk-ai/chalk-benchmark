package parse

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessJSONInputsBatchedFormat(t *testing.T) {
	// Create a temporary JSON file with batched format
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "test_batched.json")

	jsonContent := `[
		[
			{"user.id": 1, "user.name": "Alice", "user.age": 25},
			{"user.id": 2, "user.name": "Bob", "user.age": 30},
			{"user.id": 3, "user.name": "Charlie", "user.age": 35}
		],
		[
			{"user.id": 4, "user.name": "Diana", "user.age": 28},
			{"user.id": 5, "user.name": "Eve", "user.age": 32}
		]
	]`

	err := os.WriteFile(jsonFile, []byte(jsonContent), 0644)
	require.NoError(t, err)

	// Process the JSON file
	bytesList := ProcessJSONInputs(jsonFile)

	// Should have 2 batches
	assert.Equal(t, 2, len(bytesList), "expected 2 batches")

	// Each batch should contain encoded Arrow data
	assert.Greater(t, len(bytesList[0]), 0, "first batch should not be empty")
	assert.Greater(t, len(bytesList[1]), 0, "second batch should not be empty")
}

func TestProcessJSONInputsFlatFormat(t *testing.T) {
	// Create a temporary JSON file with flat format
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "test_flat.json")

	jsonContent := `[
		{"user.id": 1, "user.name": "Alice", "user.age": 25},
		{"user.id": 2, "user.name": "Bob", "user.age": 30}
	]`

	err := os.WriteFile(jsonFile, []byte(jsonContent), 0644)
	require.NoError(t, err)

	// Process the JSON file
	bytesList := ProcessJSONInputs(jsonFile)

	// Should have 2 single-row batches
	assert.Equal(t, 2, len(bytesList), "expected 2 records")

	// Each should contain encoded Arrow data
	assert.Greater(t, len(bytesList[0]), 0, "first record should not be empty")
	assert.Greater(t, len(bytesList[1]), 0, "second record should not be empty")
}

func TestJSONBatchToRecordMultipleRows(t *testing.T) {
	batch := []map[string]interface{}{
		{"user.id": float64(1), "user.name": "Alice", "user.age": float64(25)},
		{"user.id": float64(2), "user.name": "Bob", "user.age": float64(30)},
		{"user.id": float64(3), "user.name": "Charlie", "user.age": float64(35)},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Check record has 3 rows
	assert.Equal(t, int64(3), record.NumRows(), "expected 3 rows")

	// Check record has 3 columns
	assert.Equal(t, int64(3), record.NumCols(), "expected 3 columns")

	// Check field types
	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		col := record.Column(i)

		switch field.Name {
		case "user.age":
			assert.Equal(t, arrow.PrimitiveTypes.Int64, col.DataType(), "expected int64 for user.age")
		case "user.id":
			assert.Equal(t, arrow.PrimitiveTypes.Int64, col.DataType(), "expected int64 for user.id")
		case "user.name":
			assert.Equal(t, arrow.BinaryTypes.LargeString, col.DataType(), "expected string for user.name")
		}
	}
}

func TestJSONBatchToRecordSingleRow(t *testing.T) {
	batch := []map[string]interface{}{
		{"user.id": float64(1), "user.name": "Alice", "user.age": float64(25)},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Check record has 1 row
	assert.Equal(t, int64(1), record.NumRows(), "expected 1 row")

	// Check record has 3 columns
	assert.Equal(t, int64(3), record.NumCols(), "expected 3 columns")
}

func TestJSONBatchToRecordDataTypes(t *testing.T) {
	batch := []map[string]interface{}{
		{
			"int_field":    float64(42),
			"float_field":  float64(3.14),
			"string_field": "hello",
			"bool_field":   true,
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		col := record.Column(i)

		switch field.Name {
		case "int_field":
			assert.Equal(t, arrow.PrimitiveTypes.Int64, col.DataType(), "expected int64 for int_field")
		case "float_field":
			assert.Equal(t, arrow.PrimitiveTypes.Float64, col.DataType(), "expected float64 for float_field")
		case "string_field":
			assert.Equal(t, arrow.BinaryTypes.LargeString, col.DataType(), "expected string for string_field")
		case "bool_field":
			assert.Equal(t, arrow.FixedWidthTypes.Boolean, col.DataType(), "expected boolean for bool_field")
		}
	}
}

func TestJSONBatchToRecordWithArrays(t *testing.T) {
	batch := []map[string]interface{}{
		{
			"id":           float64(1),
			"int_array":    []interface{}{float64(1), float64(2), float64(3)},
			"string_array": []interface{}{"a", "b", "c"},
			"bool_array":   []interface{}{true, false, true},
		},
		{
			"id":           float64(2),
			"int_array":    []interface{}{float64(4), float64(5)},
			"string_array": []interface{}{"d", "e"},
			"bool_array":   []interface{}{false, true},
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Check record has 2 rows
	assert.Equal(t, int64(2), record.NumRows(), "expected 2 rows")

	// Check field types
	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		col := record.Column(i)

		switch field.Name {
		case "id":
			assert.Equal(t, arrow.PrimitiveTypes.Int64, col.DataType(), "expected int64 for id")
		case "int_array":
			assert.Equal(t, arrow.ListOf(arrow.PrimitiveTypes.Int64), col.DataType(), "expected list<int64> for int_array")
		case "string_array":
			assert.Equal(t, arrow.ListOf(arrow.BinaryTypes.LargeString), col.DataType(), "expected list<string> for string_array")
		case "bool_array":
			assert.Equal(t, arrow.ListOf(arrow.FixedWidthTypes.Boolean), col.DataType(), "expected list<bool> for bool_array")
		}
	}
}

func TestJSONBatchToRecordEmptyBatch(t *testing.T) {
	batch := []map[string]interface{}{}

	_, err := jsonBatchToRecord(batch)
	assert.Error(t, err, "expected error for empty batch")
	assert.Contains(t, err.Error(), "empty batch")
}

func TestJSONBatchToRecordMissingField(t *testing.T) {
	batch := []map[string]interface{}{
		{"user.id": float64(1), "user.name": "Alice"},
		{"user.id": float64(2)}, // Missing user.name
	}

	_, err := jsonBatchToRecord(batch)
	assert.Error(t, err, "expected error for missing field")
	assert.Contains(t, err.Error(), "missing in row")
}

func TestJSONBatchToRecordConsistentFieldOrdering(t *testing.T) {
	// Test that field ordering is consistent across batches
	batch1 := []map[string]interface{}{
		{"c_field": "c", "a_field": "a", "b_field": "b"},
	}

	record1, err := jsonBatchToRecord(batch1)
	require.NoError(t, err)
	defer record1.Release()

	batch2 := []map[string]interface{}{
		{"b_field": "b2", "a_field": "a2", "c_field": "c2"},
	}

	record2, err := jsonBatchToRecord(batch2)
	require.NoError(t, err)
	defer record2.Release()

	// Field order should be alphabetical due to sorting
	schema1 := record1.Schema()
	schema2 := record2.Schema()

	assert.Equal(t, schema1.Field(0).Name, schema2.Field(0).Name, "field order should be consistent")
	assert.Equal(t, schema1.Field(1).Name, schema2.Field(1).Name, "field order should be consistent")
	assert.Equal(t, schema1.Field(2).Name, schema2.Field(2).Name, "field order should be consistent")
}

func TestCreateFieldAndBuilderIntegerDetection(t *testing.T) {
	// Test that whole numbers are detected as integers
	field, builder := createFieldAndBuilder("test", float64(42))
	defer builder.Release()

	assert.Equal(t, arrow.PrimitiveTypes.Int64, field.Type, "expected int64 for whole number")

	// Test that decimals are detected as floats
	field2, builder2 := createFieldAndBuilder("test", float64(42.5))
	defer builder2.Release()

	assert.Equal(t, arrow.PrimitiveTypes.Float64, field2.Type, "expected float64 for decimal")
}

func TestJSONBatchToRecordFloatArray(t *testing.T) {
	batch := []map[string]interface{}{
		{
			"id":          float64(1),
			"float_array": []interface{}{float64(1.5), float64(2.7), float64(3.14)},
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		col := record.Column(i)

		if field.Name == "float_array" {
			assert.Equal(t, arrow.ListOf(arrow.PrimitiveTypes.Float64), col.DataType(), "expected list<float64> for float_array")
		}
	}
}

func TestRecordToBytes(t *testing.T) {
	// Create a simple batch and convert to bytes
	batch := []map[string]interface{}{
		{"user.id": float64(1), "user.name": "Alice"},
		{"user.id": float64(2), "user.name": "Bob"},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Convert to bytes
	bytes, err := recordToBytes(record)
	require.NoError(t, err)

	// Check that we got some data
	assert.Greater(t, len(bytes), 0, "expected non-empty byte array")
}

func TestJSONBatchToRecordLargeBatch(t *testing.T) {
	// Test with a larger batch to ensure it handles many rows
	batch := make([]map[string]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		batch[i] = map[string]interface{}{
			"id":   float64(i),
			"name": "User" + string(rune(i%26+65)),
			"age":  float64(20 + i%50),
		}
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(1000), record.NumRows(), "expected 1000 rows")
	assert.Equal(t, int64(3), record.NumCols(), "expected 3 columns")
}
