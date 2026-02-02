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
	batchList := ProcessJSONInputs(jsonFile)

	// Check that we got 2 batches
	assert.Len(t, batchList, 2)

	// Check that each batch contains arrow
	for i, batch := range batchList {
		assert.Greater(t, len(batch), 0, "batch %d should not be empty", i)
	}
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
	batchList := ProcessJSONInputs(jsonFile)

	// Should have 2 single-row batches
	assert.Len(t, batchList, 2)
	for i, batch := range batchList {
		assert.Greater(t, len(batch), 0, "batch %d should not be empty", i)
	}
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

func TestJSONBatchToRecordMissingField(t *testing.T) {
	batch := []map[string]interface{}{
		{"user.id": float64(1), "user.name": "Alice"},
		{"user.id": float64(2)}, // Missing user.name
	}

	_, err := jsonBatchToRecord(batch)
	assert.Error(t, err, "expected error for missing field")
	assert.Contains(t, err.Error(), "missing in row")
}

func TestJSONBatchToRecordBasicTypes(t *testing.T) {
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

	// Check schema
	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, int64(4), record.NumCols())

	// Check field types
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
			"bool_array":   []interface{}{false, false},
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Check schema
	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(4), record.NumCols())

	// Check array field types
	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		if field.Name == "int_array" {
			assert.Equal(t, arrow.LIST, field.Type.ID())
			listType := field.Type.(*arrow.ListType)
			assert.Equal(t, arrow.INT64, listType.Elem().ID())
		} else if field.Name == "string_array" {
			assert.Equal(t, arrow.LIST, field.Type.ID())
			listType := field.Type.(*arrow.ListType)
			assert.Equal(t, arrow.LARGE_STRING, listType.Elem().ID())
		} else if field.Name == "bool_array" {
			assert.Equal(t, arrow.LIST, field.Type.ID())
			listType := field.Type.(*arrow.ListType)
			assert.Equal(t, arrow.BOOL, listType.Elem().ID())
		}
	}
}

func TestJSONBatchToRecordWithEmptyBatch(t *testing.T) {
	batch := []map[string]interface{}{}

	_, err := jsonBatchToRecord(batch)
	assert.Error(t, err, "expected error for empty batch")
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

func TestJSONBatchToRecordWithSingleStruct(t *testing.T) {
	batch := []map[string]interface{}{
		{
			"user.id": float64(1),
			"profile": map[string]interface{}{
				"name": "Alice",
				"age":  float64(30),
			},
		},
		{
			"user.id": float64(2),
			"profile": map[string]interface{}{
				"name": "Bob",
				"age":  float64(25),
			},
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Check schema
	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())

	// Check struct field
	schema := record.Schema()
	profileField := schema.Field(0)
	assert.Equal(t, "profile", profileField.Name)
	assert.Equal(t, arrow.STRUCT, profileField.Type.ID())

	// Check struct has correct nested fields
	structType := profileField.Type.(*arrow.StructType)
	assert.Equal(t, 2, structType.NumFields())

	// Fields should be sorted alphabetically: age, name
	assert.Equal(t, "age", structType.Field(0).Name)
	assert.Equal(t, arrow.INT64, structType.Field(0).Type.ID())

	assert.Equal(t, "name", structType.Field(1).Name)
	assert.Equal(t, arrow.LARGE_STRING, structType.Field(1).Type.ID())
}

func TestJSONBatchToRecordWithStructArrays(t *testing.T) {
	// Test list of structs (has-many relationship)
	batch := []map[string]interface{}{
		{
			"user.id": float64(1),
			"user.orders": []interface{}{
				map[string]interface{}{
					"order.id": float64(101),
					"order.amount":   50.99,
				},
				map[string]interface{}{
					"order.id": float64(102),
					"order.amount":   75.5,
				},
			},
		},
		{
			"user.id": float64(2),
			"user.orders": []interface{}{
				map[string]interface{}{
					"order.id": float64(201),
					"order.amount":   120.5,
				},
			},
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Check schema
	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())

	// Check list-of-struct field
	// Fields are sorted alphabetically, so orders comes before user.id
	schema := record.Schema()

	// Find the orders field
	var ordersField arrow.Field
	for i := 0; i < int(record.NumCols()); i++ {
		if schema.Field(i).Name == "user.orders" {
			ordersField = schema.Field(i)
			break
		}
	}

	assert.Equal(t, "user.orders", ordersField.Name)
	assert.Equal(t, arrow.LIST, ordersField.Type.ID())

	// Check list element is a struct
	listType := ordersField.Type.(*arrow.ListType)
	assert.Equal(t, arrow.STRUCT, listType.Elem().ID())

	// Check struct fields
	structType := listType.Elem().(*arrow.StructType)
	assert.Equal(t, 2, structType.NumFields())

	// Fields should be sorted: amount, order_id
	assert.Equal(t, "order.amount", structType.Field(0).Name)
	assert.Equal(t, arrow.FLOAT64, structType.Field(0).Type.ID())

	assert.Equal(t, "order.id", structType.Field(1).Name)
	assert.Equal(t, arrow.INT64, structType.Field(1).Type.ID())
}

func TestJSONBatchToRecordWithNestedStructs(t *testing.T) {
	// Test nested structs
	batch := []map[string]interface{}{
		{
			"user.id": float64(1),
			"user.address": map[string]interface{}{
				"address.street": "123 Main St",
				"address.location": map[string]interface{}{
					"location.city":  "San Francisco",
					"location.state": "CA",
				},
			},
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Check schema
	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())

	schema := record.Schema()
	addressField := schema.Field(0)
	assert.Equal(t, "user.address", addressField.Name)
	assert.Equal(t, arrow.STRUCT, addressField.Type.ID())

	// Check nested struct
	structType := addressField.Type.(*arrow.StructType)
	assert.Equal(t, 2, structType.NumFields())

	// Check that location is also a struct
	locationField := structType.Field(0)
	assert.Equal(t, "address.location", locationField.Name)
	assert.Equal(t, arrow.STRUCT, locationField.Type.ID())

	nestedStructType := locationField.Type.(*arrow.StructType)
	assert.Equal(t, 2, nestedStructType.NumFields())
}

func TestJSONBatchToRecordWithMixedComplexTypes(t *testing.T) {
	// Test combination of primitives, arrays, and structs
	batch := []map[string]interface{}{
		{
			"user.id":   float64(1),
			"user.name": "Alice",
			"user.tags": []interface{}{"premium", "verified"}, // Use []interface{} for JSON compatibility
			"user.metadata": map[string]interface{}{
				"metadata.created_at": "2024-01-01",
				"metadata.updated_at": "2024-01-15",
			},
			"user.purchases": []interface{}{
				map[string]interface{}{
					"purchase.item":  "laptop",
					"purchase.price": 999.99,
				},
				map[string]interface{}{
					"purchase.item":  "mouse",
					"purchase.price": 29.99,
				},
			},
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Check schema has all expected fields
	schema := record.Schema()
	assert.Equal(t, int64(5), record.NumCols())

	// Verify each field type
	fieldMap := make(map[string]arrow.Field)
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		fieldMap[field.Name] = field
	}

	// Check primitive
	assert.Equal(t, arrow.INT64, fieldMap["user.id"].Type.ID())

	// Check string array
	assert.Equal(t, arrow.LIST, fieldMap["user.tags"].Type.ID())

	// Check struct
	assert.Equal(t, arrow.STRUCT, fieldMap["user.metadata"].Type.ID())

	// Check list of structs
	assert.Equal(t, arrow.LIST, fieldMap["user.purchases"].Type.ID())
	purchasesListType := fieldMap["user.purchases"].Type.(*arrow.ListType)
	assert.Equal(t, arrow.STRUCT, purchasesListType.Elem().ID())
}

func TestJSONBatchToRecordWithEmptyStruct(t *testing.T) {
	batch := []map[string]interface{}{
		{
			"user.id": float64(1),
			"user.empty":   map[string]interface{}{},
		},
	}

	record, err := jsonBatchToRecord(batch)
	require.NoError(t, err)
	defer record.Release()

	// Should create a struct with 0 fields
	schema := record.Schema()
	emptyField := schema.Field(0)
	assert.Equal(t, "user.empty", emptyField.Name)
	assert.Equal(t, arrow.STRUCT, emptyField.Type.ID())

	structType := emptyField.Type.(*arrow.StructType)
	assert.Equal(t, 0, structType.NumFields())
}

