package parse

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compressibleBatch builds rows carrying a long repeated string, standing in for the
// large JSON payloads real benchmarks send as inputs.
func compressibleBatch(rows int) []map[string]interface{} {
	payload := strings.Repeat("transaction-payload-", 200)
	batch := make([]map[string]interface{}, rows)
	for i := range batch {
		batch[i] = map[string]interface{}{
			"account.id":               float64(i),
			"account.raw_transactions": payload,
		}
	}
	return batch
}

func TestSetInputCompression(t *testing.T) {
	t.Cleanup(func() { _ = SetInputCompression("uncompressed") })

	for _, tc := range []struct{ in, want string }{
		{"", "uncompressed"},
		{"none", "uncompressed"},
		{"uncompressed", "uncompressed"},
		{"lz4", "lz4"},
		{"LZ4", "lz4"},
		{"zstd", "zstd"},
		{"ZSTD", "zstd"},
	} {
		require.NoError(t, SetInputCompression(tc.in), "codec %q should be accepted", tc.in)
		assert.Equal(t, tc.want, InputCompression())
	}

	require.Error(t, SetInputCompression("gzip"), "unsupported codec should be rejected")
}

func TestRecordToBytesCompressionShrinksPayload(t *testing.T) {
	t.Cleanup(func() { _ = SetInputCompression("uncompressed") })

	sizes := map[string]int{}
	for _, codec := range []string{"uncompressed", "lz4", "zstd"} {
		require.NoError(t, SetInputCompression(codec))

		record, err := jsonBatchToRecord(compressibleBatch(32))
		require.NoError(t, err)

		encoded, err := recordToBytes(record) // releases the record
		require.NoError(t, err)
		require.NotEmpty(t, encoded)
		sizes[codec] = len(encoded)
	}

	assert.Less(t, sizes["lz4"], sizes["uncompressed"], "lz4 should be smaller than uncompressed")
	assert.Less(t, sizes["zstd"], sizes["uncompressed"], "zstd should be smaller than uncompressed")
}

func TestRecordToBytesCompressedRoundTrips(t *testing.T) {
	t.Cleanup(func() { _ = SetInputCompression("uncompressed") })

	for _, codec := range []string{"uncompressed", "lz4", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			require.NoError(t, SetInputCompression(codec))

			record, err := jsonBatchToRecord(compressibleBatch(8))
			require.NoError(t, err)

			encoded, err := recordToBytes(record)
			require.NoError(t, err)

			// The engine reads these bytes back, so a codec it cannot decode is a
			// silent benchmark failure rather than a compile error.
			reader, err := ipc.NewFileReader(bytes.NewReader(encoded))
			require.NoError(t, err)
			defer reader.Close()

			require.Equal(t, 1, reader.NumRecords())
			got, err := reader.Record(0)
			require.NoError(t, err)
			assert.Equal(t, int64(8), got.NumRows())
		})
	}
}
