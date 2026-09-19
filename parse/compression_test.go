package parse

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// compressibleRecord builds a one-row batch with a large, repetitive string
// column, standing in for the JSON payload features that make input size the
// dominant term in a request.
func compressibleRecord(t *testing.T) arrow.Record {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "account.id", Type: arrow.BinaryTypes.String},
		{Name: "account.raw_transactions", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).Append("acct_0001")
	b.Field(1).(*array.StringBuilder).Append(strings.Repeat(`{"amount":12.5,"name":"card deposit"},`, 2000))
	return b.NewRecord()
}

func TestSetInputCompression(t *testing.T) {
	original := InputCompression()
	t.Cleanup(func() { inputCompression = original })

	for _, name := range CompressionValues {
		if err := SetInputCompression(name); err != nil {
			t.Fatalf("SetInputCompression(%q) returned %v, want nil", name, err)
		}
		if got := string(InputCompression()); got != name {
			t.Fatalf("InputCompression() = %q, want %q", got, name)
		}
	}

	if err := SetInputCompression("brotli"); err == nil {
		t.Fatal("SetInputCompression(\"brotli\") returned nil, want an error")
	}
}

// TestDefaultInputCompressionIsLZ4 pins the default, since changing it silently
// would shift every reported latency number.
func TestDefaultInputCompressionIsLZ4(t *testing.T) {
	if InputCompression() != CompressionLZ4 {
		t.Fatalf("default compression = %q, want %q", InputCompression(), CompressionLZ4)
	}
}

func TestRecordToBytesRoundTripsUnderEveryCodec(t *testing.T) {
	original := InputCompression()
	t.Cleanup(func() { inputCompression = original })

	sizes := map[string]int{}
	for _, name := range CompressionValues {
		if err := SetInputCompression(name); err != nil {
			t.Fatalf("SetInputCompression(%q): %v", name, err)
		}
		record := compressibleRecord(t)
		encoded, err := RecordToBytes(record)
		if err != nil {
			record.Release()
			t.Fatalf("RecordToBytes with %q: %v", name, err)
		}
		sizes[name] = len(encoded)

		// A reader needs no matching configuration: the codec is recorded in the
		// IPC metadata, so a plain reader decodes every variant.
		reader, err := ipc.NewFileReader(bytes.NewReader(encoded))
		if err != nil {
			record.Release()
			t.Fatalf("NewFileReader with %q: %v", name, err)
		}
		decoded, err := reader.Record(0)
		if err != nil {
			record.Release()
			t.Fatalf("Record(0) with %q: %v", name, err)
		}
		if got, want := decoded.NumRows(), record.NumRows(); got != want {
			t.Fatalf("%q: decoded %d rows, want %d", name, got, want)
		}
		wantCol := record.Column(1).(*array.String).Value(0)
		gotCol := decoded.Column(1).(*array.String).Value(0)
		if gotCol != wantCol {
			t.Fatalf("%q: payload did not survive the round trip (%d vs %d bytes)", name, len(gotCol), len(wantCol))
		}
		reader.Close()
		record.Release()
	}

	// The whole point of the flag: the compressed codecs must actually shrink a
	// compressible payload relative to sending it raw.
	for _, name := range []string{string(CompressionLZ4), string(CompressionZstd)} {
		if sizes[name] >= sizes[string(CompressionNone)] {
			t.Errorf("%s encoded to %d bytes, not smaller than uncompressed %d",
				name, sizes[name], sizes[string(CompressionNone)])
		}
	}
	t.Log(fmt.Sprintf("encoded sizes: none=%d lz4=%d zstd=%d",
		sizes[string(CompressionNone)], sizes[string(CompressionLZ4)], sizes[string(CompressionZstd)]))
}
