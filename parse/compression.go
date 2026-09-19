package parse

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
)

// Compression selects the Arrow IPC body compression applied to query inputs
// before they go on the wire as InputsFeather.
type Compression string

const (
	CompressionNone Compression = "none"
	CompressionLZ4  Compression = "lz4"
	CompressionZstd Compression = "zstd"
)

// CompressionValues lists the accepted --input-compression values, for help text
// and validation errors.
var CompressionValues = []string{string(CompressionNone), string(CompressionLZ4), string(CompressionZstd)}

// inputCompression defaults to LZ4 to match the Chalk Python client, which
// encodes inputs with compression="lz4". Sending inputs uncompressed made the
// benchmark report a wire cost no production client actually pays: on a
// 512-account sample with JSON payload features, one request was 163KB
// uncompressed at p50 and 1.7MB at the maximum, versus 27KB and 238KB under
// LZ4. That inflated client-observed latency while server-side query time was
// unchanged, which is precisely the gap a load test should not invent.
var inputCompression = CompressionLZ4

// SetInputCompression selects the codec used for input record batches. It is
// expected to be called once, from flag parsing, before any batches are built.
func SetInputCompression(name string) error {
	switch Compression(name) {
	case CompressionNone, CompressionLZ4, CompressionZstd:
		inputCompression = Compression(name)
		return nil
	default:
		return fmt.Errorf("unknown input compression %q: expected one of %v", name, CompressionValues)
	}
}

// InputCompression reports the codec currently in effect.
func InputCompression() Compression {
	return inputCompression
}

// ipcWriteOptions builds the writer options for a record batch, applying the
// configured codec. Arrow compresses the record body only; the schema and
// footer stay uncompressed, so the reader needs no matching configuration.
func ipcWriteOptions(schema *arrow.Schema) []ipc.Option {
	opts := []ipc.Option{ipc.WithSchema(schema)}
	switch inputCompression {
	case CompressionLZ4:
		opts = append(opts, ipc.WithLZ4())
	case CompressionZstd:
		opts = append(opts, ipc.WithZstd())
	}
	return opts
}
