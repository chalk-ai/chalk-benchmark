package parse

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
)

// inputCompression is the Arrow IPC codec applied to query inputs. Arrow defaults
// to uncompressed, which does not match the Chalk clients a benchmark is meant to
// stand in for -- the Python gRPC client compresses inputs with lz4 by default --
// so a benchmark run with the default here overstates on-the-wire cost.
var inputCompression = "uncompressed"

// SetInputCompression selects the Arrow IPC codec used for query inputs. Valid
// values are "uncompressed", "lz4" and "zstd".
func SetInputCompression(codec string) error {
	switch strings.ToLower(codec) {
	case "", "uncompressed", "none":
		inputCompression = "uncompressed"
	case "lz4":
		inputCompression = "lz4"
	case "zstd":
		inputCompression = "zstd"
	default:
		return fmt.Errorf("unsupported input compression %q: expected one of uncompressed, lz4, zstd", codec)
	}
	return nil
}

// InputCompression reports the codec currently applied to query inputs.
func InputCompression() string {
	return inputCompression
}

// ipcWriterOptions builds the writer options for a record, applying the configured
// codec. Kept in one place so every encoder in this package stays consistent.
func ipcWriterOptions(schema *arrow.Schema) []ipc.Option {
	opts := []ipc.Option{ipc.WithSchema(schema)}
	switch inputCompression {
	case "lz4":
		opts = append(opts, ipc.WithLZ4())
	case "zstd":
		opts = append(opts, ipc.WithZstd())
	}
	return opts
}

type BufferWriteSeeker struct {
	buf bytes.Buffer
	off int64
}

func (b *BufferWriteSeeker) Write(p []byte) (n int, err error) {
	n, err = b.buf.Write(p)
	b.off += int64(n)
	return
}

func (b *BufferWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset < 0 || offset > int64(b.buf.Len()) {
			return 0, io.EOF
		}
		b.off = offset
	case io.SeekCurrent:
		newOffset := b.off + offset
		if newOffset < 0 || newOffset > int64(b.buf.Len()) {
			return 0, io.EOF
		}
		b.off = newOffset
	case io.SeekEnd:
		newOffset := int64(b.buf.Len()) + offset
		if newOffset < 0 || newOffset > int64(b.buf.Len()) {
			return 0, io.EOF
		}
		b.off = newOffset
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	return b.off, nil
}

func (b *BufferWriteSeeker) Bytes() []byte {
	return b.buf.Bytes()
}

func recordToBytes(record arrow.Record) ([]byte, error) {
	bws := &BufferWriteSeeker{}
	fileWriter, err := ipc.NewFileWriter(bws, ipcWriterOptions(record.Schema())...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow IPC writer: %w", err)
	}
	err = fileWriter.Write(record)
	if err != nil {
		return nil, fmt.Errorf("failed to write Arrow Table to request: %w", err)
	}
	err = fileWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close Arrow Table writer: %w", err)
	}
	record.Release()
	return bws.Bytes(), nil
}

// RecordToBytes converts an Arrow record to IPC bytes
// Note: This function does NOT release the record - the caller is responsible for lifecycle management
func RecordToBytes(record arrow.Record) ([]byte, error) {
	bws := &BufferWriteSeeker{}
	fileWriter, err := ipc.NewFileWriter(bws, ipcWriterOptions(record.Schema())...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow IPC writer: %w", err)
	}
	err = fileWriter.Write(record)
	if err != nil {
		return nil, fmt.Errorf("failed to write Arrow Table to request: %w", err)
	}
	err = fileWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close Arrow Table writer: %w", err)
	}
	return bws.Bytes(), nil
}
