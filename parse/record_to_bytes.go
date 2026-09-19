package parse

import (
	"bytes"
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"io"
)

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

// recordToBytes converts an Arrow record to IPC bytes and releases the record.
func recordToBytes(record arrow.Record) ([]byte, error) {
	defer record.Release()
	return RecordToBytes(record)
}

// RecordToBytes converts an Arrow record to IPC bytes
// Note: This function does NOT release the record - the caller is responsible for lifecycle management
func RecordToBytes(record arrow.Record) ([]byte, error) {
	bws := &BufferWriteSeeker{}
	// The error from NewFileWriter was previously discarded, which left a nil
	// fileWriter to be dereferenced on the next line. Selecting a codec adds a
	// real failure path here, so it is checked.
	fileWriter, err := ipc.NewFileWriter(bws, ipcWriteOptions(record.Schema())...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow Table writer: %w", err)
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
