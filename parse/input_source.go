package parse

// InputSource represents a thread-safe source of marshalled OnlineQueryBulk requests
// for benchmarking. Implementations handle cycling through available data automatically.
type InputSource interface {
	// Next returns the next marshalled OnlineQueryBulk request bytes.
	// This method is thread-safe and will cycle through available data automatically.
	// It never returns an error under normal operation (cycling is infinite).
	Next() ([]byte, error)

	// Close releases any resources held by the input source.
	Close() error
}
