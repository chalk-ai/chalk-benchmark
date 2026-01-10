package parse

import (
	"testing"
)

func TestParseLoadSegments(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		expectNil   bool
		expected    int // number of segments
	}{
		{
			name:        "Simple integer - should return nil",
			input:       "1000",
			expectError: false,
			expectNil:   true,
		},
		{
			name:        "Single segment",
			input:       "1h@1000",
			expectError: false,
			expectNil:   false,
			expected:    1,
		},
		{
			name:        "Multiple segments",
			input:       "1h@1000,30m@2000,2h@500",
			expectError: false,
			expectNil:   false,
			expected:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segments, err := ParseLoadSegments(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectNil {
				if segments != nil {
					t.Errorf("Expected nil segments but got %d segments", len(segments))
				}
				return
			}

			if segments == nil {
				t.Errorf("Expected segments but got nil")
				return
			}

			if len(segments) != tt.expected {
				t.Errorf("Expected %d segments but got %d", tt.expected, len(segments))
			}
		})
	}
}
