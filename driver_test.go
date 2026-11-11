package event_test

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// LinearBackoff：base * attempt，封顶 max（docs/api.md §2.4）。
func TestLinearBackoff(t *testing.T) {
	b := event.LinearBackoff(time.Second, 10*time.Second)
	tests := []struct {
		name string
		attn int
		want time.Duration
	}{
		{"Attempt1_ReturnsBase", 1, time.Second},
		{"Attempt3_ReturnsBaseTimesAttempt", 3, 3 * time.Second},
		{"AttemptAtMax_ReturnsMax", 10, 10 * time.Second},
		{"AttemptBeyondMax_CapsAtMax", 100, 10 * time.Second},
		{"AttemptZero_ClampedToFirstFailure", 0, time.Second},
		{"AttemptNegative_ClampedToFirstFailure", -5, time.Second},
		{"AttemptOverflow_CapsAtMax", 1 << 62, 10 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := b.Next(tt.attn); got != tt.want {
				t.Fatalf("Next(%d) = %v, want %v", tt.attn, got, tt.want)
			}
		})
	}
}

// ExponentialBackoff：base * 2^(attempt-1)，封顶 max（docs/api.md §2.4）。
func TestExponentialBackoff(t *testing.T) {
	b := event.ExponentialBackoff(time.Second, 10*time.Second)
	tests := []struct {
		name string
		attn int
		want time.Duration
	}{
		{"Attempt1_ReturnsBase", 1, time.Second},
		{"Attempt2_Doubles", 2, 2 * time.Second},
		{"Attempt4_QuadruplesTwice", 4, 8 * time.Second},
		{"AttemptBeyondMax_CapsAtMax", 5, 10 * time.Second},
		{"AttemptZero_ClampedToFirstFailure", 0, time.Second},
		{"AttemptNegative_ClampedToFirstFailure", -3, time.Second},
		{"AttemptOverflow_CapsAtMax", 100, 10 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := b.Next(tt.attn); got != tt.want {
				t.Fatalf("Next(%d) = %v, want %v", tt.attn, got, tt.want)
			}
		})
	}
}
