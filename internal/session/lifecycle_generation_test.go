package session

import "testing"

func TestCanonicalRuntimeGeneration(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "default", raw: "", want: "1"},
		{name: "valid", raw: "3", want: "3"},
		{name: "zero", raw: "0", want: "1"},
		{name: "negative", raw: "-2", want: "1"},
		{name: "invalid", raw: "legacy", want: "1"},
		{name: "whitespace is invalid", raw: " 3 ", want: "1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanonicalRuntimeGeneration(tt.raw); got != tt.want {
				t.Fatalf("CanonicalRuntimeGeneration(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
