package dfa

import (
	"testing"
)

func TestDFA_Match(t *testing.T) {
	words := []string{"abc", "efg", "hij"}
	dfa := NewDFA(words)
	tests := []struct {
		text string
		want bool
	}{
		{"", false},
		{"a", false},
		{"ab", false},
		{"abc", true},
		{"abcd", false},
		{"efg", true},
		{"efgh", false},
		{"hij", true},
		{"hijk", false},
	}
	for _, tt := range tests {
		got := dfa.Match(tt.text)
		if got != tt.want {
			t.Errorf("Match(%q) = %v; want %v", tt.text, got, tt.want)
		}
	}
}
