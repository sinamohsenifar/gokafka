package limits

import "testing"

func TestApply(t *testing.T) {
	old := MaxResponseBytes()
	defer Apply(Config{MaxResponseBytes: old})
	Apply(Config{MaxResponseBytes: 1024})
	if MaxResponseBytes() != 1024 {
		t.Fatalf("got %d", MaxResponseBytes())
	}
	Apply(Config{})
	if MaxResponseBytes() != 1024 {
		t.Fatalf("zero should not reset, got %d", MaxResponseBytes())
	}
}
