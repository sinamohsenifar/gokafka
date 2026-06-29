package protocol

import "testing"

func TestSafePrealloc(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{-5, 0},
		{0, 0},
		{1, 1},
		{4096, 4096},
		{4097, 4096},
		{1 << 30, 4096}, // hostile count must not drive a huge make
	}
	for _, c := range cases {
		if got := safePrealloc(c.in); got != c.want {
			t.Errorf("safePrealloc(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
