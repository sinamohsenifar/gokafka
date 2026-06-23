package bufpool

import "testing"

func TestPoolGetPut(t *testing.T) {
	p := New(1024)
	b := p.Get()
	if cap(b) < 1024 {
		t.Fatalf("cap=%d want >=1024", cap(b))
	}
	b = append(b, 1, 2, 3)
	p.Put(b)
	b2 := p.Get()
	if len(b2) != 0 {
		t.Fatalf("len=%d want 0", len(b2))
	}
}

func TestGrow(t *testing.T) {
	b := []byte{1}
	out := Grow(b, 4)
	if len(out) != 4 || out[0] != 1 {
		t.Fatalf("grow=%v", out)
	}
}
