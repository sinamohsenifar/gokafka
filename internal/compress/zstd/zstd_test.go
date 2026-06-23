package zstd

import (
	"bytes"
	"os"
	"os/exec"
	"testing"
)

func TestDecodeCLI(t *testing.T) {
	in := bytes.Repeat([]byte("kafka"), 100)
	cmd := exec.Command("zstd", "-c", "-1")
	cmd.Stdin = bytes.NewReader(in)
	compressed, err := cmd.Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			t.Skip("zstd CLI not available")
		}
		t.Fatal(err)
	}
	out, err := Decode(compressed, len(in)*2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, out) {
		t.Fatalf("decode mismatch: got %d bytes", len(out))
	}
}

func TestDecodeHello(t *testing.T) {
	// From Go stdlib internal/zstd test vector.
	compressed := []byte{
		0x28, 0xb5, 0x2f, 0xfd, 0x24, 0x0d, 0x69, 0x00, 0x00,
		0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x0a,
		0x4c, 0x1f, 0xf9, 0xf1,
	}
	out, err := Decode(compressed, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hello, world\n" {
		t.Fatalf("got %q", out)
	}
}

func TestRoundTripInternal(t *testing.T) {
	in := bytes.Repeat([]byte("compressed-zstd-"), 32)
	out, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) >= len(in) {
		t.Fatalf("expected compression, got %d >= %d", len(out), len(in))
	}
	back, err := Decode(out, len(in)*2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, back) {
		t.Fatal("roundtrip mismatch")
	}
}

func TestEncodeEmpty(t *testing.T) {
	out, err := Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Decode(out, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 0 {
		t.Fatalf("expected empty, got %d bytes", len(back))
	}
}

func TestDecodeCorrupt(t *testing.T) {
	if _, err := Decode([]byte{0x28, 0xb5}, 1024); err == nil {
		t.Fatal("expected error")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
