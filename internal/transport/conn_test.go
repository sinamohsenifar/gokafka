package transport

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/auth"
)

// TestConnRequestConcurrent verifies one in-flight request per connection under -race.
func TestConnRequestConcurrent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}(nc)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, ln.Addr().String(), "race-test", auth.Config{Protocol: auth.SecurityPlaintext}, time.Second, time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = conn.Request(ctx, 18, 0, nil)
		}()
	}
	wg.Wait()
}
