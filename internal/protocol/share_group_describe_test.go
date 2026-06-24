package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestEncodeShareGroupDescribeRequest(t *testing.T) {
	body := protocol.EncodeShareGroupDescribeRequest([]string{"share-grp"}, false)
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}
