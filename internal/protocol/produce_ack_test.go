package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

func TestProduceRequestAcksEncoding(t *testing.T) {
	settings := protocol.ProduceSettings{Acks: -1, TimeoutMs: 30000}
	body, err := protocol.EncodeProduceRequest([]protocol.ProduceRecord{
		{Topic: "t", Partition: 0, Value: []byte("x")},
	}, settings)
	if err != nil {
		t.Fatal(err)
	}
	buf := wire.FromBytes(body)
	if _, err := buf.ReadUvarint(); err != nil { // transactional_id (flex compact nullable)
		t.Fatal(err)
	}
	acks, err := buf.ReadInt16()
	if err != nil {
		t.Fatal(err)
	}
	if acks != -1 {
		t.Fatalf("acks=%d want -1, body hex=%x", acks, body[:min(12, len(body))])
	}
}

func TestProduceRequestTransactionalIDEncoding(t *testing.T) {
	settings := protocol.ProduceSettings{
		Acks: -1, TimeoutMs: 30000,
		Transactional: true, TransactionalID: "my-txn",
	}
	body, err := protocol.EncodeProduceRequest([]protocol.ProduceRecord{
		{Topic: "t", Partition: 0, Value: []byte("x")},
	}, settings)
	if err != nil {
		t.Fatal(err)
	}
	buf := wire.FromBytes(body)
	txn, err := buf.ReadCompactString()
	if err != nil {
		t.Fatal(err)
	}
	if txn != "my-txn" {
		t.Fatalf("txn id=%q", txn)
	}
	acks, err := buf.ReadInt16()
	if err != nil {
		t.Fatal(err)
	}
	if acks != -1 {
		t.Fatalf("acks=%d", acks)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
