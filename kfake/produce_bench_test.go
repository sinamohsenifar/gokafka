package kfake_test

import (
	"context"
	"testing"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/kfake"
)

func benchProduceRecords(n int) []gokafka.Record {
	val := []byte("a representative kafka record value of moderate size ~ 64 bytes!!")
	recs := make([]gokafka.Record, n)
	for i := range recs {
		recs[i] = gokafka.Record{Topic: "bench", Key: []byte("key-0000"), Value: val}
	}
	return recs
}

func benchProduceSync(b *testing.B, n int) {
	broker, err := kfake.NewBroker()
	if err != nil {
		b.Fatal(err)
	}
	defer broker.Close()
	broker.AddTopic("bench", 1)
	cfg, err := gokafka.NewConfig([]string{broker.Addr()})
	if err != nil {
		b.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cli.Close()
	recs := benchProduceRecords(n)
	ctx := context.Background()
	prod := cli.Producer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := prod.ProduceSync(ctx, recs...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProduceSync1(b *testing.B)    { benchProduceSync(b, 1) }
func BenchmarkProduceSync1000(b *testing.B) { benchProduceSync(b, 1000) }
