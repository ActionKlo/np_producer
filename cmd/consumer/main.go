package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"100.66.158.79:9092", "100.66.158.79:9093", "100.66.158.79:9094"},
		Topic:     "test-topic",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(42)

	fmt.Println("Consumer started")

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
