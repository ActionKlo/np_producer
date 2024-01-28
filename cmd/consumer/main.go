package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"time"
)

type (
	Address struct {
		AddressID uuid.UUID
		Country   string
		City      string
		Street    string
		Zip       string
	}

	Customer struct {
		CustomerID  uuid.UUID
		Name        string
		LastName    string
		Email       string
		PhoneNumber string
		Address     Address
	}
	Sender struct {
		SenderID    uuid.UUID
		Name        string
		LastName    string
		Email       string
		PhoneNumber string
		Address     Address
	}

	Event struct {
		EventID          uuid.UUID
		EventTime        time.Time
		EventDescription string
	}

	Order struct {
		OrderID uuid.UUID
		Size    string
		Weight  float64
		Count   int

		Customer Customer
		Sender   Sender

		Event Event
	}
)

type (
	Payload struct {
		MessageID  uuid.UUID
		OrderID    uuid.UUID
		EventID    uuid.UUID
		EventDesc  string
		EventTime  time.Time
		ReceiverID uuid.UUID
		Data       json.RawMessage
		Settings   Settings
	}

	Settings struct {
		SettingID uuid.UUID
		Url       string
	}
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{os.Getenv("HOST")},
		//GroupID:   "test-group",
		Topic:     "test-topic",
		Partition: 0,
		MaxBytes:  10e6, // 10M
	})

	//if err := r.SetOffset(678); err != nil {
	//	log.Fatal(err)
	//}

	fmt.Println("Consumer started")

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		var payload Payload
		if err = json.Unmarshal(m.Value, &payload); err != nil {
			log.Fatal(err)
		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
