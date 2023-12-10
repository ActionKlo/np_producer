package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"sync"
	"time"
)

type status string

/*
 */
const (
	zero   status = "order processed"
	first  status = "dispatched from warehouse"
	second status = "in transit to local distribution center"
	third  status = "arrived at local distribution center"
	forth  status = "out for delivery"
	fifth  status = "delivery successfully"
)

type Message struct {
	ID          string    `json:"ID"`
	Time        time.Time `json:"Time"`
	Sender      string    `json:"Sender"`
	Status      status    `json:"Status"`
	TrackNumber string    `json:"TrackNumber"`
	Country     string    `json:"Country"`
	City        string    `json:"City"`
	Street      string    `json:"Street"`
	PostCode    string    `json:"PostCode"`
}

func newMessage(conn *kafka.Conn) error {
	message := &Message{
		ID:          gofakeit.UUID(),
		Sender:      gofakeit.Company(),
		Time:        time.Now(),
		Status:      zero,
		TrackNumber: gofakeit.CreditCardNumber(nil),
		PostCode:    gofakeit.Zip(),
		Country:     gofakeit.Country(),
		City:        gofakeit.City(),
		Street:      gofakeit.Street(),
	}

	//if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
	//	return err
	//}

	m, err := json.Marshal(message)

	_, err = conn.WriteMessages(kafka.Message{
		Value: m,
	})
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(gofakeit.IntRange(5, 15)) * time.Second)

	message.Status = first
	m, err = json.Marshal(message)

	_, err = conn.WriteMessages(kafka.Message{
		Value: m,
	})
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(gofakeit.IntRange(5, 15)) * time.Second)

	message.Status = second
	m, err = json.Marshal(message)

	_, err = conn.WriteMessages(kafka.Message{
		Value: m,
	})
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(gofakeit.IntRange(5, 15)) * time.Second)

	message.Status = third
	m, err = json.Marshal(message)

	_, err = conn.WriteMessages(kafka.Message{
		Value: m,
	})
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(gofakeit.IntRange(5, 15)) * time.Second)

	message.Status = forth
	m, err = json.Marshal(message)

	_, err = conn.WriteMessages(kafka.Message{
		Value: m,
	})
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(gofakeit.IntRange(5, 15)) * time.Second)

	message.Status = fifth
	m, err = json.Marshal(message)

	_, err = conn.WriteMessages(kafka.Message{
		Value: m,
	})
	if err != nil {
		return err
	}

	return nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(os.Getenv("HOST"))
	topic := "test-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", os.Getenv("HOST"), topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := newMessage(conn); err != nil {
				fmt.Println(err)
			}
		}()
		time.Sleep(7 * time.Second)
	}

	wg.Wait()

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
