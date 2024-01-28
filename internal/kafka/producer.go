package kafka

import (
	"context"
	"encoding/json"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"strconv"
	"sync"
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

	Receiver struct {
		ReceiverID  uuid.UUID
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

	Order struct {
		OrderID uuid.UUID
		Size    string
		Weight  int
		Count   int

		Receiver Receiver
		Sender   Sender
	}

	Payload struct {
		MessageID      uuid.UUID
		EventID        uuid.UUID
		EventType      string
		EventTime      time.Time
		TrackingNumber string
		Order          Order
		ReceiverID     uuid.UUID
	}
)

var eventType = [6]string{
	"order processed",
	"dispatched from warehouse",
	"in transit to local distribution center",
	"arrived at local distribution center",
	"out for delivery",
	"delivery successfully",
}

func newMessage(conn *kafka.Conn) error {
	order := Order{
		Size: strconv.Itoa(gofakeit.IntRange(5, 500)) + "x" +
			strconv.Itoa(gofakeit.IntRange(5, 230)) + "x" +
			strconv.Itoa(gofakeit.IntRange(5, 210)),
		Weight: gofakeit.IntRange(1, 1200),
		Count:  gofakeit.IntRange(1, 5),
		Receiver: Receiver{
			ReceiverID:  uuid.MustParse("10899528-d8a6-49c4-ab1f-2f02b98811dc"),
			Name:        gofakeit.Name(),
			LastName:    gofakeit.LastName(),
			Email:       gofakeit.Email(),
			PhoneNumber: gofakeit.Phone(),
			Address: Address{
				AddressID: uuid.New(),
				Country:   gofakeit.Country(),
				City:      gofakeit.City(),
				Street:    gofakeit.Street(),
				Zip:       gofakeit.Zip(),
			},
		},
		Sender: Sender{
			SenderID:    uuid.New(),
			Name:        gofakeit.Name(),
			LastName:    gofakeit.LastName(),
			Email:       gofakeit.Email(),
			PhoneNumber: gofakeit.Phone(),
			Address: Address{
				AddressID: uuid.New(),
				Country:   gofakeit.Country(),
				City:      gofakeit.City(),
				Street:    gofakeit.Street(),
				Zip:       gofakeit.Zip(),
			},
		},
	}

	payload := &Payload{
		TrackingNumber: gofakeit.UUID(),
		Order:          order,
		ReceiverID:     order.Receiver.ReceiverID,
	}

	for _, e := range eventType {
		payload.MessageID = uuid.New()
		payload.EventID = uuid.New()
		payload.EventType = e
		payload.EventTime = time.Now()

		m, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		_, err = conn.WriteMessages(kafka.Message{
			Value: m,
		})
		if err != nil {
			return err
		}

		time.Sleep(time.Duration(gofakeit.IntRange(2, 8)) * time.Second)
	}

	return nil
}

type Config struct {
	KafkaHost         string
	KafkaExternalHost string
	KafkaTopic        string
	KafkaPartition    int
}

type ServiceKafka struct {
	logger *zap.Logger
	config *Config
}

func New(logger *zap.Logger, cfg *Config) *ServiceKafka {
	return &ServiceKafka{
		logger: logger,
		config: cfg,
	}
}

func (k *ServiceKafka) Produce(countMessages int, delay int) error {
	k.logger.Info("kafka producer started")

	conn, err := kafka.DialLeader(context.Background(),
		"tcp",
		k.config.KafkaExternalHost,
		k.config.KafkaTopic,
		k.config.KafkaPartition)
	if err != nil {
		k.logger.Error("failed to create new DialLeader")
		return err
	}

	var wg sync.WaitGroup

	for i := 0; i < countMessages; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err = newMessage(conn); err != nil {
				k.logger.Error("failed to write message:", zap.Error(err))
			}
		}()

		time.Sleep(time.Duration(delay))
	}

	wg.Wait()

	if err := conn.Close(); err != nil {
		k.logger.Error("failed to close writer:")
		return err
	}

	k.logger.Info("kafka producer ended")
	return nil
}
