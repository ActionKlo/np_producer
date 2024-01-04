package kafka

import (
	"context"
	"encoding/json"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
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

	Shipment struct {
		ShipmentID uuid.UUID
		Size       string
		Weight     float64
		Count      int

		Customer Customer
		Sender   Sender

		Event Event
	}
)

var eventDescription = [6]string{
	"order processed",
	"dispatched from warehouse",
	"in transit to local distribution center",
	"arrived at local distribution center",
	"out for delivery",
	"delivery successfully",
}

func NewOrder(conn *kafka.Conn) error {
	message := &Shipment{
		ShipmentID: uuid.New(),
		Size:       "euro palleta",
		Weight:     gofakeit.Float64Range(100, 10000),
		Count:      gofakeit.IntRange(1, 40),
		Customer: Customer{
			CustomerID:  uuid.New(),
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
		Event: Event{
			EventID:          uuid.New(),
			EventTime:        time.Now(),
			EventDescription: eventDescription[0],
		},
	}

	for _, e := range eventDescription {
		message.Event.EventDescription = e
		message.Event.EventID = uuid.New()

		m, err := json.Marshal(message)
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

			if err = NewOrder(conn); err != nil {
				k.logger.Error("failed to write message:", zap.Error(err))
			}
		}()

		time.Sleep(time.Duration(delay))
	}

	wg.Wait()

	if err = conn.Close(); err != nil {
		k.logger.Error("failed to close writer:")
		return err
	}

	k.logger.Info("kafka producer ended")
	return nil
}
