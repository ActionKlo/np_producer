package kafka

import (
	"context"
	"encoding/json"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"np_producer/config"
	"sync"
	"time"
)

type Message struct {
	ID          string    `json:"ID"`
	Time        time.Time `json:"Time"`
	Sender      string    `json:"Sender"`
	Status      string    `json:"Status"`
	TrackNumber string    `json:"TrackNumber"`
	Country     string    `json:"Country"`
	City        string    `json:"City"`
	Street      string    `json:"Street"`
	PostCode    string    `json:"PostCode"`
}

var status = [6]string{
	"order processed",
	"dispatched from warehouse",
	"in transit to local distribution center",
	"arrived at local distribution center",
	"out for delivery",
	"delivery successfully",
}

func newMessage(conn *kafka.Conn) error {
	message := &Message{
		ID:          gofakeit.UUID(),
		Sender:      gofakeit.Company(),
		Time:        time.Now(),
		TrackNumber: gofakeit.CreditCardNumber(nil),
		PostCode:    gofakeit.Zip(),
		Country:     gofakeit.Country(),
		City:        gofakeit.City(),
		Street:      gofakeit.Street(),
	}

	for _, s := range status {
		message.Status = s

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

type ServiceKafka struct {
	logger *zap.Logger
	config *config.Config
}

func NewKafka(logger *zap.Logger, cfg *config.Config) *ServiceKafka {
	return &ServiceKafka{
		logger: logger,
		config: cfg,
	}
}

func (k *ServiceKafka) Produce() error {
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

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := newMessage(conn); err != nil {
				k.logger.Error("failed to write message:", zap.Error(err))
			}
		}()

		time.Sleep(time.Duration(gofakeit.IntRange(3, 7)) * time.Second)
	}

	wg.Wait()

	if err := conn.Close(); err != nil {
		k.logger.Error("failed to close writer:")
		return err
	}

	k.logger.Info("kafka producer ended")
	return nil
}
