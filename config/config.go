package config

import (
	"flag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"log"
	"np_producer/internal/kafka"
)

type Config struct {
	KafkaHost         string `mapstructure:"KAFKA_HOST"`
	KafkaExternalHost string `mapstructure:"KAFKA_EXTERNAL_HOST"`
	KafkaTopic        string `mapstructure:"KAFKA_TOPIC"`
	KafkaPartition    int    `mapstructure:"KAFKA_PARTITION"`
}

type Services struct {
	KafkaService *kafka.ServiceKafka
}

func New() *Config {
	var appConfig Config
	v := viper.New()
	v.SetConfigType("env")
	v.AddConfigPath(".")
	v.SetConfigName(".env")
	v.AutomaticEnv()
	if err := v.ReadInConfig(); err != nil {
		log.Fatal(err)
	}
	if err := v.Unmarshal(&appConfig); err != nil {
		log.Fatal(err)
	}
	return &appConfig
}

func (c *Config) NewKafkaService(logger *zap.Logger) *Services {
	kafkaConfig := kafka.New(logger, &kafka.Config{
		KafkaHost:         c.KafkaHost,
		KafkaExternalHost: c.KafkaExternalHost,
		KafkaTopic:        c.KafkaTopic,
		KafkaPartition:    0,
	})

	return &Services{
		KafkaService: kafkaConfig,
	}
}

type Flags struct {
	CountOrders int
	Delay       int
}

func (c *Config) ParseFlags() *Flags {
	var (
		countOrders int
		delay       int
	)

	flag.IntVar(&countOrders, "n", 1, "Count of orders")
	flag.IntVar(&delay, "d", 3, "Delay between orders")
	flag.Parse()

	return &Flags{
		CountOrders: countOrders,
		Delay:       delay,
	}
}
