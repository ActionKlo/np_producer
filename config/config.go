package config

import (
	"github.com/spf13/viper"
	"log"
)

type Config struct {
	KafkaHost         string `mapstructure:"KAFKA_HOST"`
	KafkaExternalHost string `mapstructure:"KAFKA_EXTERNAL_HOST"`
	KafkaTopic        string `mapstructure:"KAFKA_TOPIC"`
	KafkaPartition    int    `mapstructure:"KAFKA_PARTITION"`
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
