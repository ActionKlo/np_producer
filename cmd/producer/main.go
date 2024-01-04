package main

import (
	"np_producer/config"
	"np_producer/logger"
)

func main() {
	log := logger.Init()
	cfg := config.New()

	flags := cfg.ParseFlags()

	ks := cfg.NewKafkaService(log).KafkaService

	if err := ks.Produce(flags.CountOrders, flags.Delay); err != nil {
		log.Fatal("kafka producer fall dawn")
	}
}
