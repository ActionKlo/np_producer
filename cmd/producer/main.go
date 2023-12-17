package main

import (
	"flag"
	"np_producer/config"
	"np_producer/internal/kafka"
	"np_producer/logger"
)

func main() {
	log := logger.Init()
	cfg := config.New()

	var countMessages int
	flag.IntVar(&countMessages, "n", 100, "Count of messages")
	flag.Parse()

	ks := kafka.NewKafka(log, cfg)
	if err := ks.Produce(countMessages); err != nil {
		log.Fatal("kafka producer fall dawn")
	}
	// ks.Comnsume(log, cfg) // ks.WhatEver(log, cfg) ??????????????
}
