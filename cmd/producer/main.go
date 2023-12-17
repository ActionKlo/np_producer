package main

import (
	"np_producer/config"
	"np_producer/internal/kafka"
	"np_producer/logger"
)

func main() {
	log := logger.Init()
	cfg := config.New()

	ks := kafka.NewKafka(log, cfg)
	if err := ks.Produce(); err != nil {
		log.Fatal("kafka producer fall dawn")
	}
	// ks.Comnsume(log, cfg) // ks.WhatEver(log, cfg) ??????????????
}
