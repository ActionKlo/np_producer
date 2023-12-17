package logger

import (
	"go.uber.org/zap"
	"log"
)

func Init() *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	return logger
}
