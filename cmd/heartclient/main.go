package main

import (
	"context"
	"log"
	"os"

	"github.com/osamikoyo/leviathan/heartclient"
	"github.com/osamikoyo/leviathan/logger"
	"go.uber.org/zap"
)

func main() {
	url := "localhost:8080"

	logcfg := logger.Config{
		AppName:   "heartclient",
		LogLevel:  "debug",
		AddCaller: false,
		LogFile:   "logs/heartclient.log",
	}

	if err := logger.Init(logcfg); err != nil {
		log.Fatal(err)

		return
	}

	logger := logger.Get()

	logger.Info("starting heart client...",
		zap.String("url", url))

	cert := ""

	for i, arg := range os.Args {
		if arg == "--cert" {
			cert = os.Args[i+1]
		}
	}

	client, err := heartclient.NewHeartClient(url, logger, cert)
	if err != nil || client == nil {
		logger.Fatal("failed get heart client",
			zap.Error(err))

		return
	}

	res, err := client.RouteReadRequest(context.Background(), "SELECT 1")
	if err != nil {
		logger.Fatal("failed send read request", zap.Error(err))
	}

	logger.Info("successfully sended", zap.Any("resp", res))
}
