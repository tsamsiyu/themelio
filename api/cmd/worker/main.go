package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/app"
	"github.com/tsamsiyu/themelio/api/internal/worker/gc"
)

func main() {
	app := fx.New(
		app.WorkerModule,
		fx.Invoke(startWorker),
	)

	app.Run()
}

func startWorker(worker *gc.Worker, logger *zap.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal")
		cancel()
	}()

	logger.Info("Starting Themelio Worker")
	if err := worker.Start(ctx); err != nil {
		logger.Error("Failed to start worker", zap.Error(err))
		os.Exit(1)
	}
}
