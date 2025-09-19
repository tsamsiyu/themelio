package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/api/server"
	"github.com/tsamsiyu/themelio/api/internal/app"
)

func main() {
	app := fx.New(
		app.APIModule,
		fx.Invoke(startServer),
	)

	app.Run()
}

func startServer(srv *server.Server, logger *zap.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal")
		cancel()
	}()

	logger.Info("Starting Themelio API")
	if err := srv.Start(ctx); err != nil {
		logger.Error("Failed to start server", zap.Error(err))
		os.Exit(1)
	}
}
