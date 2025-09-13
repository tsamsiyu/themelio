package main

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/tsamsiyu/themelio/gcp/internal/app"
	"github.com/tsamsiyu/themelio/gcp/internal/provider"
)

func main() {
	app := fx.New(
		fx.Provide(NewLogger),
		fx.Provide(provider.NewGCPProvider),
		fx.Provide(app.New),
		fx.Invoke(StartApp),
	)
	app.Run()
}

// NewLogger creates a new zap logger
func NewLogger() (*zap.Logger, error) {
	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	zapConfig.Encoding = "json"
	zapConfig.EncoderConfig.TimeKey = "timestamp"
	zapConfig.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	return zapConfig.Build()
}

// StartApp starts the application
func StartApp(lc fx.Lifecycle, app *app.App, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting Themelio GCP Provider")
			return app.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping Themelio GCP Provider")
			return app.Stop(ctx)
		},
	})
}
