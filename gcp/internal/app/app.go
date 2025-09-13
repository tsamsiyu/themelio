package app

import (
	"context"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/gcp/internal/provider"
)

// App represents the GCP provider application
type App struct {
	logger      *zap.Logger
	gcpProvider *provider.GCPProvider
}

// New creates a new GCP provider application instance
func New(
	logger *zap.Logger,
	gcpProvider *provider.GCPProvider,
) *App {
	return &App{
		logger:      logger,
		gcpProvider: gcpProvider,
	}
}

// Start starts the GCP provider application
func (a *App) Start(ctx context.Context) error {
	a.logger.Info("Starting GCP provider application")
	if err := a.gcpProvider.Start(ctx); err != nil {
		a.logger.Error("Failed to start GCP provider", zap.Error(err))
		return err
	}
	return nil
}

// Stop stops the GCP provider application
func (a *App) Stop(ctx context.Context) error {
	a.logger.Info("Stopping GCP provider application")
	if err := a.gcpProvider.Stop(); err != nil {
		a.logger.Error("Failed to stop GCP provider", zap.Error(err))
		return err
	}
	a.logger.Info("GCP provider application stopped successfully")
	return nil
}