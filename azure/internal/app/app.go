package app

import (
	"context"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/azure/internal/provider"
)

// App represents the Azure provider application
type App struct {
	logger       *zap.Logger
	azureProvider *provider.AzureProvider
}

// New creates a new Azure provider application instance
func New(
	logger *zap.Logger,
	azureProvider *provider.AzureProvider,
) *App {
	return &App{
		logger:       logger,
		azureProvider: azureProvider,
	}
}

// Start starts the Azure provider application
func (a *App) Start(ctx context.Context) error {
	a.logger.Info("Starting Azure provider application")
	if err := a.azureProvider.Start(ctx); err != nil {
		a.logger.Error("Failed to start Azure provider", zap.Error(err))
		return err
	}
	return nil
}

// Stop stops the Azure provider application
func (a *App) Stop(ctx context.Context) error {
	a.logger.Info("Stopping Azure provider application")
	if err := a.azureProvider.Stop(); err != nil {
		a.logger.Error("Failed to stop Azure provider", zap.Error(err))
		return err
	}
	a.logger.Info("Azure provider application stopped successfully")
	return nil
}