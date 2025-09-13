package app

import (
	"context"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/aws/internal/provider"
)

// App represents the AWS provider application
type App struct {
	logger      *zap.Logger
	awsProvider *provider.AWSProvider
}

// New creates a new AWS provider application instance
func New(
	logger *zap.Logger,
	awsProvider *provider.AWSProvider,
) *App {
	return &App{
		logger:      logger,
		awsProvider: awsProvider,
	}
}

// Start starts the AWS provider application
func (a *App) Start(ctx context.Context) error {
	a.logger.Info("Starting AWS provider application")
	if err := a.awsProvider.Start(ctx); err != nil {
		a.logger.Error("Failed to start AWS provider", zap.Error(err))
		return err
	}
	return nil
}

// Stop stops the AWS provider application
func (a *App) Stop(ctx context.Context) error {
	a.logger.Info("Stopping AWS provider application")
	if err := a.awsProvider.Stop(); err != nil {
		a.logger.Error("Failed to stop AWS provider", zap.Error(err))
		return err
	}
	a.logger.Info("AWS provider application stopped successfully")
	return nil
}
