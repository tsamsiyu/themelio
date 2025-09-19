package gc

import (
	"context"

	"go.uber.org/zap"
)

type Worker struct {
	logger *zap.Logger
}

func NewWorker(logger *zap.Logger) *Worker {
	return &Worker{
		logger: logger,
	}
}

func (w *Worker) Start(ctx context.Context) error {
	return nil
}
