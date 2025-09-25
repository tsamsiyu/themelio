package gc

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/repository"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

// DeletionEvent represents a resource that needs to be processed for deletion
type DeletionEvent struct {
	ObjectKey sdkmeta.ObjectKey
	Timestamp time.Time
}

// Config holds configuration for the GC worker
type Config struct {
	PollInterval time.Duration
	Workers      int
	LockKey      string
	LockExp      time.Duration
	BatchLimit   int
}

// DefaultConfig returns default configuration for the GC worker
func DefaultConfig() *Config {
	return &Config{
		PollInterval: 1 * time.Second,
		Workers:      3,
		LockKey:      "gc-worker", // todo: generate randomly
		LockExp:      5 * time.Minute,
		BatchLimit:   10,
	}
}

// Worker is responsible for deleting resources and cleaning up owner references of children who reference the deleted resource
// It picks up resources marked for deletion
// It does not delete children resource, it marks them for deletion
// This is the single source of truth for deleting resources
type Worker struct {
	logger    *zap.Logger
	repo      repository.ResourceRepository
	config    *Config
	eventChan chan DeletionEvent
	stopChan  chan struct{}
}

func NewWorker(logger *zap.Logger, repo repository.ResourceRepository, config *Config) *Worker {
	if config == nil {
		config = DefaultConfig()
	}

	return &Worker{
		logger:    logger,
		repo:      repo,
		config:    config,
		eventChan: make(chan DeletionEvent, 100),
		stopChan:  make(chan struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Starting GC Worker",
		zap.Duration("pollInterval", w.config.PollInterval),
		zap.Int("workers", w.config.Workers))

	defer close(w.stopChan)
	defer close(w.eventChan)

	go w.producer(ctx)

	for i := 0; i < w.config.Workers; i++ {
		go w.consumer(ctx, i)
	}

	<-ctx.Done()

	w.logger.Info("GC Worker has stopped")

	return nil
}

// producer regularly queries ListDeletions and sends events to the channel
func (w *Worker) producer(ctx context.Context) {
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.pollDeletions(ctx)
		}
	}
}

// pollDeletions queries the repository for resources marked for deletion
func (w *Worker) pollDeletions(ctx context.Context) {
	deletionBatch, err := w.repo.ListDeletions(ctx, w.config.LockKey, w.config.LockExp, w.config.BatchLimit)
	if err != nil {
		w.logger.Error("Failed to list deletions", zap.Error(err))
		return
	}

	if len(deletionBatch.ObjectKeys) == 0 {
		return
	}

	w.logger.Debug("Found resources marked for deletion", zap.Int("count", len(deletionBatch.ObjectKeys)))

	for _, objectKey := range deletionBatch.ObjectKeys {
		event := DeletionEvent{
			ObjectKey: objectKey,
			Timestamp: time.Now(),
		}

		select {
		case w.eventChan <- event:
			w.logger.Debug("Sent deletion event to channel", zap.Any("objectKey", objectKey))
		case <-ctx.Done():
			return
		default:
			w.logger.Warn("Deletion event channel full, skipping event", zap.Any("objectKey", objectKey))
		}
	}
}

// consumer processes deletion events from the channel
func (w *Worker) consumer(ctx context.Context, workerID int) {
	w.logger.Debug("Starting GC consumer", zap.Int("workerID", workerID))

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("GC consumer stopping", zap.Int("workerID", workerID))
			return
		case event, ok := <-w.eventChan:
			if !ok {
				w.logger.Debug("Deletion event channel closed", zap.Int("workerID", workerID))
				return
			}
			w.processDeletionEvent(ctx, event, workerID)
		}
	}
}

// processDeletionEvent handles a single deletion event
func (w *Worker) processDeletionEvent(ctx context.Context, event DeletionEvent, workerID int) {
	w.logger.Debug("Processing deletion event",
		zap.Int("workerID", workerID),
		zap.Any("objectKey", event.ObjectKey))

	resource, err := w.repo.Get(ctx, event.ObjectKey)
	if err != nil {
		w.logger.Error("Failed to get resource for deletion",
			zap.Any("objectKey", event.ObjectKey),
			zap.Error(err))
		return
	}

	if resource == nil {
		w.logger.Debug("Resource not found, skipping deletion",
			zap.Any("objectKey", event.ObjectKey))
		return
	}

	shouldSkip, err := w.shouldSkipDeletion(ctx, resource)
	if err != nil {
		w.logger.Error("Failed to check whether deletion should be skipped",
			zap.Any("objectKey", event.ObjectKey),
			zap.Error(err))
		return
	}

	if shouldSkip {
		w.logger.Debug("Skipping deletion", zap.Any("objectKey", event.ObjectKey))
		return
	}

	err = w.repo.Delete(ctx, event.ObjectKey, w.config.LockKey)
	if err != nil {
		w.logger.Error("Failed to delete resource", zap.Any("objectKey", event.ObjectKey), zap.Error(err))
		return
	}

	w.logger.Info("Successfully deleted resource", zap.Any("objectKey", event.ObjectKey))
}

// shouldSkipDeletion checks if deletion should be skipped due to blocking owner references
func (w *Worker) shouldSkipDeletion(ctx context.Context, resource *sdkmeta.Object) (bool, error) {
	if len(resource.ObjectMeta.Finalizers) > 0 {
		return true, nil
	}

	ownerRefs := resource.ObjectMeta.OwnerReferences
	for _, ownerRef := range ownerRefs {
		if ownerRef.BlockOwnerDeletion {
			parentKey := ownerRef.ToObjectKey()
			parent, err := w.repo.Get(ctx, parentKey)
			if err != nil {
				return false, err
			}
			if parent != nil {
				return true, nil // parent resource is blocking deletion
			}
		}
	}

	return false, nil
}
