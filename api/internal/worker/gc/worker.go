package gc

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/repository/types"
)

// DeletionEvent represents a resource that needs to be processed for deletion
type DeletionEvent struct {
	ObjectKey types.ObjectKey
	Timestamp time.Time
}

// Config holds configuration for the GC worker
type Config struct {
	PollInterval time.Duration
	Workers      int
}

// DefaultConfig returns default configuration for the GC worker
func DefaultConfig() *Config {
	return &Config{
		PollInterval: 1 * time.Second,
		Workers:      3,
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
	deletions, err := w.repo.ListDeletions(ctx)
	if err != nil {
		w.logger.Error("Failed to list deletions", zap.Error(err))
		return
	}

	if len(deletions) == 0 {
		return
	}

	w.logger.Debug("Found resources marked for deletion", zap.Int("count", len(deletions)))

	for _, objectKey := range deletions {
		event := DeletionEvent{
			ObjectKey: objectKey,
			Timestamp: time.Now(),
		}

		select {
		case w.eventChan <- event:
			w.logger.Debug("Sent deletion event to channel",
				zap.String("objectKey", objectKey.String()))
		case <-ctx.Done():
			return
		default:
			w.logger.Warn("Deletion event channel full, skipping event",
				zap.String("objectKey", objectKey.String()))
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
		zap.String("objectKey", event.ObjectKey.String()))

	resource, err := w.repo.Get(ctx, event.ObjectKey)
	if err != nil {
		w.logger.Error("Failed to get resource for deletion",
			zap.String("objectKey", event.ObjectKey.String()),
			zap.Error(err))
		return
	}

	if resource == nil {
		w.logger.Debug("Resource not found, skipping deletion",
			zap.String("objectKey", event.ObjectKey.String()))
		return
	}

	shouldSkip, err := w.shouldSkipDeletion(ctx, resource)
	if err != nil {
		w.logger.Error("Failed to check whether deletion should be skipped",
			zap.String("objectKey", event.ObjectKey.String()),
			zap.Error(err))
		return
	}

	if shouldSkip {
		w.logger.Debug("Skipping deletion", zap.String("objectKey", event.ObjectKey.String()))
		return
	}

	err = w.deleteResourceWithChildren(ctx, event.ObjectKey, resource)
	if err != nil {
		w.logger.Error("Failed to delete resource",
			zap.String("objectKey", event.ObjectKey.String()),
			zap.Error(err))
		return
	}

	w.logger.Info("Successfully deleted resource", zap.String("objectKey", event.ObjectKey.String()))
}

// shouldSkipDeletion checks if deletion should be skipped due to blocking owner references
func (w *Worker) shouldSkipDeletion(ctx context.Context, resource *unstructured.Unstructured) (bool, error) {
	if resource.GetFinalizers() != nil && len(resource.GetFinalizers()) > 0 {
		return true, nil // resource has finalizers, skipping deletion
	}

	ownerRefs := resource.GetOwnerReferences()
	for _, ownerRef := range ownerRefs {
		if ownerRef.BlockOwnerDeletion != nil && *ownerRef.BlockOwnerDeletion {
			parentKey := types.OwnerRefToObjectKey(ownerRef, resource.GetNamespace())
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

// deleteResourceWithChildren deletes a resource and handles its children
// it cleans up owner references of children who reference the deleted resource;
// it also marks children for deletion if they do not have any other owner references that are blocking deletion
func (w *Worker) deleteResourceWithChildren(ctx context.Context, objectKey types.ObjectKey, object *unstructured.Unstructured) error {
	childKeys, err := w.repo.GetReversedOwnerReferences(ctx, objectKey)
	if err != nil {
		return errors.Wrap(err, "failed to get reversed owner references")
	}

	var childrenToDelete []types.ObjectKey
	var childrenToUpdate []struct {
		key    types.ObjectKey
		remove bool
	}

	for childKeyStr := range childKeys {
		childKey, err := types.ParseObjectKey(childKeyStr)
		if err != nil {
			w.logger.Error("Failed to parse child object key",
				zap.String("childKey", childKeyStr),
				zap.Error(err))
			continue
		}

		child, err := w.repo.Get(ctx, childKey)
		if err != nil {
			w.logger.Error("Failed to get child resource",
				zap.String("childKey", childKey.String()),
				zap.Error(err))
			continue
		}

		if child == nil {
			continue
		}

		childOwnerRefs := child.GetOwnerReferences()
		hasBlockingParent := false

		for _, childOwnerRef := range childOwnerRefs {
			if childOwnerRef.UID == object.GetUID() { // skip reference to object itself
				continue
			}

			if childOwnerRef.BlockOwnerDeletion != nil && *childOwnerRef.BlockOwnerDeletion {
				hasBlockingParent = true
				break
			}
		}

		if hasBlockingParent {
			childrenToUpdate = append(childrenToUpdate, struct {
				key    types.ObjectKey
				remove bool
			}{childKey, true})
		} else {
			childrenToDelete = append(childrenToDelete, childKey)
		}
	}

	var removeReferencesKeys []types.ObjectKey
	for _, childUpdate := range childrenToUpdate {
		removeReferencesKeys = append(removeReferencesKeys, childUpdate.key)
	}

	return w.repo.Delete(ctx, objectKey, childrenToDelete, removeReferencesKeys)
}
