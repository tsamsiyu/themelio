package service

import "time"

// WatchConfig contains configuration for the ResourceWatchService
type WatchConfig struct {
	// PollInterval defines how often to poll for missing events
	PollInterval time.Duration

	// CacheTimeout defines when to remove stale cache entries
	CacheTimeout time.Duration

	// DisableBackground disables the background reconciliation process (for testing)
	DisableBackground bool
}

// DefaultWatchConfig returns a default configuration
func DefaultWatchConfig() *WatchConfig {
	return &WatchConfig{
		PollInterval: 30 * time.Second,
		CacheTimeout: 5 * time.Minute,
	}
}
