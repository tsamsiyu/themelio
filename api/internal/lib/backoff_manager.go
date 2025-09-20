package lib

import (
	"math/rand"
	"time"
)

type BackoffConfig struct {
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	ResetAfter        time.Duration
}

type BackoffManager struct {
	config         BackoffConfig
	currentBackoff time.Duration
	lastReset      time.Time
}

func NewBackoffManager(config BackoffConfig) *BackoffManager {
	return &BackoffManager{
		config:         config,
		currentBackoff: config.InitialBackoff,
		lastReset:      time.Now(),
	}
}

func (b *BackoffManager) NextBackoff() time.Duration {
	now := time.Now()
	if now.Sub(b.lastReset) > b.config.ResetAfter {
		b.currentBackoff = b.config.InitialBackoff
		b.lastReset = now
	}

	backoff := b.currentBackoff
	jitter := time.Duration(rand.Float64() * float64(backoff) * 0.1)
	backoff += jitter

	b.currentBackoff = time.Duration(float64(b.currentBackoff) * b.config.BackoffMultiplier)
	if b.currentBackoff > b.config.MaxBackoff {
		b.currentBackoff = b.config.MaxBackoff
	}

	return backoff
}

func (b *BackoffManager) Reset() {
	b.currentBackoff = b.config.InitialBackoff
	b.lastReset = time.Now()
}
