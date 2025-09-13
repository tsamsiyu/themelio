package provider

import (
	"context"

	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/sdk/pkg/types"
)

// GCPProvider implements the cloud provider interface for GCP
type GCPProvider struct {
	logger *zap.Logger
}

// NewGCPProvider creates a new GCP provider instance
func NewGCPProvider(logger *zap.Logger) (*GCPProvider, error) {
	logger.Info("Creating GCP provider")
	return &GCPProvider{
		logger: logger,
	}, nil
}

// Start begins the provider's operation
func (p *GCPProvider) Start(ctx context.Context) error {
	p.logger.Info("Starting GCP provider")
	<-ctx.Done()
	p.logger.Info("GCP provider stopped")
	return nil
}

// Stop gracefully stops the provider
func (p *GCPProvider) Stop() error {
	p.logger.Info("Stopping GCP provider")
	return nil
}

// CreateNetwork creates a VPC in GCP
func (p *GCPProvider) CreateNetwork(ctx context.Context, network *types.Network) error {
	p.logger.Info("Creating GCP VPC",
		zap.String("name", network.Name),
		zap.String("cidr", network.Spec.CIDR),
		zap.String("region", network.Spec.Region))
	return nil
}

// DeleteNetwork deletes a VPC in GCP
func (p *GCPProvider) DeleteNetwork(ctx context.Context, network *types.Network) error {
	p.logger.Info("Deleting GCP VPC",
		zap.String("name", network.Name),
		zap.String("providerID", network.Status.ProviderID))
	return nil
}

// GetNetwork retrieves a VPC from GCP
func (p *GCPProvider) GetNetwork(ctx context.Context, network *types.Network) (*types.Network, error) {
	p.logger.Debug("Getting GCP VPC",
		zap.String("name", network.Name),
		zap.String("providerID", network.Status.ProviderID))
	return network, nil
}
