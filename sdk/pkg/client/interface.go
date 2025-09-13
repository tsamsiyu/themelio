package client

import (
	"context"
	"io"
)

type GroupVersionKind struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

// Client defines the interface for interacting with the Themelio API
type Client interface {
	ReplaceResource(ctx context.Context, gvk GroupVersionKind, resource *Object) error
	GetResource(ctx context.Context, gvk GroupVersionKind, namespace, name string) (*Object, error)
	ListResources(ctx context.Context, gvk GroupVersionKind, namespace string) ([]*Object, error)
	DeleteResource(ctx context.Context, gvk GroupVersionKind, namespace, name string) error
}

// Config holds configuration for the client
type Config struct {
	BaseURL string
	Timeout int
	Headers map[string]string
	TLS     TLSConfig
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	InsecureSkipVerify bool
	CertFile           string
	KeyFile            string
	CAFile             string
}

// Response represents a generic API response
type Response struct {
	StatusCode int
	Headers    map[string][]string
	Body       io.ReadCloser
}

// ListResponse represents a list response from the API
type ListResponse struct {
	Items []*Object `json:"items"`
	Total int       `json:"total"`
}

// Error represents a client error
type Error struct {
	Message    string
	StatusCode int
	Details    map[string]interface{}
}

func (e *Error) Error() string {
	return e.Message
}
