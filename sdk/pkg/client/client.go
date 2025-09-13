package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// httpClient implements the Client interface using HTTP
type httpClient struct {
	config  *Config
	client  *http.Client
	baseURL *url.URL
}

// NewClient creates a new HTTP client for the Themelio API
func NewClient(config *Config) (Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if config.BaseURL == "" {
		return nil, fmt.Errorf("base URL is required")
	}

	baseURL, err := url.Parse(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %v", err)
	}

	timeout := time.Duration(config.Timeout) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	client := &http.Client{
		Timeout: timeout,
	}

	if config.TLS.InsecureSkipVerify || config.TLS.CertFile != "" || config.TLS.KeyFile != "" || config.TLS.CAFile != "" {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: config.TLS.InsecureSkipVerify,
		}

		if config.TLS.CertFile != "" && config.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(config.TLS.CertFile, config.TLS.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		client.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	return &httpClient{
		config:  config,
		client:  client,
		baseURL: baseURL,
	}, nil
}

// ReplaceResource creates or updates a resource
func (c *httpClient) ReplaceResource(ctx context.Context, gvk GroupVersionKind, resource *Object) error {
	if resource == nil {
		return fmt.Errorf("resource cannot be nil")
	}

	jsonData, err := json.Marshal(resource)
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %v", err)
	}

	path := fmt.Sprintf("/api/v1/resources/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)

	req, err := http.NewRequestWithContext(ctx, "PUT", c.baseURL.ResolveReference(&url.URL{Path: path}).String(), bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.setCustomHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return c.handleErrorResponse(resp)
	}

	return nil
}

// GetResource retrieves a specific resource
func (c *httpClient) GetResource(ctx context.Context, gvk GroupVersionKind, namespace, name string) (*Object, error) {
	if namespace == "" {
		namespace = "default"
	}
	if name == "" {
		return nil, fmt.Errorf("resource name is required")
	}

	path := fmt.Sprintf("/api/v1/resources/%s/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, namespace, name)

	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL.ResolveReference(&url.URL{Path: path}).String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	c.setCustomHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	var resource Object
	if err := json.NewDecoder(resp.Body).Decode(&resource); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return &resource, nil
}

// ListResources lists resources of a specific type
func (c *httpClient) ListResources(ctx context.Context, gvk GroupVersionKind, namespace string) ([]*Object, error) {
	if namespace == "" {
		namespace = "default"
	}

	path := fmt.Sprintf("/api/v1/resources/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, namespace)

	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL.ResolveReference(&url.URL{Path: path}).String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	c.setCustomHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	var listResponse ListResponse
	if err := json.NewDecoder(resp.Body).Decode(&listResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return listResponse.Items, nil
}

// DeleteResource deletes a specific resource
func (c *httpClient) DeleteResource(ctx context.Context, gvk GroupVersionKind, namespace, name string) error {
	if namespace == "" {
		namespace = "default"
	}
	if name == "" {
		return fmt.Errorf("resource name is required")
	}

	path := fmt.Sprintf("/api/v1/resources/%s/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, namespace, name)

	req, err := http.NewRequestWithContext(ctx, "DELETE", c.baseURL.ResolveReference(&url.URL{Path: path}).String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	c.setCustomHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return c.handleErrorResponse(resp)
	}

	return nil
}

func (c *httpClient) setCustomHeaders(req *http.Request) {
	for key, value := range c.config.Headers {
		req.Header.Set(key, value)
	}
}

func (c *httpClient) handleErrorResponse(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &Error{
			Message:    fmt.Sprintf("HTTP %d: failed to read error response", resp.StatusCode),
			StatusCode: resp.StatusCode,
		}
	}

	var errorResponse map[string]interface{}
	if err := json.Unmarshal(body, &errorResponse); err != nil {
		return &Error{
			Message:    fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(body)),
			StatusCode: resp.StatusCode,
		}
	}

	message := fmt.Sprintf("HTTP %d", resp.StatusCode)
	if msg, ok := errorResponse["message"].(string); ok {
		message = msg
	} else if msg, ok := errorResponse["error"].(string); ok {
		message = msg
	}

	return &Error{
		Message:    message,
		StatusCode: resp.StatusCode,
		Details:    errorResponse,
	}
}
