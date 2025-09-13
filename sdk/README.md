# Themelio SDK

The SDK module contains all the generic resource types and interfaces used across Themelio providers.

## Overview

The SDK is responsible for:

- Defining generic resource types (Network, etc.)
- Providing common interfaces for cloud providers
- Kubernetes annotations for OpenAPI schema generation
- Shared utilities and constants

## Components

- **Types**: Generic resource definitions with k8s annotations
- **Interfaces**: Common interfaces for cloud providers

## Building

```bash
go build ./...
```

## Usage

Import the SDK in your provider:

```go
import "github.com/tsamsiyu/themelio/sdk/pkg/types"
```

## Resources

Currently, the following resources are defined:

- **Network**: Virtual network abstraction (VPC/VNet)
