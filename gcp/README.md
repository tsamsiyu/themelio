# Themelio GCP Provider

Google Cloud Platform provider implementation for Themelio. This module handles GCP-specific resource management.

## Overview

The GCP provider is responsible for:

- Creating and managing GCP VPCs
- Managing GCP subnets
- Handling GCP-specific configurations
- Translating generic resources to GCP resources

## Components

- **Provider**: Main GCP provider implementation
- **Types**: GCP-specific resource types (VPC, Subnet)

## Building

```bash
go build -o bin/gcp cmd/main.go
```

## Running

```bash
./bin/gcp
```

## Configuration

GCP credentials and configuration will be handled through standard GCP SDK methods.
