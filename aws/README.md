# Themelio AWS Provider

AWS provider implementation for Themelio. This module handles AWS-specific resource management.

## Overview

The AWS provider is responsible for:

- Creating and managing AWS VPCs
- Managing AWS subnets
- Handling AWS-specific configurations
- Translating generic resources to AWS resources

## Components

- **Provider**: Main AWS provider implementation
- **Types**: AWS-specific resource types (VPC, Subnet)

## Building

```bash
go build -o bin/aws cmd/main.go
```

## Running

```bash
./bin/aws
```

## Configuration

AWS credentials and configuration will be handled through standard AWS SDK methods.
