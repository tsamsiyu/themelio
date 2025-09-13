# Themelio Reconciler

The core reconciliation engine for Themelio. This module handles the reconciliation pattern implementation and etcd integration.

## Overview

The reconciler is responsible for:

- Managing the desired state of resources
- Comparing desired state with actual state
- Triggering reconciliation actions
- Storing state in etcd
- Coordinating with cloud providers

## Components

- **Controller**: Main reconciliation controller
- **ETCD Client**: State storage integration
- **Types**: Generic resource definitions

## Building

```bash
go build -o bin/reconciler cmd/main.go
```

## Running

```bash
./bin/reconciler
```

## Configuration

Configuration options will be added as the implementation progresses.
