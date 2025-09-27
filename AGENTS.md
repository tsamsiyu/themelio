# Themelio Project Overview

## Project Purpose
Themelio is a Go-based infrastructure management system that provides a simplified interface over cloud and on-premises infrastructure using the reconciliation pattern (similar to Kubernetes). It uses etcd as state storage and supports multiple cloud providers.

## Architecture
- **API**: Generic reconciliation API that provides support for reconciliation pattern implementation
- **Provider Agents**: AWS, GCP, Azure agents that implement cloud-specific logic and reconciliation
- **SDK**: Shared types and interfaces used across all components
- **ETCD**: State storage for reconciliation

## Module Dependencies
- All providers import `sdk` for shared types and `api` for reconciliation support
- API provides generic reconciliation pattern implementation
- Each module is a separate Go module with its own `go.mod`
