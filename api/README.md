# Themelio Reconciler

## Overview

REST API for cloud infrastructure management.

## Architecture

## Project Structure

```
api/
├── cmd/
│   └── main.go                     # Application entry point
├── internal/
│   ├── api/                        # API layer
│   │   ├── handlers/
│   │   ├── middleware/
│   │   └── server/
│   ├── app/
│   │   └── module.go               # Dependency injection
│   ├── config/
│   │   └── config.go               # Configuration management
│   ├── repository/                 # DB layer
│   │   ├── resource_repository.go
│   │   └── schema_repository.go
│   ├── service/                    # Service layer
│   │   ├── resource_service.go
│   │   └── types.go
│   └── errors/
│       └── errors.go               # Common errors
```
