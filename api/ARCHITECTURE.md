# API Architecture

The API subproject follows a clean 3-layer architecture pattern:

## Layer 1: API (Handlers)
**Location**: `internal/handlers/`

- **Purpose**: HTTP request/response handling, input validation, and routing
- **Responsibilities**:
  - Parse HTTP requests
  - Validate input data
  - Call service layer
  - Format HTTP responses
  - Handle HTTP-specific errors

**Key Files**:
- `resources.go` - Network resource handlers (CRUD operations)

## Layer 2: Service (Domain Logic)
**Location**: `internal/service/`

- **Purpose**: Business logic and domain operations
- **Responsibilities**:
  - Implement business rules
  - Coordinate between multiple repositories
  - Validate business constraints
  - Handle domain-specific errors
  - Transform data between layers

**Key Files**:
- `network_service.go` - Network business logic
- `types.go` - Request/response DTOs

## Layer 3: Repository (Data Access)
**Location**: `internal/repository/`

- **Purpose**: Data persistence and retrieval
- **Responsibilities**:
  - Abstract data storage implementation
  - Handle data access errors
  - Manage data transformations
  - Provide clean interface to service layer

**Key Files**:
- `network_repository.go` - Network data access operations

## Additional Components

### Middleware
**Location**: `internal/middleware/`

- **Purpose**: Cross-cutting concerns
- **Responsibilities**:
  - Request logging
  - Error handling
  - CORS configuration
  - Authentication/authorization (future)

### Server
**Location**: `internal/server/`

- **Purpose**: HTTP server configuration and routing
- **Responsibilities**:
  - Setup Gin router
  - Configure middleware
  - Wire up dependencies
  - Handle server lifecycle

## Data Flow

```
HTTP Request → Handler → Service → Repository → ETCD
                ↓         ↓         ↓
HTTP Response ← Handler ← Service ← Repository ← ETCD
```

## Benefits

1. **Separation of Concerns**: Each layer has a single responsibility
2. **Testability**: Each layer can be tested independently
3. **Maintainability**: Changes in one layer don't affect others
4. **Reusability**: Service logic can be reused across different interfaces
5. **Scalability**: Easy to add new endpoints or change data storage

## API Endpoints

### Networks
- `POST /api/v1/networks` - Create network
- `GET /api/v1/networks/:namespace/:name` - Get network
- `GET /api/v1/networks/:namespace` - List networks
- `PUT /api/v1/networks/:namespace/:name` - Update network
- `DELETE /api/v1/networks/:namespace/:name` - Delete network

### Health
- `GET /health` - Health check
