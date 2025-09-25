package types

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
	sdkschema "github.com/tsamsiyu/themelio/sdk/pkg/types/schema"
)

type Paging struct {
	Prefix                string
	LastKey               string
	IncludeLastKeyInBatch bool
	SortDesc              bool
	Limit                 int
	Revision              int64
	MinModRevision        int64
}

type Batch struct {
	Revision int64
	KVs      []KeyValue
}

type KeyValue struct {
	Key            string
	Version        int64
	ModRevision    int64
	CreateRevision int64
	Value          []byte
}

type WatchEventType string

const (
	WatchEventTypeAdded    WatchEventType = "added"
	WatchEventTypeModified WatchEventType = "modified"
	WatchEventTypeDeleted  WatchEventType = "deleted"
	WatchEventTypeError    WatchEventType = "error"
)

type WatchEvent struct {
	Type      WatchEventType    `json:"type"`
	Object    *sdkmeta.Object   `json:"object,omitempty"` // in some exceptional cases it can be nil if the object is deleted and compaction disrupted watch process
	ObjectKey sdkmeta.ObjectKey `json:"objectKey"`
	Timestamp time.Time         `json:"timestamp"`
	Revision  int64             `json:"revision"`
	Error     error             `json:"error,omitempty"`
}

type ObjectBatch struct {
	Revision int64
	Objects  []*sdkmeta.Object
}

type WatchCacheEntry struct {
	Version        int64
	CreateRevision int64
	ModRevision    int64
}

type WatchConfig struct {
	MaxRetries         int
	ReconcileBatchSize int
}

type DeletionBatch struct {
	ObjectKeys []sdkmeta.ObjectKey
	LeaseID    clientv3.LeaseID
}

// EtcdClientInterface is a minimal interface for etcd client that we can mock
// It includes the methods we need from clientv3.Client
type EtcdClientInterface interface {
	// Txn creates a transaction
	Txn(ctx context.Context) clientv3.Txn
}

// ClientWrapper interface for etcd operations
type ClientWrapper interface {
	Client() EtcdClientInterface

	// Basic CRUD operations with raw data
	Put(ctx context.Context, key string, value string) error
	Get(ctx context.Context, key string) (*KeyValue, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, paging Paging) (*Batch, error)

	// Lease management
	GrantLease(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	KeepAliveLease(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)

	// Watch operations
	Watch(ctx context.Context, prefix string, revision ...int64) (<-chan clientv3.WatchResponse, error)
}

// ResourceStore provides resource-specific operations and watch functionality
type ResourceStore interface {
	// Basic CRUD operations with automatic marshaling
	Put(ctx context.Context, obj *sdkmeta.Object) error
	Get(ctx context.Context, key sdkmeta.ObjectKey) (*sdkmeta.Object, error)
	Delete(ctx context.Context, key sdkmeta.ObjectKey) error
	List(ctx context.Context, objType *sdkmeta.ObjectType, paging *Paging) (*ObjectBatch, error)

	// Transaction operation builders
	BuildPutTxOp(obj *sdkmeta.Object) (clientv3.Op, error)

	// Watch operations
	Watch(ctx context.Context, objType *sdkmeta.ObjectType, eventChan chan<- WatchEvent, revision ...int64) error
}

// ResourceRepository interface for resource operations
type ResourceRepository interface {
	Replace(ctx context.Context, obj *sdkmeta.Object, optimisticLock bool) error
	Get(ctx context.Context, key sdkmeta.ObjectKey) (*sdkmeta.Object, error)
	List(ctx context.Context, objType *sdkmeta.ObjectType) ([]*sdkmeta.Object, error)
	Delete(ctx context.Context, key sdkmeta.ObjectKey, lockValue string) error
	Watch(ctx context.Context, objType *sdkmeta.ObjectType) (<-chan WatchEvent, error)
	MarkDeleted(ctx context.Context, key sdkmeta.ObjectKey) error
	ListDeletions(ctx context.Context, lockKey string, lockExp time.Duration, batchLimit int) (*DeletionBatch, error)
}

// SchemaRepository interface for schema operations
type SchemaRepository interface {
	StoreSchema(ctx context.Context, schema *sdkschema.ObjectSchema) error
	GetSchema(ctx context.Context, group, kind string) (*sdkschema.ObjectSchema, error)
	DeleteSchema(ctx context.Context, group, kind string) error
	ListSchemas(ctx context.Context) ([]*sdkschema.ObjectSchema, error)
}
