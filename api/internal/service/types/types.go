package types

import (
	"context"

	repositorytypes "github.com/tsamsiyu/themelio/api/internal/repository/types"
	sdkmeta "github.com/tsamsiyu/themelio/sdk/pkg/types/meta"
)

type Params struct {
	Group     string
	Version   string
	Kind      string
	Namespace string
	Name      string
}

type ResourceService interface {
	ReplaceResource(ctx context.Context, params Params, jsonData []byte) error
	GetResource(ctx context.Context, params Params) (*sdkmeta.Object, error)
	ListResources(ctx context.Context, params Params) ([]*sdkmeta.Object, error)
	DeleteResource(ctx context.Context, params Params) error
	PatchResource(ctx context.Context, params Params, patchData []byte) (*sdkmeta.Object, error)
	WatchResource(ctx context.Context, params Params, revision int64) (<-chan repositorytypes.WatchEvent, error)
}
