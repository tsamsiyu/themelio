package app

import (
	"crypto/tls"
	"fmt"

	"github.com/go-playground/validator/v10"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/tsamsiyu/themelio/api/internal/api/handlers"
	"github.com/tsamsiyu/themelio/api/internal/api/server"
	"github.com/tsamsiyu/themelio/api/internal/config"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/service"
	sharedservice "github.com/tsamsiyu/themelio/api/internal/service/shared"
	"github.com/tsamsiyu/themelio/api/internal/worker/gc"
)

// CommonModule provides shared dependencies for both API and Worker
var CommonModule = fx.Options(
	fx.Provide(
		config.Load,
		NewLogger,
		NewETCDClient,
		validator.New,
		repository.NewResourceRepository,
		repository.NewSchemaRepository,
		service.NewResourceService,
		sharedservice.NewSchemaService,
	),
)

// APIModule provides dependencies specific to the API server
var APIModule = fx.Options(
	CommonModule,
	fx.Provide(
		handlers.NewResourceHandler,
		server.NewRouter,
		server.NewServer,
	),
)

// WorkerModule provides dependencies specific to the worker
var WorkerModule = fx.Options(
	CommonModule,
	fx.Provide(
		gc.NewWorker,
	),
)

// Module is kept for backward compatibility, defaults to APIModule
var Module = APIModule

func NewLogger(cfg *config.Config) (*zap.Logger, error) {
	var zapConfig zap.Config
	if cfg.Logging.Development {
		zapConfig = zap.NewDevelopmentConfig()
	} else {
		zapConfig = zap.NewProductionConfig()
	}

	zapConfig.Level = zap.NewAtomicLevelAt(parseLogLevel(cfg.Logging.Level))
	zapConfig.Encoding = cfg.Logging.Format
	zapConfig.EncoderConfig.TimeKey = "timestamp"
	zapConfig.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder

	return zapConfig.Build()
}

func NewETCDClient(cfg *config.Config) (*clientv3.Client, error) {
	etcdConfig := clientv3.Config{
		Endpoints:            cfg.ETCD.Endpoints,
		DialTimeout:          cfg.ETCD.DialTimeout,
		DialKeepAliveTime:    cfg.ETCD.DialKeepAliveTime,
		DialKeepAliveTimeout: cfg.ETCD.DialKeepAliveTimeout,
		MaxCallSendMsgSize:   cfg.ETCD.MaxCallSendMsgSize,
		MaxCallRecvMsgSize:   cfg.ETCD.MaxCallRecvMsgSize,
		Username:             cfg.ETCD.Username,
		Password:             cfg.ETCD.Password,
	}

	// Configure TLS if enabled
	if cfg.ETCD.TLS.Enabled {
		tlsConfig, err := createTLSConfig(cfg.ETCD.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		etcdConfig.TLS = tlsConfig
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	return client, nil
}

func createTLSConfig(tlsCfg config.TLSConfig) (*tls.Config, error) {
	// TODO: Implement TLS configuration
	// This would typically load certificates and create a proper TLS config
	return nil, fmt.Errorf("TLS configuration not yet implemented")
}

func parseLogLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
