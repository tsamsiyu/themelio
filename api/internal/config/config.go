package config

import (
	"time"

	"github.com/caarlos0/env/v10"
)

type Config struct {
	Server  ServerConfig  `envPrefix:"SERVER_"`
	ETCD    ETCDConfig    `envPrefix:"ETCD_"`
	Logging LoggingConfig `envPrefix:"LOGGING_"`
}

type ServerConfig struct {
	Port            int           `env:"PORT" envDefault:"8080"`
	Host            string        `env:"HOST" envDefault:"0.0.0.0"`
	ReadTimeout     time.Duration `env:"READ_TIMEOUT" envDefault:"30s"`
	WriteTimeout    time.Duration `env:"WRITE_TIMEOUT" envDefault:"30s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

type ETCDConfig struct {
	Endpoints            []string      `env:"ENDPOINTS" envDefault:"localhost:2379" envSeparator:","`
	DialTimeout          time.Duration `env:"DIAL_TIMEOUT" envDefault:"5s"`
	DialKeepAliveTime    time.Duration `env:"DIAL_KEEP_ALIVE_TIME" envDefault:"30s"`
	DialKeepAliveTimeout time.Duration `env:"DIAL_KEEP_ALIVE_TIMEOUT" envDefault:"5s"`
	MaxCallSendMsgSize   int           `env:"MAX_CALL_SEND_MSG_SIZE" envDefault:"2097152"`
	MaxCallRecvMsgSize   int           `env:"MAX_CALL_RECV_MSG_SIZE" envDefault:"4194304"`
	Username             string        `env:"USERNAME"`
	Password             string        `env:"PASSWORD"`
	TLS                  TLSConfig     `envPrefix:"TLS_"`
}

type TLSConfig struct {
	Enabled            bool   `env:"ENABLED" envDefault:"false"`
	CAFile             string `env:"CA_FILE"`
	CertFile           string `env:"CERT_FILE"`
	KeyFile            string `env:"KEY_FILE"`
	InsecureSkipVerify bool   `env:"INSECURE_SKIP_VERIFY" envDefault:"false"`
}

type LoggingConfig struct {
	Level            string `env:"LEVEL" envDefault:"info"`
	Format           string `env:"FORMAT" envDefault:"json"`
	EnableCaller     bool   `env:"ENABLE_CALLER" envDefault:"true"`
	EnableStacktrace bool   `env:"ENABLE_STACKTRACE" envDefault:"false"`
	Development      bool   `env:"DEVELOPMENT" envDefault:"false"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
