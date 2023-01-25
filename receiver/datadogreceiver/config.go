package datadogreceiver

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"time"
)

// Config defines configuration for Datadog receiver.
type Config struct {
	confighttp.HTTPServerSettings `mapstructure:",squash"`
	// ReadTimeout is the maximum duration for reading the entire request, including the body. A zero or negative value means there will be no timeout.
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
}

var _ component.Config = (*Config)(nil)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	return nil
}
