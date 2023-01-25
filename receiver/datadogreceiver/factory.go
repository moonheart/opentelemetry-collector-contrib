package datadogreceiver

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"time"
)

const (
	typeStr   = "datadog"
	stability = component.StabilityLevelAlpha

	defaultBindEndpoint = "0.0.0.0:8126"
)

// NewFactory creates a factory for Datadog receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithTraces(createTracesReceiver, stability),
	)
}

// createDefaultConfig creates the default configuration for Datadog receiver.
func createDefaultConfig() component.Config {
	return &Config{
		HTTPServerSettings: confighttp.HTTPServerSettings{
			Endpoint: defaultBindEndpoint,
		},
		ReadTimeout: 60 * time.Second,
	}
}

// createTracesReceiver creates a trace receiver based on provided config.
func createTracesReceiver(
	_ context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	rCfg := cfg.(*Config)
	return newReceiver(rCfg, nextConsumer, set)
}
