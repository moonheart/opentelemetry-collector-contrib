module github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sapmreceiver

go 1.16

require (
	github.com/gorilla/mux v1.8.0
	github.com/jaegertracing/jaeger v1.24.0
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk v0.0.0-00010101000000-000000000000
	github.com/signalfx/sapm-proto v0.7.1
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.23.0
	go.opentelemetry.io/collector v0.30.2-0.20210723184018-3b7d6ce4830c
	go.opentelemetry.io/collector/model v0.30.2-0.20210723184018-3b7d6ce4830c
	go.uber.org/zap v1.18.1
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk => ../../internal/splunk
