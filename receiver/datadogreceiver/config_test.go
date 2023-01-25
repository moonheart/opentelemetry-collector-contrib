package datadogreceiver

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id:       component.NewID(typeStr),
			expected: createDefaultConfig(),
		},
		{
			id: component.NewIDWithName(typeStr, "customname"),
			expected: &Config{
				HTTPServerSettings: confighttp.HTTPServerSettings{
					Endpoint: "localhost:8126",
				},
				ReadTimeout: 60 * time.Second,
			},
		},
		{
			id: component.NewIDWithName(typeStr, "readtimeout"),
			expected: &Config{
				HTTPServerSettings: confighttp.HTTPServerSettings{
					Endpoint: defaultBindEndpoint,
				},
				ReadTimeout: 10 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))

			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
