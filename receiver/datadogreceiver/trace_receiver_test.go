package datadogreceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/DataDog/datadog-agent/pkg/trace/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func Test_datadogReceiver_Start(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		host    component.Host
		wantErr bool
	}{
		{
			name:    "nil host",
			wantErr: true,
		},
		{
			name: "ok",
			host: componenttest.NewNopHost(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := new(consumertest.TracesSink)
			cfg := &Config{
				HTTPServerSettings: confighttp.HTTPServerSettings{
					Endpoint: "localhost:0",
				},
			}
			r, err := newReceiver(cfg, sink, receivertest.NewNopCreateSettings())
			require.Nil(t, err)
			require.NotNil(t, r)

			err = r.Start(context.Background(), tt.host)
			assert.Equal(t, tt.wantErr, err != nil)
			if !tt.wantErr {
				require.Nil(t, r.Shutdown(context.Background()))
			}
		})
	}
}

func Test_newReceiver(t *testing.T) {
	type args struct {
		config       *Config
		nextConsumer consumer.Traces
		settings     receiver.CreateSettings
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name:    "nil consumer",
			args:    args{},
			wantErr: component.ErrNilNextConsumer,
		},
		{
			name: "everything ok",
			args: args{
				nextConsumer: consumertest.NewNop(),
				settings:     receivertest.NewNopCreateSettings(),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				HTTPServerSettings: confighttp.HTTPServerSettings{
					Endpoint: defaultBindEndpoint,
				},
				ReadTimeout: 60 * time.Second,
			}
			got, err := newReceiver(cfg, tt.args.nextConsumer, tt.args.settings)

			require.Equal(t, tt.wantErr, err)

			if tt.wantErr == nil {
				assert.NotNil(t, got)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

func Test_datadogReceiver_handleTraces(t *testing.T) {
	type args struct {
		bodyContentTypeJson bool
		headerContentType   string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "v04 msgpack header msgpack body",
			args: args{
				bodyContentTypeJson: false,
				headerContentType:   "application/msgpack",
			},
		},
		{
			name: "v04 json header json body",
			args: args{
				bodyContentTypeJson: true,
				headerContentType:   "application/json",
			},
		},
		{
			name: "v04 no header json body",
			args: args{
				bodyContentTypeJson: true,
				headerContentType:   "",
			},
		},
		{
			name: "v04 non header json body",
			args: args{
				bodyContentTypeJson: true,
				headerContentType:   "application/octet-stream",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			cfg := &Config{
				HTTPServerSettings: confighttp.HTTPServerSettings{
					Endpoint: defaultBindEndpoint,
				},
				ReadTimeout: 60 * time.Second,
			}
			d, err := newReceiver(cfg, consumertest.NewNop(), receivertest.NewNopCreateSettings())
			require.Nil(t, err)
			request := buildTestRequest(t, tt.args.bodyContentTypeJson, tt.args.headerContentType)

			recorder := httptest.NewRecorder()
			d.handleTraces(v04, recorder, request)
			assert.Equal(t, http.StatusOK, recorder.Code)
		})
	}

}

func buildTestRequest(t *testing.T, bodyContentTypeJson bool, headerContentType string) *http.Request {
	var err error
	bodyContent, err := os.ReadFile("./testdata/req_v04.json")
	require.NoError(t, err)
	if !bodyContentTypeJson {
		var ddtraces pb.Traces
		err = json.Unmarshal(bodyContent, &ddtraces)
		require.NoError(t, err)
		var pbdata []byte
		pbdata, err = ddtraces.MarshalMsg(pbdata)
		require.NoError(t, err)
		bodyContent = pbdata
	}

	request := httptest.NewRequest(http.MethodPost, "/v0.4/traces", bytes.NewBuffer(bodyContent))
	request.Header.Set(headerLang, ".NET")
	request.Header.Set(headerTracerVersion, "2.21.0.0")
	request.Header.Set(headerLangVersion, "6.0.13")
	request.Header.Set(headerLangInterpreter, ".NET")
	request.Header.Set("Content-Type", headerContentType)
	return request
}
