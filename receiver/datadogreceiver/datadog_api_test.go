package datadogreceiver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/DataDog/datadog-agent/pkg/trace/info"
	"github.com/DataDog/datadog-agent/pkg/trace/pb"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func Test_decodeTracerPayload(t *testing.T) {
	spansv01 := []pb.Span{
		{
			Service:  "test-service",
			Name:     "test-name",
			Resource: "test-resource",
			TraceID:  2389442122630582718,
			SpanID:   5384079244777812447,
			ParentID: 0,
			Start:    1674543056285555400,
			Duration: 1459397900,
			Error:    0,
			Meta: map[string]string{
				"env": "local",
			},
			Metrics:    nil,
			Type:       "http",
			MetaStruct: nil,
		},
	}
	bodyv01, err := json.Marshal(spansv01)
	require.NoError(t, err)

	bodyContent, err := os.ReadFile("./testdata/req_v04.json")
	var ddtraces pb.Traces
	err = json.Unmarshal(bodyContent, &ddtraces)
	require.NoError(t, err)

	type args struct {
		v         Version
		bodyBytes []byte
		url       string
	}
	tests := []struct {
		name   string
		args   args
		wantTp *pb.TracerPayload
	}{
		{
			name: "v01",
			args: args{
				bodyBytes: bodyv01,
				v:         v01,
				url:       fmt.Sprintf("/%v/spans", v01),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, tt.args.url, bytes.NewBuffer(tt.args.bodyBytes))
			tags := &info.Tags{
				Lang:            ".NET",
				LangVersion:     "6.0.11",
				Interpreter:     ".NET",
				LangVendor:      "",
				TracerVersion:   "2.21.1",
				EndpointVersion: string(v01),
			}
			_, err := decodeTracerPayload(tt.args.v, request, tags)
			require.NoError(t, err)
		})
	}
}
