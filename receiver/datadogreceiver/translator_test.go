package datadogreceiver

import (
	"github.com/DataDog/datadog-agent/pkg/trace/pb"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.6.1"
	"testing"
)

func Test_translateTraces(t *testing.T) {
	wantTracesOk := ptrace.NewTraces()
	resourceSpans := wantTracesOk.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr(semconv.AttributeServiceName, "test-service")
	resourceSpans.Resource().Attributes().PutStr(semconv.AttributeServiceVersion, "v1.0.0")
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	scopeSpans.Scope().SetName("datadog-java")
	scopeSpans.Scope().SetVersion("1.4.0")
	span := scopeSpans.Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID([16]byte{}))
	type args struct {
		tp *pb.TracerPayload
	}
	tests := []struct {
		name          string
		args          args
		wantTraces    ptrace.Traces
		wantSpanCount int
	}{
		{
			name: "ok",
			args: args{tp: &pb.TracerPayload{
				ContainerID:     "",
				LanguageName:    "",
				LanguageVersion: "",
				TracerVersion:   "",
				RuntimeID:       "",
				Chunks:          nil,
				Tags:            nil,
				Env:             "",
				Hostname:        "",
				AppVersion:      "",
			}},
			wantTraces: ptrace.NewTraces(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTraces, gotSpanCount := translateTraces(tt.args.tp)
			assert.Equalf(t, tt.wantTraces, gotTraces, "translateTraces(%v)", tt.args.tp)
			assert.Equalf(t, tt.wantSpanCount, gotSpanCount, "translateTraces(%v)", tt.args.tp)
		})
	}
}
