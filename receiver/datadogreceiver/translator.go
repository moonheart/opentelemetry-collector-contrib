// Copyright 2021, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datadogreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver"

import (
	"encoding/binary"
	"encoding/json"
	datadogapi "github.com/DataDog/datadog-agent/pkg/trace/api"
	datadogpb "github.com/DataDog/datadog-agent/pkg/trace/pb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.6.1"
	"io"
	"mime"
	"net/http"
	"strings"
)

func toTraces(traces datadogpb.Traces, req *http.Request) ptrace.Traces {
	dest := ptrace.NewTraces()

	for _, trace1 := range traces {
		groupByServiceName := traceGroupByServiceName(trace1)
		for serviceName, trace := range groupByServiceName {
			resSpans := dest.ResourceSpans().AppendEmpty()
			resSpans.SetSchemaUrl(semconv.SchemaURL)
			resSpans.Resource().Attributes().PutStr(semconv.AttributeServiceName, serviceName)

			scopeSpans := resSpans.ScopeSpans().AppendEmpty()
			scopeSpans.Scope().SetName("Datadog-" + req.Header.Get("Datadog-Meta-Lang"))
			scopeSpans.Scope().SetVersion(req.Header.Get("Datadog-Meta-Tracer-Version"))
			spans := ptrace.NewSpanSlice()
			spans.EnsureCapacity(len(*trace))
			for _, span := range *trace {
				newSpan := spans.AppendEmpty()

				newSpan.SetTraceID(uInt64ToTraceID(0, span.TraceID))
				newSpan.SetSpanID(uInt64ToSpanID(span.SpanID))
				newSpan.SetStartTimestamp(pcommon.Timestamp(span.Start))
				newSpan.SetEndTimestamp(pcommon.Timestamp(span.Start + span.Duration))
				newSpan.SetParentSpanID(uInt64ToSpanID(span.ParentID))
				newSpan.SetName(span.Resource)

				if span.Error > 0 {
					newSpan.Status().SetCode(ptrace.StatusCodeError)
				} else {
					newSpan.Status().SetCode(ptrace.StatusCodeOk)
				}

				attrs := newSpan.Attributes()
				attrs.EnsureCapacity(len(span.GetMeta()) + 1)
				attrs.PutStr(semconv.AttributeServiceName, span.Service)
				for k, v := range span.GetMeta() {
					k = translateDataDogKeyToOtel(k)
					if len(k) > 0 {
						attrs.PutStr(k, v)
					}
				}

				switch span.Type {
				case "web":
					newSpan.SetKind(ptrace.SpanKindServer)
				case "custom":
					newSpan.SetKind(ptrace.SpanKindUnspecified)
				default:
					newSpan.SetKind(ptrace.SpanKindClient)
				}
			}
			spans.MoveAndAppendTo(scopeSpans.Spans())

		}
	}

	return dest
}

func traceGroupByServiceName(trace datadogpb.Trace) map[string]*datadogpb.Trace {
	grouped := make(map[string]*datadogpb.Trace)

	for _, span := range trace {
		t, found := grouped[span.Service]
		if !found {
			t = &datadogpb.Trace{}
			grouped[span.Service] = t
		}
		*t = append(*t, span)
	}
	return grouped
}

func translateDataDogKeyToOtel(k string) string {
	// We don't want these
	if strings.HasPrefix(k, "_dd.") {
		return ""
	}
	switch strings.ToLower(k) {
	case "env":
		return semconv.AttributeDeploymentEnvironment
	case "version":
		return semconv.AttributeServiceVersion
	case "container_id":
		return semconv.AttributeContainerID
	case "container_name":
		return semconv.AttributeContainerName
	case "image_name":
		return semconv.AttributeContainerImageName
	case "image_tag":
		return semconv.AttributeContainerImageTag
	case "process_id":
		return semconv.AttributeProcessPID
	case "error.stacktrace":
		return semconv.AttributeExceptionStacktrace
	case "error.msg":
		return semconv.AttributeExceptionMessage
	case "db.type":
		return semconv.AttributeDBSystem
	case "db.instance":
		return semconv.AttributeDBName
	default:
		return k
	}

}

func decodeRequestVersion(v datadogapi.Version, req *http.Request, dest *datadogpb.Traces) error {
	switch v {
	case v05:
		buf := getBuffer()
		defer putBuffer(buf)
		if _, err := io.Copy(buf, req.Body); err != nil {
			return err
		}
		err := dest.UnmarshalMsgDictionary(buf.Bytes())
		if err != nil {
			return err
		}
	default:
		if err := decodeRequest(req, dest); err != nil {
			return err
		}
	}
	return nil
}

func getMediaType(req *http.Request) string {
	mt, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		return "application/json"
	}
	return mt
}

func uInt64ToTraceID(high, low uint64) pcommon.TraceID {
	traceID := [16]byte{}
	binary.BigEndian.PutUint64(traceID[:8], high)
	binary.BigEndian.PutUint64(traceID[8:], low)
	return traceID
}

func uInt64ToSpanID(id uint64) pcommon.SpanID {
	spanID := [8]byte{}
	binary.BigEndian.PutUint64(spanID[:], id)
	return spanID
}

// decodeRequest decodes the payload in http request `req` into `dest`.
// It handles only v02, v03, v04 requests.
// - ranHook reports whether the decoder was able to run the pb.MetaHook
// - err is the first error encountered
func decodeRequest(req *http.Request, dest *datadogpb.Traces) (err error) {
	switch mediaType := getMediaType(req); mediaType {
	case "application/msgpack":
		buf := getBuffer()
		defer putBuffer(buf)
		_, err = io.Copy(buf, req.Body)
		if err != nil {
			return err
		}
		_, err = dest.UnmarshalMsg(buf.Bytes())
		return err
	case "application/json":
		fallthrough
	case "text/json":
		fallthrough
	case "":
		err = json.NewDecoder(req.Body).Decode(&dest)
		return err
	default:
		// do our best
		if err1 := json.NewDecoder(req.Body).Decode(&dest); err1 != nil {
			buf := getBuffer()
			defer putBuffer(buf)
			_, err2 := io.Copy(buf, req.Body)
			if err2 != nil {
				return err2
			}
			_, err2 = dest.UnmarshalMsg(buf.Bytes())
			return err2
		}
		return nil
	}
}
