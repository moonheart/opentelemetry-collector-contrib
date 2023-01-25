package datadogreceiver

import (
	"encoding/binary"
	"fmt"
	"github.com/DataDog/datadog-agent/pkg/trace/pb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.6.1"
	"strings"
)

type resourceSpansInfo struct {
	resourceSpans *ptrace.ResourceSpans
	scopeSpansMap map[string]*ptrace.ScopeSpans
}

// translateTraces translate datadog pb.TracerPayload into ptrace.Traces
func translateTraces(tp *pb.TracerPayload) (traces ptrace.Traces, spanCount int) {
	traces = ptrace.NewTraces()

	// datadog traces are grouped by traceId, we need to map to resources groups
	resourceSpansMap := map[string]*resourceSpansInfo{}

	for _, chunk := range tp.Chunks {
		for _, span := range chunk.GetSpans() {
			rsInfo, ok := resourceSpansMap[span.Service]
			if !ok {
				rs := traces.ResourceSpans().AppendEmpty()
				rs.Resource().Attributes().PutStr(semconv.AttributeServiceName, span.Service)
				if version, ok := getVersion(tp, span, chunk); ok {
					rs.Resource().Attributes().PutStr(semconv.AttributeServiceVersion, version)
				}
				rsInfo = &resourceSpansInfo{resourceSpans: &rs, scopeSpansMap: map[string]*ptrace.ScopeSpans{}}
				resourceSpansMap[span.Service] = rsInfo
			}
			scopeKey := fmt.Sprintf("Datadog-%s-%s", tp.LanguageName, tp.TracerVersion)
			scopeSpans, ok := rsInfo.scopeSpansMap[scopeKey]
			if !ok {
				ss := rsInfo.resourceSpans.ScopeSpans().AppendEmpty()
				ss.Scope().SetName(fmt.Sprintf("Datadog-%s", tp.LanguageName))
				ss.Scope().SetVersion(tp.TracerVersion)
				scopeSpans = &ss
				rsInfo.scopeSpansMap[scopeKey] = scopeSpans
			}

			spanCount += 1
			newSpan := scopeSpans.Spans().AppendEmpty()
			newSpan.SetTraceID(uInt64ToTraceID(0, span.TraceID))
			newSpan.SetSpanID(uInt64ToSpanID(span.SpanID))
			newSpan.SetParentSpanID(uInt64ToSpanID(span.ParentID))
			newSpan.SetStartTimestamp(pcommon.Timestamp(span.Start))
			newSpan.SetEndTimestamp(pcommon.Timestamp(span.Start + span.Duration))
			newSpan.SetName(span.Resource)

			setSpanStatus(span, newSpan)
			setSpanKind(span, newSpan)

			setSpanAttrFromSpan(span, newSpan)
			setSpanAttrFromMap(chunk.GetTags(), newSpan)
			setSpanAttrFromMap(tp.GetTags(), newSpan)
			setSpanAttrFromTp(tp, newSpan)
		}
	}
	return traces, spanCount
}

// setSpanAttrFromTp extract attrs from pb.TracerPayload and set to span if not exist
func setSpanAttrFromTp(tp *pb.TracerPayload, newSpan ptrace.Span) {
	if _, ok := newSpan.Attributes().Get(semconv.AttributeDeploymentEnvironment); !ok && len(tp.Env) > 0 {
		newSpan.Attributes().PutStr(semconv.AttributeDeploymentEnvironment, tp.Env)
	}
	if _, ok := newSpan.Attributes().Get(semconv.AttributeContainerID); !ok && len(tp.ContainerID) > 0 {
		newSpan.Attributes().PutStr(semconv.AttributeContainerID, tp.ContainerID)
	}
	if _, ok := newSpan.Attributes().Get(semconv.AttributeHostName); !ok && len(tp.Hostname) > 0 {
		newSpan.Attributes().PutStr(semconv.AttributeHostName, tp.Hostname)
	}
	if _, ok := newSpan.Attributes().Get(semconv.AttributeServiceVersion); !ok && len(tp.AppVersion) > 0 {
		newSpan.Attributes().PutStr(semconv.AttributeServiceVersion, tp.AppVersion)
	}
}

// getVersion extract version info from different positions
func getVersion(tp *pb.TracerPayload, span *pb.Span, chunk *pb.TraceChunk) (string, bool) {
	if v, ok := span.Meta["version"]; ok {
		return v, true
	} else if v, ok := chunk.GetTags()["version"]; ok {
		return v, true
	} else if v, ok := tp.GetTags()["version"]; ok {
		return v, true
	} else if len(tp.AppVersion) > 0 {
		return tp.AppVersion, true
	}
	return "", false
}

// setSpanAttrFromMap set attrs from a map
func setSpanAttrFromMap(m map[string]string, newSpan ptrace.Span) {
	for k, v := range m {
		k = translateMetaToAttr(k)
		if len(k) > 0 {
			if _, ok := newSpan.Attributes().Get(k); !ok {
				newSpan.Attributes().PutStr(k, v)
			}
		}
	}
}

// setSpanAttrFromSpan set attrs from datadog span
func setSpanAttrFromSpan(span *pb.Span, newSpan ptrace.Span) {
	attributes := newSpan.Attributes()
	attributes.EnsureCapacity(len(span.Meta) + 1)
	for k, v := range span.Meta {
		k = translateMetaToAttr(k)
		if len(k) > 0 {
			attributes.PutStr(k, v)
		}
	}
	if _, ok := attributes.Get(semconv.AttributeServiceName); !ok {
		attributes.PutStr(semconv.AttributeServiceName, span.Service)
	}
}

var (
	metaAttrMap = map[string]string{
		"env":     semconv.AttributeDeploymentEnvironment,
		"service": semconv.AttributeServiceName,
		"version": semconv.AttributeServiceVersion,

		"container_id":   semconv.AttributeContainerID,
		"container_name": semconv.AttributeContainerName,
		"image_name":     semconv.AttributeContainerImageName,
		"image_tag":      semconv.AttributeContainerImageTag,

		"cloud_provider": semconv.AttributeCloudProvider,
		"region":         semconv.AttributeCloudRegion,
		"zone":           semconv.AttributeCloudAvailabilityZone,

		"task_family":        semconv.AttributeAWSECSTaskFamily,
		"task_arn":           semconv.AttributeAWSECSTaskARN,
		"ecs_cluster_name":   semconv.AttributeAWSECSClusterARN,
		"task_version":       semconv.AttributeAWSECSTaskRevision,
		"ecs_container_name": semconv.AttributeAWSECSContainerARN,

		"kube_container_name": semconv.AttributeK8SContainerName,
		"kube_cluster_name":   semconv.AttributeK8SClusterName,
		"kube_deployment":     semconv.AttributeK8SDeploymentName,
		"kube_replica_set":    semconv.AttributeK8SReplicaSetName,
		"kube_stateful_set":   semconv.AttributeK8SStatefulSetName,
		"kube_daemon_set":     semconv.AttributeK8SDaemonSetName,
		"kube_job":            semconv.AttributeK8SJobName,
		"kube_cronjob":        semconv.AttributeK8SCronJobName,
		"kube_namespace":      semconv.AttributeK8SNamespaceName,
		"pod_name":            semconv.AttributeK8SPodName,

		"process_id": semconv.AttributeProcessPID,

		"error.stacktrace": semconv.AttributeExceptionStacktrace,
		"error.msg":        semconv.AttributeExceptionMessage,
		"error.message":    semconv.AttributeExceptionMessage,
		"error.type":       semconv.AttributeExceptionType,

		"http.request.headers.host": semconv.AttributeHTTPHost,
		"http.hostname":             semconv.AttributeHTTPHost,

		"db.type":   semconv.AttributeDBSystem,
		"sql.query": semconv.AttributeDBStatement,

		"out.host":           semconv.AttributeNetPeerName,
		"out.port":           semconv.AttributeNetPeerPort,
		"mongodb.collection": semconv.AttributeDBMongoDBCollection,
	}
)

func translateMetaToAttr(metaKey string) string {
	if strings.HasPrefix(metaKey, "_dd.") {
		return ""
	}
	if v, ok := metaAttrMap[metaKey]; ok {
		return v
	}
	return metaKey
}

// setSpanKind convert meta `span.kind` into SpanKind and set to otel span
func setSpanKind(span *pb.Span, newSpan ptrace.Span) {
	switch span.Meta["span.kind"] {
	case "server":
		newSpan.SetKind(ptrace.SpanKindServer)
	case "client":
		newSpan.SetKind(ptrace.SpanKindClient)
	case "producer":
		newSpan.SetKind(ptrace.SpanKindProducer)
	case "consumer":
		newSpan.SetKind(ptrace.SpanKindConsumer)
	case "internal":
		newSpan.SetKind(ptrace.SpanKindInternal)
	default:
		newSpan.SetKind(ptrace.SpanKindUnspecified)
	}
}

// setSpanStatus extract status attr from datadog span and set to otel span
func setSpanStatus(span *pb.Span, newSpan ptrace.Span) {
	if span.Error > 0 {
		newSpan.Status().SetCode(ptrace.StatusCodeError)
	} else {
		newSpan.Status().SetCode(ptrace.StatusCodeOk)
	}
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
