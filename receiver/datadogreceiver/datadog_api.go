// codes in this file are copied from
// https://github.com/DataDog/datadog-agent/blob/main/pkg/trace/api/api.go
// with minor modifications

package datadogreceiver

import (
	"bytes"
	"encoding/json"
	"github.com/DataDog/datadog-agent/pkg/trace/info"
	"github.com/DataDog/datadog-agent/pkg/trace/pb"
	"github.com/DataDog/datadog-agent/pkg/trace/sampler"
	"io"
	"mime"
	"net/http"
	"sync"
)

// Version is a dumb way to version our collector handlers
type Version string

const (
	// v01 is DEPRECATED
	v01 Version = "v0.1"

	// v02 is DEPRECATED
	v02 Version = "v0.2"

	// v03
	// Traces: msgpack/JSON (Content-Type) slice of traces
	v03 Version = "v0.3"

	// v04
	//
	// Request: Trace chunks.
	// 	Content-Type: application/msgpack
	// 	Payload: An array of arrays of Span (pkg/trace/pb/span.proto)
	//
	// Response: Service sampling rates.
	// 	Content-Type: application/json
	// 	Payload: Object mapping span pattern to sample rate.
	//
	// The response payload is an object whose keys are of the form
	// "service:my-service,env:my-env", where "my-service" is the name of the
	// affected service, and "my-env" is the name of the relevant deployment
	// environment. The value at each key is the sample rate to apply to traces
	// beginning with a span that matches the key.
	//
	//  {
	//    "service:foosvc,env:prod": 0.223443,
	//    "service:barsvc,env:staging": 0.011249
	//  }
	//
	// There is a special key, "service:,env:", that denotes the sample rate for
	// traces that do not match any other key.
	//
	//  {
	//    "service:foosvc,env:prod": 0.223443,
	//    "service:barsvc,env:staging": 0.011249,
	//    "service:,env:": 0.5
	//  }
	//
	// Neither the "service:,env:" key nor any other need be present in the
	// response.
	//
	//  {}
	//
	v04 Version = "v0.4"

	// v05
	//
	// Request: Trace chunks with a shared dictionary of strings.
	// 	Content-Type: application/msgpack
	// 	Payload: Traces with strings de-duplicated into a dictionary (see below).
	//
	// Response: Service sampling rates (see description in v04).
	//
	// The request payload is an array containing exactly 2 elements:
	//
	// 	1. An array of all unique strings present in the payload (a dictionary referred to by index).
	// 	2. An array of traces, where each trace is an array of spans. A span is encoded as an array having
	// 	   exactly 12 elements, representing all span properties, in this exact order:
	//
	// 		 0: Service   (uint32)
	// 		 1: Name      (uint32)
	// 		 2: Resource  (uint32)
	// 		 3: TraceID   (uint64)
	// 		 4: SpanID    (uint64)
	// 		 5: ParentID  (uint64)
	// 		 6: Start     (int64)
	// 		 7: Duration  (int64)
	// 		 8: Error     (int32)
	// 		 9: Meta      (map[uint32]uint32)
	// 		10: Metrics   (map[uint32]float64)
	// 		11: Type      (uint32)
	//
	// 	Considerations:
	//
	// 	- The "uint32" typed values in "Service", "Name", "Resource", "Type", "Meta" and "Metrics" represent
	// 	  the index at which the corresponding string is found in the dictionary. If any of the values are the
	// 	  empty string, then the empty string must be added into the dictionary.
	//
	// 	- None of the elements can be nil. If any of them are unset, they should be given their "zero-value". Here
	// 	  is an example of a span with all unset values:
	//
	// 		 0: 0                    // Service is "" (index 0 in dictionary)
	// 		 1: 0                    // Name is ""
	// 		 2: 0                    // Resource is ""
	// 		 3: 0                    // TraceID
	// 		 4: 0                    // SpanID
	// 		 5: 0                    // ParentID
	// 		 6: 0                    // Start
	// 		 7: 0                    // Duration
	// 		 8: 0                    // Error
	// 		 9: map[uint32]uint32{}  // Meta (empty map)
	// 		10: map[uint32]float64{} // Metrics (empty map)
	// 		11: 0                    // Type is ""
	//
	// 		The dictionary in this case would be []string{""}, having only the empty string at index 0.
	//
	v05 Version = "v0.5"

	// V07 API
	//
	// Request: Tracer Payload.
	// 	Content-Type: application/msgpack
	// 	Payload: TracerPayload (pkg/trace/pb/tracer_payload.proto)
	//
	// Response: Service sampling rates (see description in v04).
	//
	v07 Version = "v0.7"
)

const (

	// headerContainerID specifies the name of the header which contains the ID of the
	// container where the request originated.
	headerContainerID = "Datadog-Container-ID"

	// headerLang specifies the name of the header which contains the language from
	// which the traces originate.
	headerLang = "Datadog-Meta-Lang"

	// headerLangVersion specifies the name of the header which contains the origin
	// language's version.
	headerLangVersion = "Datadog-Meta-Lang-Version"

	// headerLangInterpreter specifies the name of the HTTP header containing information
	// about the language interpreter, where applicable.
	headerLangInterpreter = "Datadog-Meta-Lang-Interpreter"

	// headerLangInterpreterVendor specifies the name of the HTTP header containing information
	// about the language interpreter vendor, where applicable.
	headerLangInterpreterVendor = "Datadog-Meta-Lang-Interpreter-Vendor"

	// headerTracerVersion specifies the name of the header which contains the version
	// of the tracer sending the payload.
	headerTracerVersion = "Datadog-Meta-Tracer-Version"
)

func tags(v Version, header http.Header) *info.Tags {
	return &info.Tags{
		Lang:            header.Get(headerLang),
		LangVersion:     header.Get(headerLangVersion),
		Interpreter:     header.Get(headerLangInterpreter),
		LangVendor:      header.Get(headerLangInterpreterVendor),
		TracerVersion:   header.Get(headerTracerVersion),
		EndpointVersion: string(v),
	}
}

// getMediaType attempts to return the media type from the Content-Type MIME header. If it fails
// it returns the default media type "application/json".
func getMediaType(req *http.Request) string {
	mt, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		return "application/json"
	}
	return mt
}

// decodeTracerPayload decodes the payload in http request `req`.
// - tp is the decoded payload
// - ranHook reports whether the decoder was able to run the pb.MetaHook
// - err is the first error encountered
func decodeTracerPayload(v Version, req *http.Request, ts *info.Tags) (tp *pb.TracerPayload, err error) {
	switch v {
	case v01:
		var spans []pb.Span
		if err = json.NewDecoder(req.Body).Decode(&spans); err != nil {
			return nil, err
		}
		return &pb.TracerPayload{
			LanguageName:    ts.Lang,
			LanguageVersion: ts.LangVersion,
			ContainerID:     getContainerID(req.Header),
			Chunks:          traceChunksFromSpans(spans),
			TracerVersion:   ts.TracerVersion,
		}, nil
	case v05:
		buf := getBuffer()
		defer putBuffer(buf)
		if _, err = io.Copy(buf, req.Body); err != nil {
			return nil, err
		}
		var traces pb.Traces
		err = traces.UnmarshalMsgDictionary(buf.Bytes())
		return &pb.TracerPayload{
			LanguageName:    ts.Lang,
			LanguageVersion: ts.LangVersion,
			ContainerID:     getContainerID(req.Header),
			Chunks:          traceChunksFromTraces(traces),
			TracerVersion:   ts.TracerVersion,
		}, err
	case v07:
		buf := getBuffer()
		defer putBuffer(buf)
		if _, err = io.Copy(buf, req.Body); err != nil {
			return nil, err
		}
		var tracerPayload pb.TracerPayload
		_, err = tracerPayload.UnmarshalMsg(buf.Bytes())
		return &tracerPayload, err
	default:
		var traces pb.Traces
		if err = decodeRequest(req, &traces); err != nil {
			return nil, err
		}
		//marshal, _ := json.Marshal(traces)
		//str := string(marshal)
		//fmt.Println(str)
		return &pb.TracerPayload{
			LanguageName:    ts.Lang,
			LanguageVersion: ts.LangVersion,
			ContainerID:     getContainerID(req.Header),
			Chunks:          traceChunksFromTraces(traces),
			TracerVersion:   ts.TracerVersion,
		}, nil
	}
}

// decodeRequest decodes the payload in http request `req` into `dest`.
// It handles only v02, v03, v04 requests.
// - err is the first error encountered
func decodeRequest(req *http.Request, dest *pb.Traces) (err error) {
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

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	buffer := bufferPool.Get().(*bytes.Buffer)
	buffer.Reset()
	return buffer
}

func putBuffer(buffer *bytes.Buffer) {
	bufferPool.Put(buffer)
}

func traceChunksFromSpans(spans []pb.Span) []*pb.TraceChunk {
	traceChunks := []*pb.TraceChunk{}
	byID := make(map[uint64][]*pb.Span)
	for _, s := range spans {
		byID[s.TraceID] = append(byID[s.TraceID], &s)
	}
	for _, t := range byID {
		traceChunks = append(traceChunks, &pb.TraceChunk{
			Priority: int32(sampler.PriorityNone),
			Spans:    t,
		})
	}
	return traceChunks
}

func traceChunksFromTraces(traces pb.Traces) []*pb.TraceChunk {
	traceChunks := make([]*pb.TraceChunk, 0, len(traces))
	for _, trace := range traces {
		traceChunks = append(traceChunks, &pb.TraceChunk{
			Priority: int32(sampler.PriorityNone),
			Spans:    trace,
		})
	}

	return traceChunks
}

func getContainerID(h http.Header) string {
	return h.Get(headerContainerID)
}
