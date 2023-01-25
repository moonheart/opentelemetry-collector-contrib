package datadogreceiver

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/obsreport"
	"go.opentelemetry.io/collector/receiver"
	"net"
	"net/http"
	"sync"
)

type datadogReceiver struct {
	nextConsumer consumer.Traces
	config       *Config
	settings     receiver.CreateSettings

	obs    *obsreport.Receiver
	server *http.Server

	shutdownWG sync.WaitGroup
	startOnce  sync.Once
	stopOnce   sync.Once
}

func newReceiver(config *Config, nextConsumer consumer.Traces, settings receiver.CreateSettings) (*datadogReceiver, error) {
	if nextConsumer == nil {
		return nil, component.ErrNilNextConsumer
	}
	obs, err := obsreport.NewReceiver(obsreport.ReceiverSettings{
		ReceiverID:             settings.ID,
		Transport:              "http",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}

	return &datadogReceiver{
		nextConsumer: nextConsumer,
		config:       config,
		settings:     settings,
		server: &http.Server{
			Addr:        config.Endpoint,
			ReadTimeout: config.ReadTimeout,
		},
		obs: obs,
	}, nil
}
func (d *datadogReceiver) Start(_ context.Context, host component.Host) error {
	if host == nil {
		return errors.New("nil host")
	}

	mux := http.ServeMux{}
	mux.Handle("/v0.1/spans", d.handleWithVersion(v01, d.handleTraces))
	mux.Handle("/v0.2/traces", d.handleWithVersion(v02, d.handleTraces))
	mux.Handle("/v0.3/traces", d.handleWithVersion(v03, d.handleTraces))
	mux.Handle("/v0.4/traces", d.handleWithVersion(v04, d.handleTraces))
	mux.Handle("/v0.5/traces", d.handleWithVersion(v05, d.handleTraces))
	mux.Handle("/v0.7/traces", d.handleWithVersion(v07, d.handleTraces))

	var err error
	d.server, err = d.config.ToServer(host, d.settings.TelemetrySettings, &mux)
	if err != nil {
		return err
	}

	var listener net.Listener
	listener, err = d.config.ToListener()
	if err != nil {
		return err
	}

	d.shutdownWG.Add(1)
	go func() {
		defer d.shutdownWG.Done()

		if errHTTP := d.server.Serve(listener); !errors.Is(errHTTP, http.ErrServerClosed) && errHTTP != nil {
			host.ReportFatalError(errHTTP)
		}
	}()

	return nil
}

func (d *datadogReceiver) Shutdown(ctx context.Context) (err error) {
	if d.server != nil {
		err = d.server.Shutdown(ctx)
	}
	d.shutdownWG.Wait()
	return err
}

func (d *datadogReceiver) handleWithVersion(v Version, f func(Version, http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if mediaType := getMediaType(req); mediaType == "application/msgpack" && (v == v01 || v == v02) {
			// msgpack is only supported for versions >= v0.3
			http.Error(w, fmt.Sprintf("unsupported media type: %q", mediaType), http.StatusUnsupportedMediaType)
			return
		}

		f(v, w, req)
	}
}

// handleTraces knows how to handle a bunch of traces
func (d *datadogReceiver) handleTraces(v Version, w http.ResponseWriter, req *http.Request) {
	var err error
	obsCtx := d.obs.StartTracesOp(req.Context())
	var spansCount int
	defer func(spansCount *int) {
		d.obs.EndTracesOp(obsCtx, "datadog", *spansCount, err)
	}(&spansCount)

	ts := tags(v, req.Header)
	tp, err := decodeTracerPayload(v, req, ts)

	if err != nil {
		errStr := fmt.Sprintf("Failed to decode datadog traces, %v", err)
		fmt.Println(errStr)
		http.Error(w, errStr, http.StatusBadRequest)
		return
	}
	traces, spansCount := translateTraces(tp)
	err = d.nextConsumer.ConsumeTraces(req.Context(), traces)
	if err != nil {
		errStr := fmt.Sprintf("Failed to consume traces, %v", err)
		fmt.Println(errStr)
		http.Error(w, errStr, http.StatusInternalServerError)
		return
	}
}
