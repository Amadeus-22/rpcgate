// Package metrics exposes rpcgate's Prometheus metrics. Recorder implements
// app.Metrics; every metric name is documented in the README.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
)

// Recorder owns a private registry, so tests can create as many as they like.
type Recorder struct {
	registry  *prometheus.Registry
	requests  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	head      *prometheus.GaugeVec
	lag       *prometheus.GaugeVec
	breaker   *prometheus.GaugeVec
	cacheHits *prometheus.CounterVec
	retries   *prometheus.CounterVec
}

// New registers all collectors, including Go runtime and process metrics.
func New() *Recorder {
	r := &Recorder{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpcgate_requests_total",
			Help: "Upstream call attempts by method, upstream and outcome (upstream=\"none\", outcome=\"no_upstream\" when nothing was eligible).",
		}, []string{"method", "upstream", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "rpcgate_request_duration_seconds",
			Help:    "Duration of upstream call attempts.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"method", "upstream"}),
		head: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "rpcgate_upstream_head",
			Help: "Latest block number reported by the upstream's health probe.",
		}, []string{"upstream"}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "rpcgate_upstream_lag_blocks",
			Help: "Blocks the upstream trails the best head.",
		}, []string{"upstream"}),
		breaker: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "rpcgate_breaker_state",
			Help: "Circuit breaker state: 0 closed, 1 half-open, 2 open.",
		}, []string{"upstream"}),
		cacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpcgate_cache_hits_total",
			Help: "Requests answered from the cache.",
		}, []string{"method"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpcgate_retries_total",
			Help: "Retries on another upstream after a retryable failure.",
		}, []string{"method"}),
	}
	r.registry.MustRegister(
		r.requests, r.duration, r.head, r.lag, r.breaker, r.cacheHits, r.retries,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return r
}

// Handler serves the registry in the Prometheus exposition format.
func (r *Recorder) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{Registry: r.registry})
}

// Registry is exposed for tests.
func (r *Recorder) Registry() *prometheus.Registry { return r.registry }

func (r *Recorder) UpstreamCall(method, name, outcome string, d time.Duration) {
	r.requests.WithLabelValues(method, name, outcome).Inc()
	r.duration.WithLabelValues(method, name).Observe(d.Seconds())
}

func (r *Recorder) NoUpstream(method string) {
	r.requests.WithLabelValues(method, "none", "no_upstream").Inc()
}

func (r *Recorder) Retry(method string) { r.retries.WithLabelValues(method).Inc() }

func (r *Recorder) CacheHit(method string) { r.cacheHits.WithLabelValues(method).Inc() }

func (r *Recorder) UpstreamState(name string, head, lag uint64, state upstream.BreakerState) {
	r.head.WithLabelValues(name).Set(float64(head))
	r.lag.WithLabelValues(name).Set(float64(lag))
	r.breaker.WithLabelValues(name).Set(float64(state))
}
