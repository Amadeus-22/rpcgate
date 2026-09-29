// Package httpapi is rpcgate's HTTP surface: the JSON-RPC endpoint, the admin
// API, health endpoints and metrics.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Amadeus-22/rpcgate/internal/app"
	"github.com/Amadeus-22/rpcgate/internal/domain/jsonrpc"
)

// Config tunes the endpoint.
type Config struct {
	RequestTimeout time.Duration // deadline for a whole request, retries included
	MaxBatchSize   int
	MaxBodyBytes   int64
}

// Server holds the handlers' dependencies.
type Server struct {
	router  *app.Router
	limiter *app.RateLimiter
	pool    *app.Pool
	metrics http.Handler
	log     *slog.Logger
	cfg     Config
}

// New wires the HTTP surface.
func New(router *app.Router, limiter *app.RateLimiter, pool *app.Pool, metrics http.Handler, log *slog.Logger, cfg Config) *Server {
	return &Server{router: router, limiter: limiter, pool: pool, metrics: metrics, log: log, cfg: cfg}
}

// Handler returns the routed handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", s.handleRPC)
	mux.HandleFunc("GET /v1/upstreams", s.handleUpstreams)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", s.metrics)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, http.StatusNotFound, "not_found", "no such endpoint")
	})
	return mux
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "JSON-RPC requests must use POST")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeRPC(w, http.StatusRequestEntityTooLarge, jsonrpc.NewError(nil, jsonrpc.CodeInvalidRequest,
				"request body exceeds "+strconv.FormatInt(s.cfg.MaxBodyBytes, 10)+" bytes"))
			return
		}
		writeRPC(w, http.StatusBadRequest, jsonrpc.NewError(nil, jsonrpc.CodeParseError, "could not read request body"))
		return
	}

	msg, err := jsonrpc.Parse(body)
	switch {
	case errors.Is(err, jsonrpc.ErrEmptyBatch):
		writeRPC(w, http.StatusBadRequest, jsonrpc.NewError(nil, jsonrpc.CodeInvalidRequest, "empty batch"))
		return
	case err != nil:
		writeRPC(w, http.StatusBadRequest, jsonrpc.NewError(nil, jsonrpc.CodeParseError, "parse error: body is not valid JSON-RPC"))
		return
	case len(msg.Items) > s.cfg.MaxBatchSize:
		writeRPC(w, http.StatusBadRequest, jsonrpc.NewError(nil, jsonrpc.CodeInvalidRequest,
			"batch of "+strconv.Itoa(len(msg.Items))+" exceeds the limit of "+strconv.Itoa(s.cfg.MaxBatchSize)))
		return
	}

	reqs, index := validRequests(msg)
	if err := s.limiter.Allow(r.Header.Get("X-Api-Key"), max(1, len(reqs))); err != nil {
		s.writeLimitError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()

	if !msg.Batch {
		s.serveSingle(ctx, w, msg.Items[0])
		return
	}
	results := s.router.HandleBatch(ctx, reqs)
	out := make([]jsonrpc.Response, 0, len(msg.Items))
	for i, item := range msg.Items {
		if item.Invalid != nil {
			out = append(out, *item.Invalid)
			continue
		}
		if item.Request.IsNotification() {
			continue
		}
		res := results[index[i]]
		if res.Err != nil {
			resp, _ := s.gatewayError(ctx, item.Request, res.Err)
			out = append(out, resp)
			continue
		}
		out = append(out, res.Response)
	}
	if len(out) == 0 {
		w.WriteHeader(http.StatusNoContent) // a batch of notifications gets no body
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) serveSingle(ctx context.Context, w http.ResponseWriter, item jsonrpc.Item) {
	if item.Invalid != nil {
		writeRPC(w, http.StatusOK, *item.Invalid)
		return
	}
	resp, err := s.router.Handle(ctx, item.Request)
	status := http.StatusOK
	if err != nil {
		resp, status = s.gatewayError(ctx, item.Request, err)
	}
	if item.Request.IsNotification() {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeRPC(w, status, resp)
}

// validRequests returns the requests to route and, per message item, its
// position in that slice.
func validRequests(msg jsonrpc.Message) ([]jsonrpc.Request, map[int]int) {
	reqs := make([]jsonrpc.Request, 0, len(msg.Items))
	index := make(map[int]int, len(msg.Items))
	for i, item := range msg.Items {
		if item.Invalid == nil {
			index[i] = len(reqs)
			reqs = append(reqs, item.Request)
		}
	}
	return reqs, index
}

// gatewayError turns a router failure into the client-facing JSON-RPC error
// and HTTP status. The detailed error is logged here, once; the client gets
// a stable message that never names internal addresses.
func (s *Server) gatewayError(ctx context.Context, req jsonrpc.Request, err error) (jsonrpc.Response, int) {
	status, message := http.StatusBadGateway, "upstream request failed"
	switch {
	case errors.Is(err, app.ErrNoUpstream):
		status, message = http.StatusServiceUnavailable, "no healthy upstream"
	case errors.Is(err, app.ErrDeadline):
		status, message = http.StatusGatewayTimeout, "request deadline exceeded"
	case errors.Is(err, app.ErrWriteUnknown):
		message = app.ErrWriteUnknown.Error()
	}
	s.log.LogAttrs(ctx, slog.LevelWarn, "request failed",
		slog.String("method", req.Method), slog.Int("status", status), slog.String("error", err.Error()))
	return jsonrpc.NewError(req.ID, jsonrpc.CodeInternal, message), status
}

func (s *Server) writeLimitError(w http.ResponseWriter, err error) {
	if errors.Is(err, app.ErrRateLimited) {
		w.Header().Set("Retry-After", "1")
		writeRPC(w, http.StatusTooManyRequests, jsonrpc.NewError(nil, jsonrpc.CodeLimitExceeded, "rate limit exceeded"))
		return
	}
	writeRPC(w, http.StatusUnauthorized, jsonrpc.NewError(nil, jsonrpc.CodeInvalidRequest, err.Error()))
}

func (s *Server) handleUpstreams(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"upstreams": s.pool.Status()})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.pool.Ready() {
		writeAPIError(w, http.StatusServiceUnavailable, "no_upstream_reachable", "no upstream answered its last health probe")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeRPC(w http.ResponseWriter, status int, resp jsonrpc.Response) { writeJSON(w, status, resp) }

func writeAPIError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client may be gone; nothing useful to do
}
