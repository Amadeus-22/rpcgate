# 0004 — In-process state (no Redis) for the MVP

Status: accepted · 2026-09

## Context

rpcgate keeps four kinds of state: upstream health (head, latency, breaker),
client rate-limit buckets, upstream `max_rps` buckets and the response cache.
Sharing them across replicas would need Redis or similar: another service to
run, a network hop on the request path, and a new failure mode (gateway up,
state store down).

## Decision

All state lives in process memory, owned by `app.Pool`, `app.RateLimiter` and
`adapter/lrucache`. Each upstream has its own mutex so routing and probing do
not contend globally. The cache sits behind the `app.Cache` port.

## Consequences

- One binary, no dependencies, sub-microsecond state access.
- With N replicas, rate limits are per replica (a key effectively gets N × rps)
  and each replica warms its own cache. Health state converges independently in
  each replica within one probe interval, which is fine: it is derived from
  upstreams, not shared truth.
- A restart loses the cache and breaker history; the first probe round runs
  before the listener opens, so routing is correct from the first request.
- Phase 3 can add a Redis `Cache` adapter and a shared limiter without touching
  the router, because both are already behind ports.
