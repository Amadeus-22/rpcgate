# 0002 — Never auto-retry writes

Status: accepted · 2026-09

## Context

A timeout or 5xx from a provider does not say whether `eth_sendRawTransaction`
reached the mempool. Retrying on another provider is usually harmless for a
signed transaction (same hash, same nonce), but not always: providers differ in
mempool policy, private-relay providers can front-run or leak order flow, and a
client that also retries produces duplicate submissions whose errors
("nonce too low", "already known") look like failures. Hiding the ambiguity makes
the client's own retry logic wrong.

## Decision

`jsonrpc.IsWrite` methods (`eth_sendRawTransaction`, `eth_sendTransaction`) get
exactly one attempt on one upstream. On failure the router reports what it knows:

- HTTP 429, other 4xx, or JSON-RPC `-32005`: the provider refused; nothing was
  sent. The client gets the provider's answer or "upstream request failed".
- Network error, timeout, 5xx, invalid response: `ErrWriteUnknown` — HTTP 502,
  `-32603` "transaction submission failed and was not retried; its outcome is
  unknown".

## Consequences

- The client, which owns the nonce and the signing key, decides whether to
  resend. It can check `eth_getTransactionByHash` first.
- Write availability is lower than read availability during provider incidents.
  The breaker still moves the *next* write away from a failing upstream.
- Chaos test `TestWriteIsNeverRetried` pins the behaviour for 500, timeout, 429
  and `-32005`.
