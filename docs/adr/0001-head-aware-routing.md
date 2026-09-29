# 0001 — Head-aware routing

Status: accepted · 2026-09

## Context

Providers behind one endpoint are never at exactly the same block. A request for
block N sent to a node whose head is N-2 returns `null` or "header not found" for
a block that exists. Clients read that as "no such block" and fail or, worse, act
on it. Round-robin or latency-only balancing makes this a random, per-request
failure that is hard to debug.

## Decision

The health checker tracks each upstream's head (`eth_blockNumber` every
`health.interval`). `jsonrpc.Block` extracts the block every request depends on
and `selection.Eligible` filters upstreams by it:

- **Explicit number N** (hex, `earliest`, EIP-1898 `{blockNumber}`, `eth_getLogs` `toBlock`):
  upstreams with head ≥ N, *including* ones lagging more than `max_lag_blocks` —
  a lagging node is still a correct answer for old blocks. A block above every
  head goes to the best-head upstreams, which answer as any node would (`null`).
- **Tip** (`latest`, `pending`, `safe`, `finalized`, omitted block, a block hash,
  unknown methods): only upstreams at the best head among available ones.
- **Head-independent** (`eth_chainId`, `net_version`, `eth_sendRawTransaction`…):
  any upstream lagging at most `max_lag_blocks`.

Hash lookups are treated as tip because a hash says nothing about height; only the
freshest node is sure to have seen it.

## Consequences

- No "block not found" for blocks that exist, as long as one upstream has them.
- Tip traffic concentrates on whichever upstreams are exactly at the best head.
  Among those, equal upstreams are rotated so load still spreads. A tolerance of
  one block would spread more at the cost of occasionally serving a head one
  block old; it is a roadmap option, not the default.
- On retry, the tried upstream is excluded and the best head is recomputed over
  the rest, so a retried `latest` may be answered one block behind. That is
  preferable to failing.
- Head freshness is bounded by the probe interval (default 2 s, about 1/6 of an
  Ethereum slot).
