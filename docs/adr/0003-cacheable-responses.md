# 0003 — What is cacheable, and why

Status: accepted · 2026-09

## Context

Caching cuts provider cost and latency, but a stale answer from a JSON-RPC
gateway is a correctness bug: a reorged block, a receipt that disappeared, a
balance from three blocks ago. The cache must only hold answers that cannot change.

## Decision

`jsonrpc.CacheRuleFor` and `jsonrpc.Cacheable` define a short allow-list:

| Rule | Methods | Stored when |
|---|---|---|
| Forever | `eth_chainId`, `net_version` | always |
| Param final | `eth_getBlockByNumber`, `eth_getBlockReceipts`, `eth_getBlockTransactionCountByNumber`, `eth_getTransactionByBlockNumberAndIndex` | the request names a block number ≤ best head − `finality_depth` |
| Result final | `eth_getBlockByHash`, `eth_getTransactionByHash`, `eth_getTransactionReceipt` | the answer's block number ≤ best head − `finality_depth` |

Never cached: anything tagged `latest`/`pending`/`safe`/`finalized`, `null`
results ("not found yet" becomes "found" one block later), errors, and every
other method. The key is method plus compacted params. The client's `id` is
never part of the stored value.

## Consequences

- Correct under reorgs shallower than `finality_depth` (default 64 blocks, about
  two epochs on Ethereum mainnet); set it for the chain being served.
- `eth_call`/`eth_getBalance` at an old explicit block are deterministic but not
  cached: they are rarer, larger in variety, and caching them needs per-method
  review. They are easy to add to the table later.
- Hex-case differences in params (`0xAB` vs `0xab`) miss the cache; harmless.
- The in-process LRU is bounded by bytes (`cache.max_bytes`), see ADR 0004.
