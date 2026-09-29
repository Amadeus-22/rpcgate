package jsonrpc

import (
	"bytes"
	"encoding/json"
	"strings"
)

// IsWrite reports whether a method changes chain state. Writes are sent to
// exactly one upstream and never retried: a timeout does not tell us whether
// the transaction was broadcast (ADR 0002).
func IsWrite(method string) bool {
	switch method {
	case "eth_sendRawTransaction", "eth_sendTransaction":
		return true
	}
	return false
}

// CacheRule says when a successful answer may be stored.
type CacheRule int

const (
	// CacheNever: the answer can change (anything touching latest state).
	CacheNever CacheRule = iota
	// CacheForever: the answer is fixed for the chain (eth_chainId).
	CacheForever
	// CacheIfParamFinal: cache when the request names a block by number and
	// that block is at least finality_depth below the best head.
	CacheIfParamFinal
	// CacheIfResultFinal: lookups by hash; cache when the block the answer
	// belongs to is at least finality_depth below the best head.
	CacheIfResultFinal
)

// cacheRules is deliberately short: only block and receipt lookups, whose
// answers are immutable once final (ADR 0003).
var cacheRules = map[string]CacheRule{
	"eth_chainId":                             CacheForever,
	"net_version":                             CacheForever,
	"eth_getBlockByNumber":                    CacheIfParamFinal,
	"eth_getBlockReceipts":                    CacheIfParamFinal,
	"eth_getBlockTransactionCountByNumber":    CacheIfParamFinal,
	"eth_getTransactionByBlockNumberAndIndex": CacheIfParamFinal,
	"eth_getBlockByHash":                      CacheIfResultFinal,
	"eth_getTransactionByHash":                CacheIfResultFinal,
	"eth_getTransactionReceipt":               CacheIfResultFinal,
}

// CacheRuleFor returns the cache rule for a method.
func CacheRuleFor(method string) CacheRule { return cacheRules[method] }

// Cacheable decides whether a successful result may be stored, given the
// request's block, the best head across upstreams and the finality depth.
func Cacheable(req Request, ref BlockRef, result json.RawMessage, bestHead, finalityDepth uint64) bool {
	if isNull(result) {
		// "not found" today may be "found" after the next block.
		return false
	}
	switch CacheRuleFor(req.Method) {
	case CacheForever:
		return true
	case CacheIfParamFinal:
		return ref.Kind == RefNumber && isFinal(ref.Number, bestHead, finalityDepth)
	case CacheIfResultFinal:
		n, ok := resultBlockNumber(req.Method, result)
		return ok && isFinal(n, bestHead, finalityDepth)
	}
	return false
}

func isFinal(block, bestHead, depth uint64) bool {
	return bestHead >= depth && block <= bestHead-depth
}

// resultBlockNumber reads the block height out of a block, transaction or
// receipt object. Pending transactions have a null blockNumber and are not final.
func resultBlockNumber(method string, result json.RawMessage) (uint64, bool) {
	var fields struct {
		Number      *string `json:"number"`
		BlockNumber *string `json:"blockNumber"`
	}
	if err := json.Unmarshal(result, &fields); err != nil {
		return 0, false
	}
	field := fields.BlockNumber
	if method == "eth_getBlockByHash" {
		field = fields.Number
	}
	if field == nil {
		return 0, false
	}
	n, err := ParseQuantity(*field)
	return n, err == nil
}

func isNull(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || string(trimmed) == "null"
}

// CacheKey identifies a request for caching: method plus compacted params.
func CacheKey(req Request) string {
	var buf bytes.Buffer
	buf.WriteString(req.Method)
	buf.WriteByte('|')
	if err := json.Compact(&buf, req.Params); err != nil {
		buf.Write(req.Params)
	}
	return buf.String()
}

// MetricLabel maps a client-supplied method name to a bounded label set, so a
// client sending random method names cannot blow up metric cardinality.
func MetricLabel(method string) string {
	if knownMethods[method] {
		return method
	}
	for _, prefix := range []string{"eth_", "net_", "web3_", "debug_", "trace_"} {
		if strings.HasPrefix(method, prefix) {
			return prefix + "other"
		}
	}
	return "other"
}

var knownMethods = func() map[string]bool {
	m := map[string]bool{
		"eth_blockNumber":          true,
		"eth_gasPrice":             true,
		"eth_maxPriorityFeePerGas": true,
		"eth_blobBaseFee":          true,
		"eth_syncing":              true,
		"eth_getLogs":              true,
		"eth_sendTransaction":      true,
	}
	for k := range blockParamIndex {
		m[k] = true
	}
	for k := range anyUpstreamMethods {
		m[k] = true
	}
	for k := range cacheRules {
		m[k] = true
	}
	return m
}()
