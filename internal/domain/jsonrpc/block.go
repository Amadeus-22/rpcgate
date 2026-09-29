package jsonrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// RefKind says which upstreams can answer a request, based on the block it names.
type RefKind int

const (
	// RefAny: the answer does not depend on chain state (eth_chainId) or must
	// not be pinned to a head (eth_sendRawTransaction). Any healthy upstream.
	RefAny RefKind = iota
	// RefTip: the answer depends on the newest state (latest, pending, safe,
	// finalized, an omitted block, a block hash, eth_blockNumber...). Only
	// upstreams at the best known head.
	RefTip
	// RefNumber: the request names block Number. Upstreams whose head >= Number.
	RefNumber
)

func (k RefKind) String() string {
	switch k {
	case RefAny:
		return "any"
	case RefTip:
		return "tip"
	case RefNumber:
		return "number"
	}
	return "unknown"
}

// BlockRef is the block a request depends on.
type BlockRef struct {
	Kind   RefKind
	Number uint64 // set when Kind == RefNumber
}

// ErrInvalidBlock is returned when a block parameter cannot be understood.
var ErrInvalidBlock = errors.New("invalid block parameter")

// blockParamIndex maps methods to the position of their block parameter.
// A missing optional parameter means "latest", as in every Ethereum client.
var blockParamIndex = map[string]int{
	"eth_getBalance":                          1,
	"eth_getCode":                             1,
	"eth_getTransactionCount":                 1,
	"eth_getStorageAt":                        2,
	"eth_call":                                1,
	"eth_estimateGas":                         1,
	"eth_createAccessList":                    1,
	"eth_getProof":                            2,
	"eth_feeHistory":                          1,
	"eth_getBlockByNumber":                    0,
	"eth_getBlockReceipts":                    0,
	"eth_getBlockTransactionCountByNumber":    0,
	"eth_getTransactionByBlockNumberAndIndex": 0,
	"eth_getUncleCountByBlockNumber":          0,
	"eth_getUncleByBlockNumberAndIndex":       0,
}

// anyUpstreamMethods can be served by any healthy upstream regardless of head.
var anyUpstreamMethods = map[string]bool{
	"eth_chainId":            true,
	"net_version":            true,
	"net_listening":          true,
	"web3_clientVersion":     true,
	"web3_sha3":              true,
	"eth_sendRawTransaction": true,
}

// Block extracts the block a request depends on. Methods rpcgate does not
// know are treated as RefTip: routing them to the freshest upstream is never wrong.
func Block(req Request) (BlockRef, error) {
	if anyUpstreamMethods[req.Method] {
		return BlockRef{Kind: RefAny}, nil
	}
	if req.Method == "eth_getLogs" {
		return logsBlock(req.Params)
	}
	idx, ok := blockParamIndex[req.Method]
	if !ok {
		return BlockRef{Kind: RefTip}, nil
	}
	params, err := PositionalParams(req.Params)
	if err != nil {
		return BlockRef{}, fmt.Errorf("%w: %w", ErrInvalidBlock, err)
	}
	if idx >= len(params) {
		return BlockRef{Kind: RefTip}, nil
	}
	return parseBlockParam(params[idx])
}

// parseBlockParam understands tags, hex numbers, block hashes and the
// EIP-1898 object form {"blockNumber": ...} / {"blockHash": ...}.
func parseBlockParam(raw json.RawMessage) (BlockRef, error) {
	if len(raw) > 0 && raw[0] == '{' {
		var obj struct {
			BlockNumber *string `json:"blockNumber"`
			BlockHash   *string `json:"blockHash"`
		}
		if err := json.Unmarshal(raw, &obj); err != nil {
			return BlockRef{}, fmt.Errorf("%w: %w", ErrInvalidBlock, err)
		}
		switch {
		case obj.BlockHash != nil:
			return BlockRef{Kind: RefTip}, nil
		case obj.BlockNumber != nil:
			return parseBlockString(*obj.BlockNumber)
		}
		return BlockRef{}, fmt.Errorf("%w: object needs blockNumber or blockHash", ErrInvalidBlock)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return BlockRef{}, fmt.Errorf("%w: %s", ErrInvalidBlock, raw)
	}
	return parseBlockString(s)
}

func parseBlockString(s string) (BlockRef, error) {
	switch s {
	case "latest", "pending", "safe", "finalized":
		return BlockRef{Kind: RefTip}, nil
	case "earliest":
		return BlockRef{Kind: RefNumber, Number: 0}, nil
	}
	if isHash(s) {
		// A hash says nothing about height; the freshest upstreams are the
		// only ones certain to have seen it.
		return BlockRef{Kind: RefTip}, nil
	}
	n, err := ParseQuantity(s)
	if err != nil {
		return BlockRef{}, fmt.Errorf("%w: %w", ErrInvalidBlock, err)
	}
	return BlockRef{Kind: RefNumber, Number: n}, nil
}

// logsBlock routes eth_getLogs by the upper end of its range: an upstream that
// has toBlock has every block below it.
func logsBlock(params json.RawMessage) (BlockRef, error) {
	list, err := PositionalParams(params)
	if err != nil {
		return BlockRef{}, fmt.Errorf("%w: %w", ErrInvalidBlock, err)
	}
	if len(list) == 0 {
		return BlockRef{}, fmt.Errorf("%w: eth_getLogs needs a filter object", ErrInvalidBlock)
	}
	var filter struct {
		ToBlock   *string `json:"toBlock"`
		BlockHash *string `json:"blockHash"`
	}
	if err := json.Unmarshal(list[0], &filter); err != nil {
		return BlockRef{}, fmt.Errorf("%w: %w", ErrInvalidBlock, err)
	}
	if filter.BlockHash != nil || filter.ToBlock == nil {
		return BlockRef{Kind: RefTip}, nil
	}
	return parseBlockString(*filter.ToBlock)
}

// ParseQuantity parses an Ethereum hex quantity such as "0x1b4".
func ParseQuantity(s string) (uint64, error) {
	digits, ok := strings.CutPrefix(s, "0x")
	if !ok || digits == "" {
		return 0, fmt.Errorf("quantity %q: missing 0x prefix or digits", s)
	}
	n, err := strconv.ParseUint(digits, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("quantity %q: %w", s, err)
	}
	return n, nil
}

// FormatQuantity is the inverse of ParseQuantity.
func FormatQuantity(n uint64) string { return "0x" + strconv.FormatUint(n, 16) }

func isHash(s string) bool {
	digits, ok := strings.CutPrefix(s, "0x")
	if !ok || len(digits) != 64 {
		return false
	}
	for _, c := range digits {
		if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
			return false
		}
	}
	return true
}
