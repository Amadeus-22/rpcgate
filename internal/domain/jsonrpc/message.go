// Package jsonrpc holds the JSON-RPC 2.0 types rpcgate speaks and the Ethereum
// specific rules built on them: which methods write, which block a request
// names and which answers may be cached. It performs no I/O.
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Version is the only protocol version accepted.
const Version = "2.0"

// Standard JSON-RPC 2.0 error codes plus the Ethereum "limit exceeded" code
// (EIP-1474) that providers return when they throttle a caller.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternal       = -32603
	CodeLimitExceeded  = -32005
)

// ErrEmptyBatch is returned by Parse for a batch with no items.
var ErrEmptyBatch = errors.New("empty batch")

// Request is a single JSON-RPC call. ID is kept raw so the client's value
// (number, string or null) is echoed back byte for byte.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether the client omitted the id, in which case
// JSON-RPC 2.0 says no response is sent.
func (r Request) IsNotification() bool { return len(r.ID) == 0 }

// Response is a single JSON-RPC answer; exactly one of Result and Error is set.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is the JSON-RPC error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("json-rpc error %d: %s", e.Code, e.Message) }

// Retryable reports whether the error means "this provider refused to do the
// work right now", so the same call may succeed on another provider.
func (e *Error) Retryable() bool { return e != nil && e.Code == CodeLimitExceeded }

// NewResult builds a success response for id.
func NewResult(id, result json.RawMessage) Response {
	return Response{JSONRPC: Version, ID: nullIfEmpty(id), Result: result}
}

// NewError builds an error response for id.
func NewError(id json.RawMessage, code int, message string) Response {
	return Response{JSONRPC: Version, ID: nullIfEmpty(id), Error: &Error{Code: code, Message: message}}
}

func nullIfEmpty(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

// Item is one entry of an incoming message: either a valid request or the
// error response that item earns because it was malformed.
type Item struct {
	Request Request
	Invalid *Response
}

// Message is a parsed HTTP body.
type Message struct {
	Batch bool
	Items []Item
}

// Parse decodes a JSON-RPC body. A body that is not JSON (or an empty batch)
// is an error for the whole message; an individual malformed item inside a
// batch only invalidates that item, as JSON-RPC 2.0 requires.
func Parse(body []byte) (Message, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return Message{}, errors.New("parse body: empty")
	}
	if trimmed[0] != '[' {
		item, err := parseItem(trimmed)
		if err != nil {
			return Message{}, err
		}
		return Message{Items: []Item{item}}, nil
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(trimmed, &raws); err != nil {
		return Message{}, fmt.Errorf("parse batch: %w", err)
	}
	if len(raws) == 0 {
		return Message{}, ErrEmptyBatch
	}
	msg := Message{Batch: true, Items: make([]Item, 0, len(raws))}
	for _, raw := range raws {
		item, err := parseItem(raw)
		if err != nil {
			resp := NewError(nil, CodeInvalidRequest, "invalid request")
			item = Item{Invalid: &resp}
		}
		msg.Items = append(msg.Items, item)
	}
	return msg, nil
}

// parseItem returns an error only when raw is not a JSON object at all.
// Structurally valid objects that break the protocol become Invalid items.
func parseItem(raw []byte) (Item, error) {
	var req Request
	if err := json.Unmarshal(raw, &req); err != nil {
		return Item{}, fmt.Errorf("parse request: %w", err)
	}
	if problem := validate(req); problem != "" {
		resp := NewError(req.ID, CodeInvalidRequest, problem)
		return Item{Invalid: &resp}, nil
	}
	return Item{Request: req}, nil
}

func validate(req Request) string {
	switch {
	case req.JSONRPC != Version:
		return `invalid request: jsonrpc must be "2.0"`
	case req.Method == "":
		return "invalid request: method is required"
	case len(req.Params) > 0 && req.Params[0] != '[' && req.Params[0] != '{' && string(req.Params) != "null":
		return "invalid request: params must be an array or object"
	case len(req.ID) > 0 && req.ID[0] != '"' && req.ID[0] != 'n' && !isNumberStart(req.ID[0]):
		return "invalid request: id must be a string, number or null"
	}
	return ""
}

func isNumberStart(b byte) bool { return b == '-' || (b >= '0' && b <= '9') }

// PositionalParams decodes params as an array. Named (object) params and
// absent params yield an empty slice: Ethereum methods are all positional.
func PositionalParams(params json.RawMessage) ([]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(params)
	if len(trimmed) == 0 || string(trimmed) == "null" || trimmed[0] != '[' {
		return nil, nil
	}
	var out []json.RawMessage
	if err := json.Unmarshal(trimmed, &out); err != nil {
		return nil, fmt.Errorf("decode params: %w", err)
	}
	return out, nil
}
