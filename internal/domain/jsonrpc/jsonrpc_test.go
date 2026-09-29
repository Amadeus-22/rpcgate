package jsonrpc

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantErr   bool
		wantBatch bool
		wantItems int
		invalid   []int // indexes of items expected to be invalid
	}{
		{name: "single", body: `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`, wantItems: 1},
		{name: "single with whitespace", body: " \n{\"jsonrpc\":\"2.0\",\"id\":\"a\",\"method\":\"eth_chainId\",\"params\":[]}", wantItems: 1},
		{name: "batch", body: `[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":2,"method":"b"}]`, wantBatch: true, wantItems: 2},
		{name: "batch with invalid item", body: `[{"jsonrpc":"2.0","id":1,"method":"a"},1,{"jsonrpc":"1.0","id":3,"method":"c"}]`, wantBatch: true, wantItems: 3, invalid: []int{1, 2}},
		{name: "empty body", body: "  ", wantErr: true},
		{name: "not json", body: "{nope", wantErr: true},
		{name: "empty batch", body: "[]", wantErr: true},
		{name: "wrong version", body: `{"jsonrpc":"1.0","id":1,"method":"a"}`, wantItems: 1, invalid: []int{0}},
		{name: "missing method", body: `{"jsonrpc":"2.0","id":1}`, wantItems: 1, invalid: []int{0}},
		{name: "scalar params", body: `{"jsonrpc":"2.0","id":1,"method":"a","params":5}`, wantItems: 1, invalid: []int{0}},
		{name: "object id", body: `{"jsonrpc":"2.0","id":{},"method":"a"}`, wantItems: 1, invalid: []int{0}},
		{name: "null id", body: `{"jsonrpc":"2.0","id":null,"method":"a"}`, wantItems: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := Parse([]byte(tt.body))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if msg.Batch != tt.wantBatch || len(msg.Items) != tt.wantItems {
				t.Fatalf("batch=%v items=%d, want batch=%v items=%d", msg.Batch, len(msg.Items), tt.wantBatch, tt.wantItems)
			}
			invalid := map[int]bool{}
			for _, i := range tt.invalid {
				invalid[i] = true
			}
			for i, item := range msg.Items {
				if (item.Invalid != nil) != invalid[i] {
					t.Errorf("item %d invalid = %v, want %v", i, item.Invalid != nil, invalid[i])
				}
				if item.Invalid != nil && item.Invalid.Error.Code != CodeInvalidRequest {
					t.Errorf("item %d code = %d", i, item.Invalid.Error.Code)
				}
			}
		})
	}
}

func TestEmptyBatchSentinel(t *testing.T) {
	if _, err := Parse([]byte("[ ]")); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("err = %v, want ErrEmptyBatch", err)
	}
}

func TestNotification(t *testing.T) {
	msg, err := Parse([]byte(`{"jsonrpc":"2.0","method":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !msg.Items[0].Request.IsNotification() {
		t.Fatal("request without id should be a notification")
	}
	msg, _ = Parse([]byte(`{"jsonrpc":"2.0","id":null,"method":"a"}`))
	if msg.Items[0].Request.IsNotification() {
		t.Fatal("id null is not a notification")
	}
}

func TestResponseConstructors(t *testing.T) {
	r := NewError(nil, CodeInternal, "boom")
	out, _ := json.Marshal(r)
	if string(out) != `{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"boom"}}` {
		t.Fatalf("got %s", out)
	}
	r = NewResult(json.RawMessage(`"x"`), json.RawMessage(`"0x1"`))
	out, _ = json.Marshal(r)
	if string(out) != `{"jsonrpc":"2.0","id":"x","result":"0x1"}` {
		t.Fatalf("got %s", out)
	}
	if (&Error{Code: CodeLimitExceeded}).Retryable() != true || (&Error{Code: -32000}).Retryable() {
		t.Fatal("only -32005 is retryable")
	}
	if (&Error{Code: 1, Message: "m"}).Error() == "" {
		t.Fatal("empty error string")
	}
}

func req(method, params string) Request {
	return Request{JSONRPC: Version, ID: json.RawMessage("1"), Method: method, Params: json.RawMessage(params)}
}

func TestBlock(t *testing.T) {
	hash := `"0x` + "ab" + "000000000000000000000000000000000000000000000000000000000000cd" + `"`
	tests := []struct {
		name    string
		req     Request
		want    BlockRef
		wantErr bool
	}{
		{"chainId is any", req("eth_chainId", ``), BlockRef{Kind: RefAny}, false},
		{"send raw tx is any", req("eth_sendRawTransaction", `["0x00"]`), BlockRef{Kind: RefAny}, false},
		{"blockNumber is tip", req("eth_blockNumber", `[]`), BlockRef{Kind: RefTip}, false},
		{"unknown method is tip", req("foo_bar", `[]`), BlockRef{Kind: RefTip}, false},
		{"balance latest", req("eth_getBalance", `["0xabc","latest"]`), BlockRef{Kind: RefTip}, false},
		{"balance omitted block", req("eth_getBalance", `["0xabc"]`), BlockRef{Kind: RefTip}, false},
		{"balance number", req("eth_getBalance", `["0xabc","0x64"]`), BlockRef{Kind: RefNumber, Number: 100}, false},
		{"storage number", req("eth_getStorageAt", `["0xabc","0x0","0x10"]`), BlockRef{Kind: RefNumber, Number: 16}, false},
		{"block by number", req("eth_getBlockByNumber", `["0x1b4",false]`), BlockRef{Kind: RefNumber, Number: 436}, false},
		{"earliest", req("eth_getBlockByNumber", `["earliest",false]`), BlockRef{Kind: RefNumber, Number: 0}, false},
		{"pending", req("eth_getBlockByNumber", `["pending",false]`), BlockRef{Kind: RefTip}, false},
		{"finalized", req("eth_getBlockByNumber", `["finalized",false]`), BlockRef{Kind: RefTip}, false},
		{"call eip1898 number", req("eth_call", `[{},{"blockNumber":"0x10"}]`), BlockRef{Kind: RefNumber, Number: 16}, false},
		{"call eip1898 hash", req("eth_call", `[{},{"blockHash":`+hash+`}]`), BlockRef{Kind: RefTip}, false},
		{"call hash string", req("eth_call", `[{},`+hash+`]`), BlockRef{Kind: RefTip}, false},
		{"call eip1898 empty", req("eth_call", `[{},{}]`), BlockRef{}, true},
		{"bad hex", req("eth_getBlockByNumber", `["0xzz",false]`), BlockRef{}, true},
		{"no prefix", req("eth_getBlockByNumber", `["100",false]`), BlockRef{}, true},
		{"not a string", req("eth_getBlockByNumber", `[100,false]`), BlockRef{}, true},
		{"bad params", req("eth_getBlockByNumber", `[`), BlockRef{}, true},
		{"logs to number", req("eth_getLogs", `[{"fromBlock":"0x1","toBlock":"0x20"}]`), BlockRef{Kind: RefNumber, Number: 32}, false},
		{"logs to latest", req("eth_getLogs", `[{"fromBlock":"0x1","toBlock":"latest"}]`), BlockRef{Kind: RefTip}, false},
		{"logs no to", req("eth_getLogs", `[{"fromBlock":"0x1"}]`), BlockRef{Kind: RefTip}, false},
		{"logs by hash", req("eth_getLogs", `[{"blockHash":`+hash+`}]`), BlockRef{Kind: RefTip}, false},
		{"logs no filter", req("eth_getLogs", `[]`), BlockRef{}, true},
		{"logs bad filter", req("eth_getLogs", `[5]`), BlockRef{}, true},
		{"logs bad params", req("eth_getLogs", `[`), BlockRef{}, true},
		{"named params are ignored", req("eth_getBalance", `{"a":1}`), BlockRef{Kind: RefTip}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Block(tt.req)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidBlock) {
					t.Fatalf("err %v does not wrap ErrInvalidBlock", err)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestQuantity(t *testing.T) {
	tests := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{"0x0", 0, false},
		{"0x1b4", 436, false},
		{"0xFF", 255, false},
		{"0x", 0, true},
		{"1b4", 0, true},
		{"0x10000000000000000", 0, true},
	}
	for _, tt := range tests {
		got, err := ParseQuantity(tt.in)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("ParseQuantity(%q) = %d, %v", tt.in, got, err)
		}
		if !tt.wantErr && FormatQuantity(got) != "0x"+trimLower(tt.in[2:]) {
			t.Errorf("FormatQuantity(%d) = %s", got, FormatQuantity(got))
		}
	}
}

func trimLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'F' {
			b[i] = c + 32
		}
	}
	return string(b)
}

func TestRefKindString(t *testing.T) {
	for k, want := range map[RefKind]string{RefAny: "any", RefTip: "tip", RefNumber: "number", RefKind(9): "unknown"} {
		if k.String() != want {
			t.Errorf("%d.String() = %s", k, k.String())
		}
	}
}

func TestIsWrite(t *testing.T) {
	if !IsWrite("eth_sendRawTransaction") || !IsWrite("eth_sendTransaction") || IsWrite("eth_call") {
		t.Fatal("write classification wrong")
	}
}

func TestCacheable(t *testing.T) {
	const best, depth = 1000, 64
	tests := []struct {
		name   string
		req    Request
		result string
		want   bool
	}{
		{"chainId forever", req("eth_chainId", `[]`), `"0x1"`, true},
		{"final block by number", req("eth_getBlockByNumber", `["0x100",false]`), `{"number":"0x100"}`, true},
		{"boundary block is final", req("eth_getBlockByNumber", `["0x3a8",false]`), `{"number":"0x3a8"}`, true}, // 936 = 1000-64
		{"recent block by number", req("eth_getBlockByNumber", `["0x3a9",false]`), `{"number":"0x3a9"}`, false},
		{"latest never", req("eth_getBlockByNumber", `["latest",false]`), `{"number":"0x1"}`, false},
		{"null never", req("eth_getBlockByNumber", `["0x1",false]`), `null`, false},
		{"final receipt", req("eth_getTransactionReceipt", `["0xaa"]`), `{"blockNumber":"0x10"}`, true},
		{"recent receipt", req("eth_getTransactionReceipt", `["0xaa"]`), `{"blockNumber":"0x3e7"}`, false},
		{"pending tx", req("eth_getTransactionByHash", `["0xaa"]`), `{"blockNumber":null}`, false},
		{"final block by hash", req("eth_getBlockByHash", `["0xaa",false]`), `{"number":"0x10"}`, true},
		{"block by hash reads number", req("eth_getBlockByHash", `["0xaa",false]`), `{"blockNumber":"0x10"}`, false},
		{"garbage result", req("eth_getTransactionReceipt", `["0xaa"]`), `[1]`, false},
		{"balance never", req("eth_getBalance", `["0xa","0x1"]`), `"0x0"`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := Block(tt.req)
			if err != nil {
				t.Fatal(err)
			}
			if got := Cacheable(tt.req, ref, json.RawMessage(tt.result), best, depth); got != tt.want {
				t.Fatalf("Cacheable = %v, want %v", got, tt.want)
			}
		})
	}
	if Cacheable(req("eth_getBlockByNumber", `["0x0",false]`), BlockRef{Kind: RefNumber}, json.RawMessage(`{}`), 10, 64) {
		t.Fatal("nothing is final while the chain is shorter than the finality depth")
	}
}

func TestCacheKey(t *testing.T) {
	a := CacheKey(req("eth_getBlockByNumber", `["0x1", false]`))
	b := CacheKey(req("eth_getBlockByNumber", "[ \"0x1\",\n false ]"))
	if a != b || a != `eth_getBlockByNumber|["0x1",false]` {
		t.Fatalf("keys differ: %q %q", a, b)
	}
	if CacheKey(req("eth_chainId", ``)) != "eth_chainId|" {
		t.Fatal("empty params key")
	}
}

func TestMetricLabel(t *testing.T) {
	for in, want := range map[string]string{
		"eth_call":        "eth_call",
		"eth_blockNumber": "eth_blockNumber",
		"eth_madeUp123":   "eth_other",
		"debug_trace":     "debug_other",
		"DROP TABLE":      "other",
	} {
		if got := MetricLabel(in); got != want {
			t.Errorf("MetricLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPositionalParams(t *testing.T) {
	for _, in := range []string{``, `null`, `{"a":1}`} {
		got, err := PositionalParams(json.RawMessage(in))
		if err != nil || got != nil {
			t.Errorf("PositionalParams(%q) = %v, %v", in, got, err)
		}
	}
	got, err := PositionalParams(json.RawMessage(`[1,"a"]`))
	if err != nil || len(got) != 2 {
		t.Fatalf("got %v, %v", got, err)
	}
}
