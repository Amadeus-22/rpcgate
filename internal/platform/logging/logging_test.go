package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNewWritesJSONAtLevel(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, slog.LevelInfo)
	log.Debug("hidden")
	log.Info("shown", slog.String("upstream_name", "a"))

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("not one JSON line: %q", buf.String())
	}
	if line["msg"] != "shown" || line["upstream_name"] != "a" {
		t.Fatalf("got %v", line)
	}
}
