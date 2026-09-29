// Package logging builds the single JSON logger passed through rpcgate.
package logging

import (
	"io"
	"log/slog"
)

// New returns a JSON slog logger writing to w at level.
func New(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}
