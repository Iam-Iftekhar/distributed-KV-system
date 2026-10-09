package logger

import (
	"log/slog"
	"os"
)

// Setup creates a structured logger.
// In a distributed system, attaching the Node ID to every log is critical.
func Setup(level slog.Level, nodeID string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: level,
	}
	
	handler := slog.NewTextHandler(os.Stdout, opts)
	logger := slog.New(handler)
	
	if nodeID != "" {
		logger = logger.With("node_id", nodeID)
	}
	
	// Set as default for standard library compatibility
	slog.SetDefault(logger)
	return logger
}