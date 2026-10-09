package logger

import (
	"log/slog"
	"testing"
)

func TestSetupLogger(t *testing.T) {
	log := Setup(slog.LevelDebug, "test-node")
	if log == nil {
		t.Fatal("Expected non-nil logger")
	}
}