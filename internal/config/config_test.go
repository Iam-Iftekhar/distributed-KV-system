package config

import (
	"os"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	// Save os.Args and restore it later to avoid polluting test state
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Mock arguments for testing
	os.Args = []string{"cmd", "-id", "test-node", "-port", "9999"}
	
	cfg := Parse()
	
	if cfg.NodeID != "test-node" {
		t.Errorf("Expected NodeID 'test-node', got '%s'", cfg.NodeID)
	}
	
	if cfg.Port != 9999 {
		t.Errorf("Expected Port 9999, got %d", cfg.Port)
	}
}