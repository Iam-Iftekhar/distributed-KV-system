package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/iam-iftekhar/kvraft/internal/kv"
)

func TestTCPServer_Integration(t *testing.T) {
	store := kv.NewMemoryStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := NewTCPServer(0, store, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := srv.Start(ctx)
		if err != nil {
			t.Errorf("Server stopped with error: %v", err)
		}
	}()

	time.Sleep(20 * time.Millisecond)

	if srv.listener == nil {
		t.Fatal("Server listener failed to initialize")
	}

	addr := srv.listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	executeCommand := func(cmd string) string {
		_, err := fmt.Fprintf(conn, "%s\n", cmd)
		if err != nil {
			t.Fatalf("Failed to send command: %v", err)
		}

		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read response: %v", err)
		}
		return strings.TrimSpace(response)
	}

	if resp := executeCommand("SET user_1 benimaru"); resp != "OK" {
		t.Errorf("Expected OK, got %q", resp)
	}

	if resp := executeCommand("GET user_1"); resp != "VALUE benimaru" {
		t.Errorf("Expected 'VALUE benimaru', got %q", resp)
	}

	if resp := executeCommand("GET non_existent"); resp != "NOT_FOUND" {
		t.Errorf("Expected NOT_FOUND, got %q", resp)
	}

	if resp := executeCommand("DELETE user_1"); resp != "OK" {
		t.Errorf("Expected OK, got %q", resp)
	}
	if resp := executeCommand("GET user_1"); resp != "NOT_FOUND" {
		t.Errorf("Expected NOT_FOUND, got %q", resp)
	}

	cancel()
	srv.Stop()
}