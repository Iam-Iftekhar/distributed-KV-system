package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/iam-iftekhar/kvraft/internal/config"
	"github.com/iam-iftekhar/kvraft/internal/kv"
	"github.com/iam-iftekhar/kvraft/internal/logger"
	"github.com/iam-iftekhar/kvraft/internal/server"
	"github.com/iam-iftekhar/kvraft/internal/storage"
)

func main() {
	cfg := config.Parse()

	var level slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	log := logger.Setup(level, cfg.NodeID)
	log.Info("Starting KVRaft Node", "node_id", cfg.NodeID, "port", cfg.Port)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Initialize WAL
	dataDir := "./data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Error("Failed to create data directory", "error", err)
		os.Exit(1)
	}

	walPath := filepath.Join(dataDir, cfg.NodeID+".wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		log.Error("Failed to open WAL file", "error", err)
		os.Exit(1)
	}
	defer wal.Close()

	// 2. Initialize store and perform WAL replay (Recovery)
	store := kv.NewMemoryStore()
	entries, err := wal.Replay()
	if err != nil {
		log.Error("Failed to replay WAL", "error", err)
		os.Exit(1)
	}

	for _, entry := range entries {
		switch entry.Op {
		case "SET":
			store.Set(entry.Key, entry.Value)
		case "DELETE":
			store.Delete(entry.Key)
		}
	}
	log.Info("WAL replay complete", "recovered_entries", len(entries))

	// 3. Start TCP server with WAL integration wrapper
	durableStore := &DurableKVStore{
		store: store,
		wal:   wal,
	}

	tcpServer := server.NewTCPServer(cfg.Port, durableStore, log)

	go func() {
		if err := tcpServer.Start(ctx); err != nil {
			log.Error("Server stopped with error", "error", err)
		}
	}()

	<-ctx.Done()
	log.Info("Shutdown signal received, stopping server...")

	tcpServer.Stop()
	log.Info("Server stopped successfully")
}

// DurableKVStore decorates kv.Store to write to WAL before mutating memory
type DurableKVStore struct {
	store kv.Store
	wal   *storage.WAL
}

func (d *DurableKVStore) Set(key, value string) {
	if err := d.wal.Write(storage.LogEntry{Op: "SET", Key: key, Value: value}); err != nil {
		slog.Error("WAL write failed", "op", "SET", "key", key, "error", err)
		return
	}
	d.store.Set(key, value)
}

func (d *DurableKVStore) Get(key string) (string, bool) {
	return d.store.Get(key)
}

func (d *DurableKVStore) Delete(key string) {
	if err := d.wal.Write(storage.LogEntry{Op: "DELETE", Key: key}); err != nil {
		slog.Error("WAL write failed", "op", "DELETE", "key", key, "error", err)
		return
	}
	d.store.Delete(key)
}