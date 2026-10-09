package config

import (
	"flag"
)

type Config struct {
	NodeID   string
	Port     int
	LogLevel string
}

func Parse() Config {
	cfg := Config{}
	flag.StringVar(&cfg.NodeID, "id", "node1", "Unique ID for this node")
	flag.IntVar(&cfg.Port, "port", 8001, "Port to listen on")
	flag.StringVar(&cfg.LogLevel, "log", "info", "Log level (debug, info, warn, error)")
	flag.Parse()
	return cfg
}