package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/iam-iftekhar/kvraft/internal/kv"
)

type TCPServer struct {
	port     int
	store    kv.Store
	listener net.Listener
	wg       sync.WaitGroup
	log      *slog.Logger
}

func NewTCPServer(port int, store kv.Store, log *slog.Logger) *TCPServer {
	return &TCPServer{
		port:  port,
		store: store,
		log:   log,
	}
}

// Start opens the TCP listener and blocks until context is cancelled or an error occurs.
func (s *TCPServer) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.log.Info("TCP server listening", "addr", addr)

	go func() {
		<-ctx.Done()
		s.log.Info("Context cancelled, closing TCP listener...")
		s.listener.Close()
	}()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection") {
				return nil
			}
			s.log.Error("Failed to accept connection", "error", err)
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *TCPServer) Stop() {
	s.wg.Wait()
	s.log.Info("All active connections closed")
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	s.log.Debug("New connection", "remote", remoteAddr)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		command := strings.ToUpper(parts[0])

		var response string

		switch command {
		case "SET":
			if len(parts) < 3 {
				response = "ERR wrong number of arguments for SET\n"
			} else {
				s.store.Set(parts[1], parts[2])
				response = "OK\n"
			}

		case "GET":
			if len(parts) != 2 {
				response = "ERR wrong number of arguments for GET\n"
			} else {
				val, ok := s.store.Get(parts[1])
				if ok {
					response = fmt.Sprintf("VALUE %s\n", val)
				} else {
					response = "NOT_FOUND\n"
				}
			}

		case "DELETE":
			if len(parts) != 2 {
				response = "ERR wrong number of arguments for DELETE\n"
			} else {
				s.store.Delete(parts[1])
				response = "OK\n"
			}

		default:
			response = fmt.Sprintf("ERR unknown command '%s'\n", command)
		}

		_, err := conn.Write([]byte(response))
		if err != nil {
			s.log.Error("Failed to write to connection", "remote", remoteAddr, "error", err)
			break
		}
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		s.log.Error("Connection read error", "remote", remoteAddr, "error", err)
	}
	s.log.Debug("Connection closed", "remote", remoteAddr)
}