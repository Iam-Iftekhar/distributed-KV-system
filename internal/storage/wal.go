package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var (
	ErrCorruptedEntry = errors.New("wal: corrupted entry detected")
)

type LogEntry struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"val,omitempty"`
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// Write appends a state command to the WAL and flushes to disk via fsync.
func (w *WAL) Write(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	payloadLen := uint32(len(payload))
	checksum := crc32.ChecksumIEEE(payload)

	// Format: [4B Length][4B Checksum][Payload]
	buf := make([]byte, 8+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], payloadLen)
	binary.BigEndian.PutUint32(buf[4:8], checksum)
	copy(buf[8:], payload)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	return w.file.Sync()
}

// Replay reads all valid operations from the WAL from the beginning.
func (w *WAL) Replay() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []LogEntry
	header := make([]byte, 8)

	for {
		_, err := io.ReadFull(w.file, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}

		length := binary.BigEndian.Uint32(header[0:4])
		expectedChecksum := binary.BigEndian.Uint32(header[4:8])

		payload := make([]byte, length)
		_, err = io.ReadFull(w.file, payload)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Corrupted / truncated trailing record from un-synced crash
				break
			}
			return nil, err
		}

		if crc32.ChecksumIEEE(payload) != expectedChecksum {
			return nil, ErrCorruptedEntry
		}

		var entry LogEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}