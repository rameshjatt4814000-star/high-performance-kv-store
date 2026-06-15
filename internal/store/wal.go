package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	RecordTypeSet    byte = 1
	RecordTypeDelete byte = 2
)

var (
	ErrChecksumMismatch = errors.New("wal record checksum mismatch")
	ErrCorruptRecord     = errors.New("wal record corrupted")
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
	size int64
}

// NewWAL opens or creates a WAL file in the given directory.
func NewWAL(dataDir string, filename string) (*WAL, error) {
	path := filepath.Join(dataDir, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat wal file: %w", err)
	}

	return &WAL{
		file: file,
		path: path,
		size: info.Size(),
	}, nil
}

// Append logs a mutation to the WAL on disk.
func (w *WAL) Append(recType byte, key string, value []byte, version uint64, expiresAt time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keyBytes := []byte(key)
	keyLen := len(keyBytes)
	valLen := len(value)

	if keyLen > 65535 {
		return fmt.Errorf("key length %d exceeds maximum of 65535", keyLen)
	}

	// Payload is: Type(1) + Version(8) + ExpiresAt(8) + KeyLen(2) + Key + Value
	payloadSize := 1 + 8 + 8 + 2 + keyLen + valLen
	recordSize := 4 + 4 + payloadSize // Length(4) + CRC(4) + Payload

	buf := make([]byte, recordSize)

	// Length (4 bytes)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadSize))

	// Payload construction
	payload := buf[8:]
	payload[0] = recType
	binary.BigEndian.PutUint64(payload[1:9], version)

	var expNano int64
	if !expiresAt.IsZero() {
		expNano = expiresAt.UnixNano()
	}
	binary.BigEndian.PutUint64(payload[9:17], uint64(expNano))
	binary.BigEndian.PutUint16(payload[17:19], uint16(keyLen))
	copy(payload[19:19+keyLen], keyBytes)
	if valLen > 0 {
		copy(payload[19+keyLen:], value)
	}

	// Calculate CRC32 over the payload
	checksum := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(buf[4:8], checksum)

	// Write to file
	n, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write to wal: %w", err)
	}
	w.size += int64(n)

	// Force sync to disk for durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %w", err)
	}

	return nil
}

// Replay reads the WAL file from the start and applies all records using the callback.
func (w *WAL) Replay(applyFunc func(recType byte, key string, value []byte, version uint64, expiresAt time.Time) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to start
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start of wal: %w", err)
	}

	reader := w.file
	headerBuf := make([]byte, 8)

	for {
		_, err := io.ReadFull(reader, headerBuf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // Normal termination
			}
			return fmt.Errorf("failed to read wal header: %w", err)
		}

		payloadSize := binary.BigEndian.Uint32(headerBuf[0:4])
		expectedCrc := binary.BigEndian.Uint32(headerBuf[4:8])

		payload := make([]byte, payloadSize)
		_, err = io.ReadFull(reader, payload)
		if err != nil {
			return fmt.Errorf("failed to read wal payload: %w", err)
		}

		// Verify checksum
		actualCrc := crc32.ChecksumIEEE(payload)
		if actualCrc != expectedCrc {
			return ErrChecksumMismatch
		}

		// Parse payload
		if len(payload) < 19 {
			return ErrCorruptRecord
		}

		recType := payload[0]
		version := binary.BigEndian.Uint64(payload[1:9])
		expNano := int64(binary.BigEndian.Uint64(payload[9:17]))
		keyLen := int(binary.BigEndian.Uint16(payload[17:19]))

		if len(payload) < 19+keyLen {
			return ErrCorruptRecord
		}

		key := string(payload[19 : 19+keyLen])
		var val []byte
		if len(payload) > 19+keyLen {
			val = payload[19+keyLen:]
		}

		var expiresAt time.Time
		if expNano > 0 {
			expiresAt = time.Unix(0, expNano)
		}

		if err := applyFunc(recType, key, val, version, expiresAt); err != nil {
			return fmt.Errorf("failed to apply wal record: %w", err)
		}
	}

	// Seek back to end for future append operations
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end of wal: %w", err)
	}

	return nil
}

// Close closes the underlying log file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Size returns current size of WAL file.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Path returns the path to the WAL file.
func (w *WAL) Path() string {
	return w.path
}
