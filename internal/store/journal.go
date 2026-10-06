package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Record struct {
	Kind      string          `json:"kind"`
	EntityID  string          `json:"entity_id"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type journalKey struct {
	kind     string
	entityID string
}

type Journal struct {
	mu        sync.Mutex
	path      string
	committed map[journalKey]Record
	indexed   bool
}

func NewJournal(path string) *Journal {
	return &Journal{path: path}
}

func (j *Journal) Append(kind, entityID string, value any, at time.Time) error {
	if kind == "" || entityID == "" {
		return errors.New("journal kind and entity ID are required")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode journal payload: %w", err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ensureIndexedLocked(); err != nil {
		return err
	}
	if err := j.appendLocked(kind, entityID, payload, at); err != nil {
		return err
	}
	j.committed[journalKey{kind, entityID}] = Record{
		Kind: kind, EntityID: entityID, Timestamp: at.UTC(),
		Payload: append(json.RawMessage(nil), payload...),
	}
	return nil
}

// CommitOnce appends value under (kind, entityID) only if that key has not been
// committed before. When a new record is written it returns the record with
// appended=true. When the key already exists it returns the previously stored
// record with appended=false and writes nothing, so retries never produce a
// duplicate line.
func (j *Journal) CommitOnce(kind, entityID string, value any, at time.Time) (Record, bool, error) {
	if kind == "" || entityID == "" {
		return Record{}, false, errors.New("journal kind and entity ID are required")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return Record{}, false, fmt.Errorf("encode journal payload: %w", err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ensureIndexedLocked(); err != nil {
		return Record{}, false, err
	}
	key := journalKey{kind, entityID}
	if existing, ok := j.committed[key]; ok {
		existing.Payload = append(json.RawMessage(nil), existing.Payload...)
		return existing, false, nil
	}
	if err := j.appendLocked(kind, entityID, payload, at); err != nil {
		return Record{}, false, err
	}
	record := Record{Kind: kind, EntityID: entityID, Timestamp: at.UTC(), Payload: payload}
	j.committed[key] = Record{
		Kind: kind, EntityID: entityID, Timestamp: at.UTC(),
		Payload: append(json.RawMessage(nil), payload...),
	}
	return record, true, nil
}

func (j *Journal) ensureIndexedLocked() error {
	if j.indexed {
		return nil
	}
	file, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		j.committed = make(map[journalKey]Record)
		j.indexed = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()
	committed := make(map[journalKey]Record)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return fmt.Errorf("decode journal record: %w", err)
		}
		record.Payload = append(json.RawMessage(nil), record.Payload...)
		committed[journalKey{record.Kind, record.EntityID}] = record
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read journal: %w", err)
	}
	j.committed = committed
	j.indexed = true
	return nil
}

func (j *Journal) appendLocked(kind, entityID string, payload json.RawMessage, at time.Time) error {
	record := Record{Kind: kind, EntityID: entityID, Timestamp: at.UTC(), Payload: payload}
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode journal record: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(j.path), 0o755); err != nil {
		return fmt.Errorf("create journal directory: %w", err)
	}
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append journal: %w", err)
	}
	return file.Sync()
}

func (j *Journal) Records() ([]Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	file, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()
	var records []Record
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode journal record: %w", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read journal: %w", err)
	}
	return records, nil
}
