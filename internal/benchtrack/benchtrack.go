// Package benchtrack records Go benchmark results into a JSONL history
// file and detects performance regressions between recorded runs.
//
// The package is the library behind the benchtrack command: it parses
// "go test -bench" text output and benchstat CSV output, appends results
// to a history file, and compares a current run against a baseline run
// with per-metric regression thresholds.
package benchtrack

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// SchemaVersion is the current JSONL record schema version written by
// Append. Load accepts any schema version so older histories remain
// readable.
const SchemaVersion = 1

// Record is a single benchmark measurement stored in the history file.
// BytesPerOp and AllocsPerOp are nil when the source output did not
// report memory statistics (for example "go test -bench" without
// -benchmem).
type Record struct {
	SchemaVersion int       `json:"schema"`
	Timestamp     time.Time `json:"ts"`
	Commit        string    `json:"commit,omitempty"`
	GOOS          string    `json:"goos"`
	GOARCH        string    `json:"goarch"`
	Name          string    `json:"name"`
	Iterations    int64     `json:"iterations"`
	NsPerOp       float64   `json:"ns_per_op"`
	BytesPerOp    *float64  `json:"bytes_per_op,omitempty"`
	AllocsPerOp   *float64  `json:"allocs_per_op,omitempty"`
}

// Append writes records to w as JSONL, one JSON object per line. Records
// with a zero SchemaVersion are stamped with the current SchemaVersion.
func Append(w io.Writer, records []Record) error {
	for i := range records {
		rec := records[i]
		if rec.SchemaVersion == 0 {
			rec.SchemaVersion = SchemaVersion
		}
		line, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("benchtrack: encode record %q: %w", rec.Name, err)
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			return fmt.Errorf("benchtrack: write record %q: %w", rec.Name, err)
		}
	}
	return nil
}

// AppendFile appends records to the JSONL history file at path, creating
// the file (and parent directories) if needed.
func AppendFile(path string, records []Record) error {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("benchtrack: create history directory %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("benchtrack: open history %s: %w", path, err)
	}
	defer f.Close()
	return Append(f, records)
}

// Load reads a JSONL history from r. Blank lines are skipped; any other
// line that fails to decode is reported with its 1-based line number.
func Load(r io.Reader) ([]Record, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var records []Record
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Bytes()
		if len(trimSpace(line)) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("benchtrack: history line %d: %w", lineNo, err)
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("benchtrack: read history: %w", err)
	}
	return records, nil
}

// LoadFile reads a JSONL history file from disk.
func LoadFile(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("benchtrack: open history %s: %w", path, err)
	}
	defer f.Close()
	return Load(f)
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\r') {
		b = b[1:]
	}
	for len(b) > 0 {
		c := b[len(b)-1]
		if c != ' ' && c != '\t' && c != '\r' {
			break
		}
		b = b[:len(b)-1]
	}
	return b
}
