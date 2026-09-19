package benchtrack

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAppendLoadRoundtrip(t *testing.T) {
	ts := time.Date(2026, 9, 19, 1, 2, 3, 456789, time.UTC)
	records := []Record{
		{
			SchemaVersion: SchemaVersion,
			Timestamp:     ts,
			Commit:        "abc123",
			GOOS:          "darwin",
			GOARCH:        "arm64",
			Name:          "BenchmarkAdd-8",
			Iterations:    1000,
			NsPerOp:       123.5,
			BytesPerOp:    floatPtr(512),
			AllocsPerOp:   floatPtr(3),
		},
		{
			SchemaVersion: SchemaVersion,
			Timestamp:     ts,
			GOOS:          "darwin",
			GOARCH:        "arm64",
			Name:          "BenchmarkNoMem-8",
			Iterations:    10,
			NsPerOp:       999,
		},
	}

	var buf bytes.Buffer
	if err := Append(&buf, records); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := strings.Count(buf.String(), "\n"); got != 2 {
		t.Fatalf("expected 2 JSONL lines, got %d", got)
	}

	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(loaded, records) {
		t.Fatalf("roundtrip mismatch:\n got %+v\nwant %+v", loaded, records)
	}
}

func TestAppendStampsSchemaVersion(t *testing.T) {
	var buf bytes.Buffer
	if err := Append(&buf, []Record{{Name: "BenchmarkX-8", NsPerOp: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded[0].SchemaVersion != SchemaVersion {
		t.Fatalf("expected schema %d, got %d", SchemaVersion, loaded[0].SchemaVersion)
	}
}

func TestAppendFileLoadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "history.jsonl")
	r1 := []Record{{Name: "BenchmarkA-8", NsPerOp: 1, Timestamp: time.Now().UTC()}}
	r2 := []Record{{Name: "BenchmarkB-8", NsPerOp: 2, Timestamp: time.Now().UTC()}}
	if err := AppendFile(path, r1); err != nil {
		t.Fatalf("AppendFile: %v", err)
	}
	if err := AppendFile(path, r2); err != nil {
		t.Fatalf("AppendFile second call: %v", err)
	}
	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if len(loaded) != 2 || loaded[0].Name != "BenchmarkA-8" || loaded[1].Name != "BenchmarkB-8" {
		t.Fatalf("unexpected history %+v", loaded)
	}
}

func TestLoadCorruptLine(t *testing.T) {
	input := `{"schema":1,"name":"BenchmarkA-8","ns_per_op":1}

{"schema":1,"name":`
	_, err := Load(strings.NewReader(input))
	if err == nil {
		t.Fatal("expected error for corrupt JSONL")
	}
	if !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("error should cite line 3, got %v", err)
	}
}

func TestLoadFileMissing(t *testing.T) {
	_, err := LoadFile(filepath.Join(t.TempDir(), "nope.jsonl"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestNewRecordsNormalizesUTC(t *testing.T) {
	loc := time.FixedZone("UTC+8", 8*3600)
	ts := time.Date(2026, 9, 19, 10, 0, 0, 0, loc)
	records := NewRecords([]ParsedBenchmark{{Name: "BenchmarkX-8", NsPerOp: 5}}, ts, "deadbeef", "linux", "amd64")
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	rec := records[0]
	if rec.Timestamp.Location() != time.UTC {
		t.Fatalf("timestamp not UTC: %v", rec.Timestamp.Location())
	}
	if rec.Commit != "deadbeef" || rec.GOOS != "linux" || rec.GOARCH != "amd64" {
		t.Fatalf("metadata mismatch %+v", rec)
	}
	if rec.SchemaVersion != SchemaVersion {
		t.Fatalf("expected schema %d, got %d", SchemaVersion, rec.SchemaVersion)
	}
	if rec.BytesPerOp != nil || rec.AllocsPerOp != nil {
		t.Fatalf("absent memory metrics must stay nil %+v", rec)
	}
}

func TestMain(m *testing.M) {
	// Guard against tests depending on the local timezone.
	os.Setenv("TZ", "UTC")
	os.Exit(m.Run())
}
